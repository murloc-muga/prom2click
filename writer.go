package main

import (
	"fmt"
	"sort"
	"time"

	"sync"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var insertSQL = `INSERT INTO %s.%s
	(date, name, tags, val, ts)
	VALUES	(?, ?, ?, ?, ?)`

type p2cWriter struct {
	conf           *config
	requests       chan *p2cRequest
	wg             *sync.WaitGroup
	recvCounter    prometheus.Counter
	writeCounter   prometheus.Counter
	ko             prometheus.Counter
	timings        prometheus.Histogram
	queueSizeGauge prometheus.GaugeFunc
}

func NewP2CWriter(conf *config, reqs chan *p2cRequest) (*p2cWriter, error) {
	var subNamesapce = "writer"
	w := new(p2cWriter)
	w.conf = conf
	w.requests = reqs
	w.wg = new(sync.WaitGroup)
	// CreateDBTable(w.db, w.conf.ChDB, w.conf.ChTable)

	w.recvCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "request_samples_total",
			Help:      "Total number of remote write request samples",
		},
	)

	w.writeCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "write_samples_total",
			Help:      "Total number of processed samples sent to storage.",
		},
	)

	w.ko = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "database_error_total",
			Help:      "Total number of processed samples which failed on send to remote storage.",
		},
	)

	w.timings = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "sent_batch_duration_seconds",
			Help:      "Duration of sample batch send calls to the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
	)
	w.queueSizeGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "channel_size",
		},
		func() float64 {
			return float64(len(w.requests))
		},
	)
	prometheus.MustRegister(w.recvCounter, w.writeCounter)
	prometheus.MustRegister(w.ko, w.timings, w.queueSizeGauge)

	return w, nil
}

func (c *p2cWriter) write(req *prompb.WriteRequest) {
	for _, series := range req.Timeseries {
		c.recvCounter.Add(float64(len(series.Samples)))
		var (
			name string
			tags []string
		)
		var isAlerts bool
		for _, label := range series.Labels {
			if model.LabelName(label.Name) == model.MetricNameLabel {
				name = label.Value
				if name == "ALERTS" || name == "ALERTS_FOR_STATUS" {
					isAlerts = true
					break
				}
			} else {
				// store tags in <key>=<value> format
				// allows for has(tags, "key=val") searches
				// probably impossible/difficult to do regex searches on tags
				t := fmt.Sprintf("%s=%s", label.Name, label.Value)
				tags = append(tags, t)
			}
		}
		if isAlerts {
			continue
		}
		for _, sample := range series.Samples {
			p2c := new(p2cRequest)
			p2c.name = name
			p2c.ts = time.Unix(sample.Timestamp/1000, 0)
			p2c.val = sample.Value
			p2c.tags = tags
			c.requests <- p2c
		}
	}
}

func (w *p2cWriter) StartProcess() {
	log.Infof("start write goroutine process with %d", w.conf.Process)
	sql := fmt.Sprintf(insertSQL, w.conf.DB, w.conf.Table)
	for i := 0; i < w.conf.Process; i++ {
		go func() {
			w.wg.Add(1)
			log.Infoln("Writer starting..")
			ok := true
			for ok {
				// get next batch of requests
				var reqs []*p2cRequest

				timeout := time.NewTimer(time.Second * 5)
			batch:
				for i := 0; i < w.conf.Batch; i++ {
					var req *p2cRequest
					// if request not enough and timeout, insert current request to database first.
					select {
					case req, ok = <-w.requests:
						if !ok {
							log.Warnln("Writer stopping..")
							break batch
						}
						reqs = append(reqs, req)
					case <-timeout.C:
						break batch
					}
				}

				// ensure we have something to send..
				nmetrics := len(reqs)
				if nmetrics < 1 {
					continue
				}
				tstart := time.Now()

				// post them to db all at once
				tx, err := db.Begin()
				if err != nil {
					log.Errorf("begin transaction: %s\n", err.Error())
					w.ko.Inc()
					continue
				}
				// build statements
				smt, err := tx.Prepare(sql)
				if err != nil {
					log.Errorf("prepare statement: %s\n", err.Error())
					w.ko.Inc()
					continue
				}
				defer smt.Close()
				for _, req := range reqs {
					// ensure tags are inserted in the same order each time
					// possibly/probably impacts indexing?
					sort.Strings(req.tags)
					_, err = smt.Exec(req.ts, req.name, clickhouse.Array(req.tags),
						req.val, req.ts)

					if err != nil {
						log.Errorf("statement exec: %s\n", err.Error())
						w.ko.Inc()
					}
				}

				// commit and record metrics
				if err = tx.Commit(); err != nil {
					log.Errorf("commit failed: %s\n", err.Error())
					w.ko.Inc()
				} else {
					w.writeCounter.Add(float64(nmetrics))
					w.timings.Observe(time.Since(tstart).Seconds())
				}

			}
			log.Infoln("Writer stopped..")
			w.wg.Done()
		}()
	}
}

func (w *p2cWriter) Wait() {
	w.wg.Wait()
}
