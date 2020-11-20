package main

import (
	"database/sql"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/prompb"
	"gopkg.in/tylerb/graceful.v1"
)

const (
	namespace = "prom2click"
)

var db *sql.DB

type p2cRequest struct {
	name string
	tags []string
	val  float64
	ts   time.Time
}

type p2cServer struct {
	requests    chan *p2cRequest
	mux         *http.ServeMux
	conf        *config
	writer      *p2cWriter
	reader      *p2cReader
	rx          prometheus.Counter
	writeReqCnt prometheus.Counter
	readReqCnt  prometheus.Counter
}

func NewP2CServer(conf *config) (*p2cServer, error) {
	var err error

	db, err = sql.Open("clickhouse", conf.DSN)
	if err != nil {
		log.Errorf("connecting to clickhouse: %s\n", err.Error())
		return nil, err
	}

	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			log.Errorln(err)
		}
		return nil, err
	}
	db.SetMaxIdleConns(conf.DBMaxIdle)
	db.SetMaxOpenConns(conf.DBMaxOpen)

	c := new(p2cServer)
	c.requests = make(chan *p2cRequest, conf.ChanSize)
	c.mux = http.NewServeMux()
	c.conf = conf

	c.writer, err = NewP2CWriter(conf, c.requests)
	if err != nil {
		log.Errorf("Error creating clickhouse writer: %s\n", err.Error())
		return c, err
	}

	c.reader, err = NewP2CReader(conf)
	if err != nil {
		log.Errorf("Error creating clickhouse reader: %s\n", err.Error())
		return c, err
	}

	c.rx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "received_samples_total",
			Help:      "Total number of received samples.",
		},
	)
	inFlightGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "in_flight_requests",
		Help:      "A gauge of requests currently being served by the wrapped handler.",
	})

	counter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "api_requests_total",
			Help:      "A counter for requests to the wrapped handler.",
		},
		[]string{"code", "method"},
	)

	// duration is partitioned by the HTTP method and handler. It uses custom
	// buckets based on the expected request duration.
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "request_duration_seconds",
			Help:      "A histogram of latencies for requests.",
			Buckets:   []float64{.25, .5, 1, 2.5, 5, 10},
		},
		[]string{"handler", "code", "method"},
	)
	prometheus.MustRegister(c.rx)
	prometheus.MustRegister(inFlightGauge, counter, duration)
	writeHandler := promhttp.InstrumentHandlerInFlight(inFlightGauge,
		promhttp.InstrumentHandlerDuration(duration.MustCurryWith(prometheus.Labels{"handler": c.conf.HTTPWritePath}),
			promhttp.InstrumentHandlerCounter(counter, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				compressed, err := ioutil.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer r.Body.Close()
				reqBuf, err := snappy.Decode(nil, compressed)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				var req prompb.WriteRequest
				if err := proto.Unmarshal(reqBuf, &req); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				go c.writer.write(&req)
			})),
		),
	)

	readHandler := promhttp.InstrumentHandlerInFlight(inFlightGauge,
		promhttp.InstrumentHandlerDuration(duration.MustCurryWith(prometheus.Labels{"handler": "/read"}),
			promhttp.InstrumentHandlerCounter(counter, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				compressed, err := ioutil.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				defer r.Body.Close()
				reqBuf, err := snappy.Decode(nil, compressed)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				var req prompb.ReadRequest
				if err := proto.Unmarshal(reqBuf, &req); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				var resp *prompb.ReadResponse
				resp, err = c.reader.Read(&req)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				data, err := proto.Marshal(resp)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}

				w.Header().Set("Content-Type", "application/x-protobuf")
				w.Header().Set("Content-Encoding", "snappy")

				compressed = snappy.Encode(nil, data)
				if _, err := w.Write(compressed); err != nil {
					log.Error(err)
					return
				}
			})),
		),
	)
	c.mux.Handle(c.conf.HTTPWritePath, writeHandler)
	c.mux.Handle("/read", readHandler)
	c.mux.Handle(c.conf.HTTPMetricsPath, promhttp.Handler())

	return c, nil
}

func (c *p2cServer) Start() error {
	log.Infoln("HTTP server starting...")
	c.writer.StartProcess()
	return graceful.RunWithErr(c.conf.HTTPAddr, c.conf.HTTPTimeout, c.mux)
}

func (c *p2cServer) Shutdown() {
	close(c.requests)
	c.writer.Wait()

	wchan := make(chan struct{})
	go func() {
		c.writer.Wait()
		close(wchan)
	}()

	select {
	case <-wchan:
		log.Infof("Writer shutdown cleanly..")
	// All done!
	case <-time.After(10 * time.Second):
		log.Warnf("Writer shutdown timed out, samples will be lost..")
	}
	db.Close()
}
