package main

import (
	"context"
	"database/sql"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/prompb"
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
	srv         *http.Server
	conf        *config
	writer      *p2cWriter
	reader      *p2cReader
	writeReqCnt prometheus.Counter
	readReqCnt  prometheus.Counter
	ctx         context.Context
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
	c.conf = conf

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func(cancel context.CancelFunc) {
		select {
		case <-sig:
			cancel()
		}
	}(cancel)
	c.ctx = ctx

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
		[]string{"handler", "code", "method"},
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
	prometheus.MustRegister(inFlightGauge, counter, duration)
	writeHandler := promhttp.InstrumentHandlerInFlight(inFlightGauge,
		promhttp.InstrumentHandlerDuration(duration.MustCurryWith(prometheus.Labels{"handler": c.conf.HTTPWritePath}),
			promhttp.InstrumentHandlerCounter(counter.MustCurryWith(prometheus.Labels{"handler": c.conf.HTTPWritePath}), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			promhttp.InstrumentHandlerCounter(counter.MustCurryWith(prometheus.Labels{"handler": "/read"}), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	http.Handle(c.conf.HTTPWritePath, writeHandler)
	http.Handle("/read", readHandler)
	http.Handle(c.conf.HTTPMetricsPath, promhttp.Handler())
	return c, nil
}

func (c *p2cServer) Start() error {
	log.Infoln("HTTP server starting...")
	c.writer.StartProcess()
	c.srv = &http.Server{Addr: c.conf.HTTPAddr}
	go func() {
		<-c.ctx.Done()
		c.srv.Shutdown(context.Background())
	}()
	return c.srv.ListenAndServe()
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
