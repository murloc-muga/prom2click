package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
)

// a lot of this borrows directly from:
// 	https://github.com/prometheus/prometheus/blob/master/documentation/examples/remote_storage/remote_storage_adapter/main.go

type config struct {
	//tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	DSN             string
	DB              string
	DBMaxIdle       int
	DBMaxOpen       int
	Table           string
	Batch           int
	ChanSize        int
	Process         int
	CHQuantile      float64
	CHMinPeriod     int
	HTTPTimeout     time.Duration
	HTTPAddr        string
	HTTPWritePath   string
	HTTPMetricsPath string
	LogLevel        string
}

var (
	versionFlag bool
)

func main() {
	conf := parseFlags()

	if versionFlag {
		fmt.Println(version.Print("prom2click"))
		return
	}
	log.Base().SetLevel(conf.LogLevel)
	log.Infoln("Starting up..")

	srv, err := NewP2CServer(conf)
	if err != nil {
		log.Errorf("Error: could not create server: %s\n", err.Error())
		return
	}
	err = srv.Start()
	if err != nil {
		return
	}

	log.Infoln("Shutting down..")
	srv.Shutdown()
	log.Infoln("Exiting..")
}

func parseFlags() *config {
	cfg := new(config)

	// print version?
	flag.BoolVar(&versionFlag, "version", false, "Version")

	// clickhouse dsn
	ddsn := "tcp://127.0.0.1:9000?username=&password=&database=metrics&" +
		"read_timeout=10&write_timeout=10&alt_hosts="
	flag.StringVar(&cfg.DSN, "ch.dsn", ddsn,
		"The clickhouse server DSN to write to eg."+
			"tcp://host1:9000?username=user&password=qwerty&database=clicks&"+
			"read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000"+
			"(see https://github.com/kshvakov/clickhouse).",
	)

	// clickhouse db
	flag.StringVar(&cfg.DB, "ch.db", "metrics",
		"The clickhouse database to write to.",
	)

	flag.IntVar(&cfg.DBMaxIdle, "ch.db.maxIdle", 2, "The database max idle connection.")
	flag.IntVar(&cfg.DBMaxOpen, "ch.db.maxopen", 0, "The database max open connection.")

	// clickhouse table
	flag.StringVar(&cfg.Table, "ch.table", "samples",
		"The clickhouse table to write to.",
	)

	// clickhouse insertion batch size
	flag.IntVar(&cfg.Batch, "ch.batch", 65536,
		"Clickhouse write batch size (n metrics).",
	)

	// channel buffer size between http server => clickhouse writer(s)
	flag.IntVar(&cfg.ChanSize, "ch.buffer", 65536,
		"Maximum internal channel buffer size (n requests).",
	)

	flag.IntVar(&cfg.Process, "ch.process", 1,
		"The number of goroutine to consume channel and write to storage.",
	)

	// quantile (eg. 0.9 for 90th) for aggregation of timeseries values from CH
	flag.Float64Var(&cfg.CHQuantile, "ch.quantile", 0.75,
		"Quantile/Percentile for time series aggregation when the number "+
			"of points exceeds ch.maxsamples.",
	)

	// http shutdown and request timeout
	flag.IntVar(&cfg.CHMinPeriod, "ch.minperiod", 10,
		"The minimum time range for Clickhouse time aggregation in seconds.",
	)

	// http listen address
	flag.StringVar(&cfg.HTTPAddr, "web.address", ":9201",
		"Address to listen on for web endpoints.",
	)

	// http prometheus remote write endpoint
	flag.StringVar(&cfg.HTTPWritePath, "web.write", "/write",
		"Address to listen on for remote write requests.",
	)

	// http prometheus metrics endpoint
	flag.StringVar(&cfg.HTTPMetricsPath, "web.metrics", "/metrics",
		"Address to listen on for metric requests.",
	)

	// http shutdown and request timeout
	flag.DurationVar(&cfg.HTTPTimeout, "web.timeout", 30*time.Second,
		"The timeout to use for HTTP requests and server shutdown. Defaults to 30s.",
	)

	flag.Parse()

	return cfg
}
