package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type p2cReader struct {
	conf    *config
	rx      prometheus.Counter
	tx      prometheus.Counter
	timings prometheus.Histogram
}

// getTimePeriod return select and where SQL chunks relating to the time period -or- error
func (r *p2cReader) getTimePeriod(query *prompb.Query) (string, string, error) {

	var tselSQL = "SELECT COUNT() AS CNT, toUInt32(ts)*1000 AS t"
	var twhereSQL = "WHERE date >= toDate(%d) AND date <= toDate(%d) AND ts >= toDateTime(%d) AND ts <= toDateTime(%d)"
	var err error
	tstart := query.StartTimestampMs / 1000
	tend := query.EndTimestampMs / 1000

	// valid time period
	if tend < tstart {
		err = errors.New("Start time is after end time")
		return "", "", err
	}
	whereSQL := fmt.Sprintf(twhereSQL, tstart, tstart, tend, tend)

	return tselSQL, whereSQL, nil
}

func (r *p2cReader) getSQL(query *prompb.Query) (string, error) {
	// time related select sql, where sql chunks
	tselectSQL, twhereSQL, err := r.getTimePeriod(query)
	if err != nil {
		return "", err
	}

	// match sql chunk
	var mwhereSQL []string
	// build an sql statement chunk for each matcher in the query
	// yeah, this is a bit ugly..
	for _, m := range query.Matchers {
		// __name__ is handled specially - match it directly
		// as it is stored in the name column (it's also in tags as __name__)
		// note to self: add name to index.. otherwise this will be slow..
		if m.Name == model.MetricNameLabel {
			var whereAdd string
			whereAdd = fmt.Sprintf(` name='%s' `, strings.Replace(m.Value, `'`, `\'`, -1))
			mwhereSQL = append(mwhereSQL, whereAdd)
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			asql := "has(tags, '%s=%s') = 1"
			wstr := fmt.Sprintf(asql, m.Name, strings.Replace(m.Value, `'`, `\'`, -1))
			mwhereSQL = append(mwhereSQL, wstr)

		case prompb.LabelMatcher_NEQ:
			asql := "has(tags, '%s=%s') = 0"
			wstr := fmt.Sprintf(asql, m.Name, strings.Replace(m.Value, `'`, `\'`, -1))
			mwhereSQL = append(mwhereSQL, wstr)

		case prompb.LabelMatcher_RE:
			asql := `arrayExists(x -> match(x, '^%s=%s$'),tags) = 1`
			// we can't have ^ in the regexp since keys are stored in arrays of key=value
			val := strings.TrimPrefix(m.Value, "^")
			val = strings.TrimSuffix(val, "$")
			val = strings.Replace(val, `/`, `\/`, -1)
			mwhereSQL = append(mwhereSQL, fmt.Sprintf(asql, m.Name, val))

		case prompb.LabelMatcher_NRE:
			asql := `arrayExists(x -> match(x, '^%s=%s$'),tags) = 0`
			val := strings.TrimPrefix(m.Value, "^")
			val = strings.TrimSuffix(val, "$")
			val = strings.Replace(val, `/`, `\/`, -1)
			mwhereSQL = append(mwhereSQL, fmt.Sprintf(asql, m.Name, val))
		}
	}

	// put select and where together with group by etc
	tempSQL := "%s, name, tags, quantile(%f)(val) as value FROM %s.%s %s AND %s GROUP BY t, name, tags ORDER BY t"
	sql := fmt.Sprintf(tempSQL, tselectSQL, r.conf.CHQuantile, r.conf.DB, r.conf.Table, twhereSQL,
		strings.Join(mwhereSQL, " AND "))
	return sql, nil
}

func NewP2CReader(conf *config) (*p2cReader, error) {
	var subNamesapce = "reader"
	r := new(p2cReader)
	r.conf = conf

	r.rx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "request_query_total",
			Help:      "Total number of read request query.",
		},
	)
	r.tx = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "response_samples_total",
			Help:      "Total number of response samples total.",
		},
	)
	r.timings = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subNamesapce,
			Name:      "duration_seconds",
			Help:      "Duration of sample batch read from the remote storage.",
			Buckets:   prometheus.DefBuckets,
		},
	)
	prometheus.MustRegister(r.rx)
	prometheus.MustRegister(r.tx)
	prometheus.MustRegister(r.timings)

	return r, nil
}

func (r *p2cReader) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	var err error
	var sqlStr string
	var rows *sql.Rows

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: make([]*prompb.TimeSeries, 0, 0)},
		},
	}
	// need to map tags to timeseries to record samples
	var tsres = make(map[string]*prompb.TimeSeries)

	// for debugging/figuring out query format/etc
	rcount := 0
	start := time.Now()
	for _, q := range req.Queries {
		// remove me..
		log.Debugf("\nquery: start: %d, end: %d\n\n", q.StartTimestampMs, q.EndTimestampMs)

		// get the select sql
		sqlStr, err = r.getSQL(q)
		log.Debugf("query: running sql: %s\n\n", sqlStr)
		if err != nil {
			log.Errorf("reader: getSQL: %s\n", err.Error())
			return &resp, err
		}

		// get the select sql
		if err != nil {
			log.Errorf("reader: getSQL: %s\n", err.Error())
			return &resp, err
		}

		// todo: metrics on number of errors, rows, selects, timings, etc
		rows, err = db.Query(sqlStr)
		if err != nil {
			log.Errorf("query error: %s \n %s\n", sqlStr, err)
			return &resp, err
		}
		defer rows.Close()

		// build map of timeseries from sql result

		for rows.Next() {
			rcount++
			var (
				cnt   int
				t     int64
				name  string
				tags  []string
				value float64
			)
			if err = rows.Scan(&cnt, &t, &name, &tags, &value); err != nil {
				log.Errorf("scan: %s\n", err.Error())
				continue
			}

			// borrowed from influx remote storage adapter - array sep
			key := strings.Join(tags, "\xff")
			ts, ok := tsres[key]
			if !ok {
				ts = &prompb.TimeSeries{
					Labels: makeLabels(tags),
				}
				tsres[key] = ts
			}
			ts.Samples = append(ts.Samples, prompb.Sample{
				Value:     float64(value),
				Timestamp: t,
			})
		}
	}

	// now add results to response
	for _, ts := range tsres {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}
	r.rx.Add(float64(len(req.Queries)))
	r.tx.Add(float64(len(tsres)))
	r.timings.Observe(time.Since(start).Seconds())

	log.Debugf("query: returning %d rows for %d queries\n", rcount, len(req.Queries))

	return &resp, nil

}

func makeLabels(tags []string) []*prompb.Label {
	lpairs := make([]*prompb.Label, 0, len(tags))
	// (currently) writer includes __name__ in tags so no need to add it here
	// may change this to save space later..
	for _, tag := range tags {
		vals := strings.SplitN(tag, "=", 2)
		if len(vals) != 2 {
			log.Errorf("unpacking tag key/val: %s\n", tag)
			continue
		}
		if vals[1] == "" {
			continue
		}
		lpairs = append(lpairs, &prompb.Label{
			Name:  vals[0],
			Value: vals[1],
		})
	}
	return lpairs
}
