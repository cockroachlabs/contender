// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	attemptBuckets = []float64{
		1., 2., 3., 4., 5., 6., 7., 8., 9.,
		10, 20, 30, 40, 50, 60, 70, 80, 90,
	}

	latencyBuckets = []float64{
		.001, .002, .003, .004, .005, .006, .007, .008, .009,
		.01, .02, .03, .04, .05, .06, .07, .08, .09,
		0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
		1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
		10., 20., 30., 40., 50., 60.,
	}

	attemptCount = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_attempts_total",
		Help:    "the number of retries that occur within a single worker loop",
		Buckets: attemptBuckets,
	})
	pgerrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgerror_codes_total",
		Help: "the number of postgres errors",
	}, []string{"code"})
	workerLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "worker_overall_latency_seconds",
		Help:    "the overall worker latency for successful requests",
		Buckets: latencyBuckets,
	})
)

func metricsServer(ctx context.Context, db *pgxpool.Pool) error {
	// Set up some one-shot gauges to record configuration.
	info := promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "flag_info",
	}, []string{"flag"})
	info.WithLabelValues("max_conns").Set(float64(*MaxConns))
	info.WithLabelValues("think_time").Set(ThinkTime.Seconds())
	info.WithLabelValues("unique_ids").Set(float64(*UniqueIds))
	if g := info.WithLabelValues("select_for_update"); *UseForUpdate {
		g.Set(1)
	} else {
		g.Set(0)
	}
	info.WithLabelValues("workers_per_id").Set(float64(*WorkersPerId))

	// DB Information
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "db_acquired_connection_count",
	}, func() float64 { return float64(db.Stat().AcquiredConns()) })
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "db_idle_connection_count",
	}, func() float64 { return float64(db.Stat().IdleConns()) })

	l, err := net.Listen("tcp", *MetricsServer)
	if err != nil {
		return errors.Wrap(err, "opening port")
	}
	log.Printf("listening on %s", l.Addr())
	srv := http.Server{
		Handler: promhttp.Handler(),
	}
	go func() {
		<-ctx.Done()
		grace, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = srv.Shutdown(grace)
	}()
	return srv.Serve(l)
}
