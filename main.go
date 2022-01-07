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
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

//go:generate go run github.com/cockroachdb/crlfmt -w .

var (
	ConnString = flag.String(
		"conn",
		"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
		"database connection string")
	MaxConns = flag.Int(
		"maxConns",
		10000,
		"the maximum number of open database connections")
	MetricsServer = flag.String(
		"http",
		":8181",
		"a bind string for the metrics server")
	ThinkTime = flag.Duration(
		"thinkTime",
		25*time.Millisecond,
		"the amount of time for workers to sleep during transaction")
	TolerateErrors = flag.Bool(
		"tolerateErrors",
		false,
		"set to true to ignore non-retryable errors")
	UniqueIds = flag.Int(
		"uniqueIds",
		1,
		"the number of unique keys to update")
	UseForUpdate = flag.Bool(
		"selectForUpdate",
		false,
		"if true, use SELECT FOR UPDATE")
	UseSavePoint = flag.Bool(
		"savePoint",
		false,
		"if true, use advanced retry approach to increase TX priorities")
	WorkersPerId = flag.Int(
		"workersPerId",
		1,
		"the number of workers trying to update each id")
)

func main() {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		log.Printf("command failed: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func run(ctx context.Context) error {
	poolCfg, err := pgxpool.ParseConfig(*ConnString)
	poolCfg.MaxConns = int32(*MaxConns)

	pool, err := pgxpool.ConnectConfig(ctx, poolCfg)
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	go metricsServer(ctx, pool)

	if _, err := pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS contend (
	id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
	value INT NOT NULL DEFAULT 0
)`,
	); err != nil {
		return errors.Wrap(err, "could not create table")
	}

	group, ctx := errgroup.WithContext(ctx)

	for i := 0; i < *UniqueIds; i++ {
		top, err := newWorker(ctx, pool)
		if err != nil {
			return err
		}

		for j := 0; j < *WorkersPerId; j++ {
			group.Go(func() error {
				return top.run(ctx)
			})
		}
	}

	return group.Wait()
}
