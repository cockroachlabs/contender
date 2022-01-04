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
	"math/rand"
	"time"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

type worker struct {
	db             *pgxpool.Pool
	forUpdate      bool
	id             uuid.UUID
	thinkTime      time.Duration
	tolerateErrors bool
}

func newWorker(ctx context.Context, db *pgxpool.Pool) (*worker, error) {
	var id uuid.UUID
	if err := retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(ctx, "INSERT INTO contend (id) VALUES (DEFAULT) RETURNING id").Scan(&id)
	}); err != nil {
		return nil, errors.Wrap(err, "allocating unique id")
	}

	return &worker{
		db:             db,
		forUpdate:      *UseForUpdate,
		id:             id,
		thinkTime:      *ThinkTime,
		tolerateErrors: *TolerateErrors,
	}, nil
}

func (w *worker) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.thinkTime):
			start := time.Now()
			if err := w.runOneIteration(ctx); err != nil {
				if w.tolerateErrors {
					log.Printf("ignoring error: %v", err)
					continue
				}
				return err
			}
			latency := time.Since(start)
			workerLatency.Observe(latency.Seconds())
		}
	}
}

func (w *worker) runOneIteration(ctx context.Context) error {
	attempts := 0
	defer func() {
		attemptCount.Observe(float64(attempts))
	}()
	return retry(ctx, func(ctx context.Context) error {
		attempts++
		tx, err := w.db.Begin(ctx)
		if err != nil {
			return errors.Wrapf(err, "open transaction %s", w.id)
		}
		defer tx.Rollback(ctx)

		q := "SELECT value FROM contend WHERE id = $1"
		if w.forUpdate {
			q += " FOR UPDATE"
		}

		var current int
		if err := tx.QueryRow(ctx, q, w.id).Scan(&current); err != nil {
			return errors.Wrapf(err, "select current value %s", w.id)
		}

		time.Sleep(w.thinkTime)

		next := rand.Int()
		if _, err := tx.Exec(ctx,
			"UPDATE contend SET value = $1 WHERE id = $2",
			next, w.id,
		); err != nil {
			return errors.Wrapf(err, "updating %s", w.id)
		}

		if err := tx.Commit(ctx); err != nil {
			return errors.Wrapf(err, "committing %s", w.id)
		}
		return nil
	})
}
