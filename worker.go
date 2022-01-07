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
	"github.com/jackc/pgtype/pgxtype"
)

type worker struct {
	db             *pgxpool.Pool
	forUpdate      bool
	id             uuid.UUID
	thinkTime      time.Duration
	tolerateErrors bool
	useSavePoint   bool
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
		useSavePoint:   *UseSavePoint,
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

		if w.useSavePoint {
			if err := w.savepointLoop(ctx, tx); err != nil {
				return err
			}
		} else if err := w.doTransaction(ctx, tx); err != nil {
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			return errors.Wrapf(err, "committing %s", w.id)
		}
		return nil
	})
}

// https://www.cockroachlabs.com/docs/stable/advanced-client-side-transaction-retries.html#retry-savepoints
func (w *worker) savepointLoop(ctx context.Context, tx pgxtype.Querier) error {
	if _, err := tx.Exec(ctx, "SAVEPOINT cockroach_restart"); err != nil {
		return errors.Wrapf(err, "create savepoint %s", w.id)
	}

	attempt := func() error {
		if err := w.doTransaction(ctx, tx); err != nil {
			return err
		}

		_, err := tx.Exec(ctx, "RELEASE SAVEPOINT cockroach_restart")
		return errors.Wrapf(err, "releasing savepoint %s", w.id)
	}
	restarts := 0
	defer func() {
		restartCount.Observe(float64(restarts))
	}()

	// We have an inner loop to set up the savepoint
	for {
		err := attempt()
		if isRetryable(err) {
			if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
				return errors.Wrapf(err, "rollback to savepoint %s", w.id)
			}
			restarts++
			continue
		}
		return err
	}
}

func (w *worker) doTransaction(ctx context.Context, tx pgxtype.Querier) error {
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
	_, err := tx.Exec(ctx,
		"UPDATE contend SET value = $1 WHERE id = $2",
		next, w.id)
	return errors.Wrapf(err, "updating %s", w.id)
}
