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

	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
)

func retry(ctx context.Context, fn func(context.Context) error) error {
	for {
		err := fn(ctx)
		if !isRetryable(err) {
			return err
		}
	}
}

// isRetryable checks the embedded error code of a postgres exception to
// determine if it should be retried. A nil error is not retryable.
func isRetryable(err error) bool {
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
		pgerrorCount.WithLabelValues(pgErr.Code).Inc()

		switch pgErr.Code {
		case
			"40001", // Serialization Failure
			"40003", // Statement Completion Unknown
			"08003", // Connection Does Not Exist
			"08006": // Connection Failure
			return true
		}
	}

	return false
}
