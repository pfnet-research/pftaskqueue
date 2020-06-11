// Licensed to Preferred Networks, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Preferred Networks, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package redis

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v7"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/common"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	"github.com/rs/zerolog"
)

const (
	GlobalKeyPrefix = "_pftaskqueue:"
)

var _ iface.Backend = &Backend{}

type Backend struct {
	*common.Backend
	*config.RedisConfig
}

func NewBackend(logger zerolog.Logger, cfg config.Config) (iface.Backend, error) {
	return &Backend{
		Backend: &common.Backend{
			Logger: logger.With().Str("component", "redis-backend").Logger(),
		},
		RedisConfig: cfg.Redis,
	}, nil
}

func (b *Backend) runTxWithBackOff(ctx context.Context, logger zerolog.Logger, txf func(tx *redis.Tx) error, keys ...string) error {
	bf := b.Backoff.NewBackoff()
	bf = backoff.WithContext(bf, ctx)
	err := backoff.RetryNotify(
		func() error {
			return b.runTxOnce(ctx, logger.With().Logger().Level(zerolog.Disabled), txf, keys...)
		},
		bf,
		func(err error, next time.Duration) {
			if err == redis.TxFailedErr {
				logger.Debug().Err(err).Dur("retryAfter", next).Msg("RedisClient transaction failed. Retry after duration")
			}
		},
	)
	if err != nil {
		if bf.NextBackOff() == backoff.Stop {
			logger.Debug().Err(err).Msg("RedisClient operation failed. Backoff retry limit exceeded")
		} else {
			logger.Debug().Err(err).Msg("RedisClient operation failed. Retry canceled")
		}
	}
	return err
}

func (b *Backend) runTxOnce(ctx context.Context, logger zerolog.Logger, txf func(tx *redis.Tx) error, keys ...string) error {
	err := b.Client.WatchContext(ctx, txf, keys...)
	switch {
	case err == nil:
		// success
		return nil
	case err == redis.TxFailedErr:
		logger.Debug().Err(err).Msg("RedisClient transaction failed")
		return err
	default:
		logger.Debug().Err(err).Msg("RedisClient returned error")
		return err
	}
}
