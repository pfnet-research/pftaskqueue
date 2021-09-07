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

package config

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v7"
)

type Config struct {
	BackendType string
	Redis       *RedisConfig
}

type RedisConfig struct {
	KeyPrefix         string
	Client            *redis.Client
	Backoff           BackoffConfig
	ChunkSizeInGet    int
	ChunkSizeInDelete int
}

// TODO: support UniversalOptions
type RedisClientConfig struct {
	Addr     string `json:"addr" yaml:"addr" default:""`
	Password string `json:"password" yaml:"password" default:""`
	DB       int    `json:"db" yaml:"db" default:"0"`

	DialTimeout        time.Duration `json:"dialTimeout" yaml:"dialTimeout" default:"30s"`
	ReadTimeout        time.Duration `json:"readTimeout" yaml:"readTimeout" default:"10m"`
	WriteTimeout       time.Duration `json:"writeTimeout" yaml:"writeTimeout" default:"10m"`
	PoolSize           int           `json:"poolSize" yaml:"poolSize" default:"-"`
	MinIdleConns       int           `json:"minIdleConns" yaml:"minIdleConns" default:"-"`
	MaxConnAge         time.Duration `json:"maxConnAge" yaml:"maxConnAge" default:"-"`
	PoolTimeout        time.Duration `json:"poolTimeout" yaml:"poolTimeout" default:"-"`
	IdleTimeout        time.Duration `json:"idleTimeout" yaml:"idleTimeout" default:"5m"`
	IdleCheckFrequency time.Duration `json:"idleCheckFrequency" yaml:"idleCheckFrequency" default:"1m"`

	ChunkSizeInGet    int `json:"chunkSizeInGet" yaml:"chunkSizeInGet" default:"10000"`
	ChunkSizeInDelete int `json:"chunkSizeInDelete" yaml:"chunkSizeInGet" default:"1000"`
}

func (c RedisClientConfig) NewClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:               c.Addr,
		Password:           c.Password,
		DB:                 c.DB,
		DialTimeout:        c.DialTimeout,
		ReadTimeout:        c.ReadTimeout,
		WriteTimeout:       c.WriteTimeout,
		PoolSize:           c.PoolSize,
		MinIdleConns:       c.MinIdleConns,
		MaxConnAge:         c.MaxConnAge,
		PoolTimeout:        c.PoolTimeout,
		IdleTimeout:        c.IdleTimeout,
		IdleCheckFrequency: c.IdleCheckFrequency,
	})
}

type BackoffConfig struct {
	InitialInterval     time.Duration `json:"initialInterval" yaml:"initialInterval" default:"500ms"`
	RandomizationFactor float64       `json:"randomizationFactor" yaml:"randomizationFactor" default:"0.5"`
	Multiplier          float64       `json:"multiplier" yaml:"multiplier" default:"1.2"`
	MaxInterval         time.Duration `json:"maxInterval" yaml:"maxInterval" default:"60s"`
	// After MaxElapsedTime the ExponentialBackOff returns Stop.
	// It never stops if MaxElapsedTime == 0.
	MaxElapsedTime time.Duration `json:"maxElapsedTime" yaml:"maxElapsedTime" default:"10m"`
	MaxRetry       int64         `json:"maxRetry" yaml:"maxRetry" default:"-1"`
}

func (bc BackoffConfig) NewBackoff() backoff.BackOff {
	bf := backoff.BackOff(&backoff.ExponentialBackOff{
		InitialInterval:     bc.InitialInterval,
		RandomizationFactor: bc.RandomizationFactor,
		Multiplier:          bc.Multiplier,
		MaxInterval:         bc.MaxInterval,
		MaxElapsedTime:      bc.MaxElapsedTime,
		Clock:               backoff.SystemClock,
		Stop:                backoff.Stop,
	})
	if bc.MaxRetry >= 0 {
		bf = backoff.WithMaxRetries(bf, uint64(bc.MaxRetry))
	}
	return bf
}
