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

package testutil

import (
	"fmt"

	"github.com/go-redis/redis/v7"
	"github.com/ory/dockertest"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	"github.com/pkg/errors"
)

func PrepareRedisContainer(clientConfig config.RedisClientConfig) (*redis.Client, *dockertest.Resource, *dockertest.Pool, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "Could not connect to docker")
	}

	resource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "Could not start container")
	}

	var client *redis.Client
	if err = pool.Retry(func() error {
		clientConfig.Addr = fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp"))
		client = clientConfig.NewClient()
		return client.Ping().Err()
	}); err != nil {
		return nil, nil, nil, errors.Wrapf(err, "Could not connect to redis container")
	}
	return client, resource, pool, nil
}

func CleanupRedisContainer(pool *dockertest.Pool, resource *dockertest.Resource) error {
	return pool.Purge(resource)
}
