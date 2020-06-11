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

package worker_test

import (
	"os"
	"testing"

	"github.com/pfnet-research/pftaskqueue/pkg/backend/testutil"

	"github.com/go-redis/redis/v7"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest"
	backendconfig "github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	pool     *dockertest.Pool
	resource *dockertest.Resource
	client   *redis.Client
	logger   zerolog.Logger
)

func TestWorker(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Worker Suite")
}

var _ = BeforeSuite(func() {
	var err error
	client, resource, pool, err = testutil.PrepareRedisContainer(backendconfig.DefaultRedisClientOption())
	Expect(err).NotTo(HaveOccurred())

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Level(zerolog.DebugLevel)
	logger = log.Logger.With().Caller().Stack().Logger()
})

var _ = AfterSuite(func() {
	_ = client.Close()
	_ = testutil.CleanupRedisContainer(pool, resource)
})
