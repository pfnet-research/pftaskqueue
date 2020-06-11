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

package factory

import (
	"github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/redis"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

func NewBackend(logger zerolog.Logger, cfg config.Config) (iface.Backend, error) {
	switch cfg.BackendType {
	case "redis":
		return redis.NewBackend(logger, cfg)
	default:
		return nil, errors.Errorf("backendType=%s is not supported", cfg.BackendType)
	}
}
