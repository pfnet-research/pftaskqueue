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
	"github.com/go-playground/validator/v10"
	"github.com/hashicorp/go-multierror"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/worker"
	"github.com/pfnet-research/pftaskqueue/pkg/util"
	"github.com/pkg/errors"
)

var (
	Validator = func() *validator.Validate {
		v := validator.New()
		worker.RegisterValidator(v)
		return v
	}()
)

type WorkerConfig struct {
	QueueName string `json:"queueName" yaml:"queueName" default:"-" validate:"required,min=1,max=256,excludesall=:"`

	worker.WorkerSpec `json:",inline" yaml:",inline" mapstructure:",squash"`

	NumWorkerSalvageOnStartup int `json:"numWorkerSalvageOnStartup" yaml:"numWorkerSalvageOnStartup" default:"-1" validate:"required"`
}

func (wc WorkerConfig) Validate() *util.ValidationError {
	err := Validator.Struct(&wc)
	if err != nil {
		var verrs *multierror.Error
		for _, verr := range err.(validator.ValidationErrors) {
			fieldName := verr.Field()
			switch fieldName {
			case "Name":
				verrs = multierror.Append(verrs, errors.New("worker name's max length is 256"))
			case "QueueName":
				verrs = multierror.Append(verrs, errors.New("worker queue name can't include ':' and its max length is 256"))
			case "Concurrency":
				verrs = multierror.Append(verrs, errors.New("worker concurrency must be positive"))
			case "WorkDir":
				verrs = multierror.Append(verrs, errors.New("worker work dir must be existed and writable"))
			default:
				verrs = multierror.Append(verrs, errors.New("unknown validation error"))
			}
		}
		return (*util.ValidationError)(verrs)
	}
	return nil
}
