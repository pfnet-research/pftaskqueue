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

package taskqueue

import (
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pfnet-research/pftaskqueue/pkg/util"
	"github.com/pkg/errors"
)

const (
	TaskQueueStateActive  TaskQueueState = "active"
	TaskQueueStateSuspend TaskQueueState = "suspend"
)

var (
	RegisterValidator = func(v *validator.Validate) {
		if err := v.RegisterValidation("isValidTaskQueueState", ValidTaskQueueState); err != nil {
			panic(err)
		}
	}
	Validator = func() *validator.Validate {
		v := validator.New()
		RegisterValidator(v)
		return v
	}()
)

type TaskQueue struct {
	// UID is uuid of the queue. This can distinguish two queues with the same name in different lifecycle.
	UID    uuid.UUID `json:"uid" yaml:"uid" validate:"required"`
	Spec   TaskQueueSpec
	Status TaskQueueStatus
}
type TaskQueueStatus struct {
	// CreatedAt is the timestamp of this queue
	CreatedAt time.Time `json:"createdAt,omitempty" yaml:"createdAt,omitempty"`
	// UpdatedAt is the last updated timestamp
	UpdatedAt time.Time `json:"updatedAt,omitempty" yaml:"updatedAt,omitempty"`
}
type TaskQueueState string

func NewTaskQueue(queueSpec TaskQueueSpec) *TaskQueue {
	now := time.Now()
	return &TaskQueue{
		UID:  uuid.New(),
		Spec: queueSpec,
		Status: TaskQueueStatus{
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

}

type TaskQueueSpec struct {
	Name  string         `json:"name" validate:"required,min=1,max=256,excludesall=:"`
	State TaskQueueState `json:"state" validate:"isValidTaskQueueState,required"`
}

func ValidTaskQueueState(fl validator.FieldLevel) bool {
	return fl.Field().String() == string(TaskQueueStateActive) || fl.Field().String() == string(TaskQueueStateSuspend)
}

func NewTaskQueueSpec(name string, state TaskQueueState) TaskQueueSpec {
	return TaskQueueSpec{
		Name:  name,
		State: state,
	}
}

func (s TaskQueueSpec) Validate() *util.ValidationError {
	err := Validator.Struct(&s)
	if err != nil {
		var verrs *multierror.Error
		for _, verr := range err.(validator.ValidationErrors) {
			fieldName := verr.Field()
			switch fieldName {
			case "Name":
				verrs = multierror.Append(verrs, errors.New("queue name can't include ':' and its max length is 256"))
			case "State":
				verrs = multierror.Append(verrs, errors.New("queue state must be one of ['active' or 'suspend']"))
			default:
				verrs = multierror.Append(verrs, errors.Wrapf(err, "unknown validation error"))
			}
		}
		return (*util.ValidationError)(verrs)
	}
	return nil
}

func ValidateQueueName(queueName string) *util.ValidationError {
	temp := NewTaskQueueSpec(queueName, TaskQueueStateActive)
	if err := Validator.StructPartial(&temp, "Name"); err != nil {
		var verrs *multierror.Error
		for _, verr := range err.(validator.ValidationErrors) {
			fieldName := verr.Field()
			switch fieldName {
			case "Name":
				verrs = multierror.Append(verrs, errors.New("queue-name can't include ':' and its max length is 256"))
			default:
				verrs = multierror.Append(verrs, errors.Wrapf(err, "unknown validation error"))
			}
		}
		return (*util.ValidationError)(verrs)
	}
	return nil
}
