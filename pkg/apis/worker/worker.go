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

package worker

import (
	"errors"
	"io/ioutil"
	"os"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pfnet-research/pftaskqueue/pkg/util"
)

var (
	WorkerPhaseRunning   WorkerPhase = "Running"
	WorkerPhaseSucceeded WorkerPhase = "Succeeded"
	WorkerPhaseFailed    WorkerPhase = "Failed"

	WorkerReasonSuccess  WorkerReason = "Success"
	WorkerReasonFailure  WorkerReason = "Failure"
	WorkerReasonLost     WorkerReason = "Lost"
	WorkerReasonSalvaged WorkerReason = "Salvaged"

	RegisterValidator = func(v *validator.Validate) {
		if err := v.RegisterValidation("isWorkDirValid", IsWorkDirValid); err != nil {
			panic(err)
		}
	}
	Validator = func() *validator.Validate {
		v := validator.New()
		RegisterValidator(v)
		return v
	}()
)

type WorkerPhase string
type WorkerReason string

type Worker struct {
	UID      uuid.UUID `json:"uid" yaml:"uid"`
	QueueUID uuid.UUID `json:"queueUID" yaml:"queueUID"`
	Spec     WorkerSpec
	Status   WorkerStatus
}

type WorkerSpec struct {
	Name                   string          `json:"name" yaml:"name" default:"-" validate:"required,min=1,max=256"`
	Concurrency            int             `json:"concurrency" yaml:"concurrency" default:"1" validate:"required,min=1"`
	TaskHandler            TaskHandlerSpec `json:"taskHandler" yaml:"taskHandler" validate:"required"`
	HeartBeat              HeartBeatSpec   `json:"heartBeat" yaml:"heartBeat" validate:"required"`
	ExitOnSuspend          bool            `json:"exitOnSuspend" yaml:"exitOnSuspend" default:"true"`
	ExitOnEmpty            bool            `json:"exitOnEmpty" yaml:"exitOnEmpty" default:"false"`
	ExitOnEmptyGracePeriod time.Duration   `json:"exitOnEmptyGracePeriod" yaml:"exitOnEmptyGracePeriod" default:"10s"`
	NumTasks               int             `json:"numTasks" yaml:"numTasks" default:"100"`
	WorkDir                string          `json:"workDir" yaml:"workDir" default:"/tmp" validate:"isWorkDirValid,required"`
}

type TaskHandlerSpec struct {
	DefaultCommandTimeout time.Duration `json:"defaultTimeout" yaml:"defaultTimeout" default:"30m" validate:"required"`
	Commands              []string      `json:"commands" yaml:"commands" default:"[\"cat\"]" validate:"required"`
}

type HeartBeatSpec struct {
	SalvageDuration    time.Duration `json:"salvageDuration" yaml:"salvageDuration" default:"15s" validate:"required"`
	ExpirationDuration time.Duration `json:"expirationDuration" yaml:"expirationDuration" default:"10s" validate:"required"`
	Interval           time.Duration `json:"interval" yaml:"interval" default:"2s" validate:"required"`
}

type WorkerStatus struct {
	Phase           WorkerPhase  `json:"phase" yaml:"phase"`
	Reason          WorkerReason `json:"reason,omitempty" yaml:"reason,omitempty"`
	StartedAt       time.Time    `json:"startedAt" yaml:"startedAt"`
	FinishedAt      time.Time    `json:"finishedAt,omitempty" yaml:"finishedAt,omitempty"`
	LastHeartBeatAt time.Time    `json:"lastHeartBeatAt,omitempty" yaml:"lastHeartBeatAt,omitempty"`
	SalvagedBy      *uuid.UUID   `json:"salvagedBy,omitempty" yaml:"salvagedBy,omitempty"`
	SalvagedAt      *time.Time   `json:"salvagedAt,omitempty" yaml:"salvagedAt,omitempty"`
}

func NewWorker(spec WorkerSpec, queueUID uuid.UUID) *Worker {
	now := time.Now()
	return &Worker{
		UID:      uuid.New(),
		QueueUID: queueUID,
		Spec:     spec,
		Status: WorkerStatus{
			Phase:           WorkerPhaseRunning,
			Reason:          WorkerReason(""),
			StartedAt:       now,
			FinishedAt:      now,
			LastHeartBeatAt: now,
		},
	}
}

func (w *Worker) SetLastHeartBeatAt(t time.Time) error {
	if w.Status.Phase != WorkerPhaseRunning {
		return errors.New("Worker must be Running phase to set HeartBeat")
	}
	w.Status.LastHeartBeatAt = t
	return nil
}

func (w *Worker) SetSucceeded(t time.Time) error {
	if w.Status.Phase != WorkerPhaseRunning {
		return errors.New("Worker must be Running phase to set Succeeded")
	}
	w.Status.Phase = WorkerPhaseSucceeded
	w.Status.Reason = WorkerReasonSuccess
	w.Status.FinishedAt = t
	w.Status.LastHeartBeatAt = t
	return nil
}

func (w *Worker) SetFailed(t time.Time, reason WorkerReason) error {
	if w.Status.Phase != WorkerPhaseRunning {
		return errors.New("Worker must be Running phase to set Failed")
	}
	w.Status.Phase = WorkerPhaseFailed
	w.Status.Reason = reason
	w.Status.FinishedAt = t
	w.Status.LastHeartBeatAt = t
	return nil
}

func (w *Worker) IsLostOn(t time.Time) bool {
	switch w.Status.Phase {
	case WorkerPhaseSucceeded:
		return false
	case WorkerPhaseFailed:
		if w.Status.Reason == WorkerReasonLost {
			return true
		}
		return false
	default: // Running
		if w.Status.LastHeartBeatAt.Add(w.Spec.HeartBeat.ExpirationDuration).Before(t) {
			return true
		}
		return false
	}
}

func (w *Worker) AllowToSalvageOn(t time.Time) bool {
	return w.IsLostOn(t) && w.Status.LastHeartBeatAt.
		Add(w.Spec.HeartBeat.ExpirationDuration).
		Add(w.Spec.HeartBeat.SalvageDuration).Before(t)
}

func (w *Worker) UpdateStatusIfLostOn(t time.Time) {
	if w.IsLostOn(t) {
		w.Status.Phase = WorkerPhaseFailed
		w.Status.Reason = WorkerReasonLost
	}
}

func (w *Worker) SetSalvagedByAndOn(by uuid.UUID, on time.Time) {
	w.Status.Phase = WorkerPhaseFailed
	w.Status.Reason = WorkerReasonSalvaged
	w.Status.SalvagedBy = &by
	w.Status.SalvagedAt = &on
}

func (spec WorkerSpec) Validate() error {
	err := Validator.Struct(&spec)
	if err != nil {
		var verrs *multierror.Error
		for _, verr := range err.(validator.ValidationErrors) {
			fieldName := verr.Field()
			switch fieldName {
			case "Name":
				verrs = multierror.Append(verrs, errors.New("worker name's max length is 256"))
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

// Exists && IsDir && Writable
func IsWorkDirValid(fl validator.FieldLevel) bool {
	path := fl.Field().String()

	// follow symlink and convert clean absolute path
	absPath, err := util.ResolveRealAbsPath(path)
	if err != nil {
		return false
	}

	if !util.IsPathExists(absPath) {
		return false
	}

	fileinfo, _ := os.Stat(absPath)
	if !fileinfo.IsDir() {
		return false
	}

	// writable and dir creatable
	touchFile, err := ioutil.TempFile(absPath, ".pftaskqueue_touch_")
	if err != nil {
		return false
	}
	defer os.Remove(touchFile.Name())

	touchDir, err := ioutil.TempDir(absPath, ".pftaskqueue_touchdir_")
	if err != nil {
		return false
	}
	defer os.RemoveAll(touchDir)

	return true
}
