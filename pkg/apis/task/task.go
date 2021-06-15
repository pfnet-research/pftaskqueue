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

package task

import (
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type TaskPhase string
type TaskResultType string
type TaskResultReason string

const (
	TaskPhasePending    TaskPhase = "Pending"
	TaskPhaseReceived   TaskPhase = "Received"
	TaskPhaseProcessing TaskPhase = "Processing"
	TaskPhaseSucceeded  TaskPhase = "Succeeded"
	TaskPhaseFailed     TaskPhase = "Failed"

	TaskResultSuccess TaskResultType = "Success"
	TaskResultFailure TaskResultType = "Failure"

	TaskResultReasonSucceded      TaskResultReason = "Succeeded"
	TaskResultReasonSignaled      TaskResultReason = "Signaled"
	TaskResultReasonTimeout       TaskResultReason = "Timeout"
	TaskResultReasonFailed        TaskResultReason = "Failed"
	TaskResultReasonInternalError TaskResultReason = "InternalError"
)

type Task struct {
	// TODO change the type to uuid.UUID
	UID       string     `json:"uid" yaml:"uid"`
	ParentUID *string    `json:"parentUID,omitempty" yaml:"parentUID,omitempty"`
	Spec      TaskSpec   `json:"spec" yaml:"spec"`
	Status    TaskStatus `json:"status" yaml:"status"`
}

type TaskSpec struct {
	Name           string `json:"name" yaml:"name,omitempty"`
	Payload        string `json:"payload" yaml:"payload"`
	RetryLimit     int    `json:"retryLimit,omitempty" yaml:"retryLimit,omitempty"`
	TimeoutSeconds int    `json:"timeoutSeconds,omitempty" yaml:"timeoutSeconds,omitempty"`
}

func (s TaskSpec) ActualTimeout(defaultTimeout time.Duration) time.Duration {
	timeout := time.Duration(s.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return timeout
}

type TaskStatus struct {
	Phase         TaskPhase    `json:"phase" yaml:"phase"`
	CreatedAt     time.Time    `json:"createdAt" yaml:"createdAt"`
	CurrentRecord *TaskRecord  `json:"currentWork,omitempty" yaml:"currentWork,omitempty"`
	FailureCount  int          `json:"failureCount" yaml:"failureCount"`
	SalvageCount  int          `json:"salvageCount" yaml:"salvageCount"`
	History       []TaskRecord `json:"history,omitempty" yaml:"history,omitempty"`
}

type TaskRecord struct {
	WorkerUID  string      `json:"workerUID" yaml:"workerUID"`
	WorkerName string      `json:"workerName" yaml:"workerName"`
	ProcessUID string      `json:"processUID" yaml:"processUID"`
	ReceivedAt time.Time   `json:"receivedAt" yaml:"receivedAt"`
	StartedAt  *time.Time  `json:"startedAt,omitempty" yaml:"startedAt,omitempty"`
	FinishedAt *time.Time  `json:"finishedAt,omitempty" yaml:"finishedAt,omitempty"`
	Result     *TaskResult `json:"result,omitempty" yaml:"result,omitempty"`
	SalvagedBy *uuid.UUID  `json:"salvagedBy,omitempty" yaml:"salvagedBy,omitempty"`
	SalvagedAt *time.Time  `json:"salvagedAt,omitempty" yaml:"salvagedAt,omitempty"`
}

type TaskResult struct {
	Type    TaskResultType   `json:"type" yaml:"type"`
	Reason  TaskResultReason `json:"reason" yaml:"reason"`
	Message *string          `json:"message,omitempty" yaml:"message,omitempty"`
	Payload *string          `json:"payload,omitempty" yaml:"payload,omitempty"`
}

func newPendingStatus() TaskStatus {
	return TaskStatus{
		Phase:     TaskPhasePending,
		CreatedAt: time.Now(),
	}
}

func (t *Task) newRecord(workerName, workerUID string) *TaskRecord {
	return &TaskRecord{
		WorkerUID:  workerUID,
		WorkerName: workerName,
		ProcessUID: uuid.New().String(),
		ReceivedAt: time.Now(),
	}
}

func NewTask(spec TaskSpec, parent *Task) *Task {
	if parent == nil {
		return &Task{
			UID:       uuid.New().String(),
			ParentUID: nil,
			Spec:      spec,
			Status:    newPendingStatus(),
		}
	}
	parentUID := parent.UID
	return &Task{
		UID:       uuid.New().String(),
		ParentUID: &parentUID,
		Spec:      spec,
		Status:    newPendingStatus(),
	}
}

func (t *Task) SetReceived(workerName, workerUID string) error {
	if t.Status.Phase != TaskPhasePending {
		return errors.Errorf("invalid status: actual=%s expected=%s", t.Status.Phase, TaskPhasePending)
	}
	if t.Status.CurrentRecord != nil {
		return errors.New("invalid status: currentRecord is not nil")
	}

	t.Status.Phase = TaskPhaseReceived
	t.Status.CurrentRecord = t.newRecord(workerName, workerUID)
	return nil
}

func (t *Task) SetProcessing() error {
	if t.Status.Phase != TaskPhaseReceived {
		return errors.Errorf("invalid status: actual=%s expected=%s", t.Status.Phase, TaskPhaseReceived)
	}
	if t.Status.CurrentRecord == nil {
		return errors.New("invalid status: currentWork is nil")
	}

	now := time.Now()
	t.Status.Phase = TaskPhaseProcessing
	t.Status.CurrentRecord.StartedAt = &now

	return nil
}

func (t *Task) IsWorkerLost(defaultTimeoutSeconds int) bool {
	timeout := time.Duration(t.Spec.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = time.Duration(defaultTimeoutSeconds) * time.Second
	}

	if !(t.Status.Phase == TaskPhaseReceived || t.Status.Phase == TaskPhaseProcessing) {
		return false
	}
	if t.Status.CurrentRecord == nil {
		return false
	}

	if t.Status.CurrentRecord.StartedAt != nil {
		return t.Status.CurrentRecord.StartedAt.Add(timeout).Before(time.Now())
	}

	return t.Status.CurrentRecord.ReceivedAt.Add(timeout).Before(time.Now())
}

func (t *Task) SetSuccess(payload *string, message *string, historyLengthLimit int) error {
	if t.Status.Phase != TaskPhaseProcessing {
		return errors.Errorf("invalid status: actual=%s expected=%s", t.Status.Phase, TaskPhaseProcessing)
	}
	if t.Status.CurrentRecord == nil {
		return errors.New("invalid status: currentWork is nil")
	}

	now := time.Now()
	t.Status.Phase = TaskPhaseSucceeded

	current := t.Status.CurrentRecord
	t.Status.CurrentRecord = nil

	current.Result = &TaskResult{
		Type:    TaskResultSuccess,
		Reason:  TaskResultReasonSucceded,
		Message: message,
		Payload: payload,
	}
	current.FinishedAt = &now

	if t.Status.History == nil {
		t.Status.History = []TaskRecord{}
	}
	t.Status.History = append(t.Status.History, *current)

	if len(t.Status.History) > historyLengthLimit {
		t.Status.History = t.Status.History[len(t.Status.History)-historyLengthLimit:]
	}

	return nil
}

func (t *Task) RecordFailure(reason TaskResultReason, payload *string, message *string, historyLengthLimit int) (bool, error) {
	if t.Status.Phase != TaskPhaseProcessing && t.Status.Phase != TaskPhaseReceived {
		return false, errors.Errorf("invalid status: actual=%s expected=[%s,%s]", t.Status.Phase, TaskPhaseProcessing, TaskPhaseReceived)
	}
	if t.Status.CurrentRecord == nil {
		return false, errors.Errorf("invalid status: currentWork is nil")
	}

	now := time.Now()

	current := t.Status.CurrentRecord
	t.Status.CurrentRecord = nil

	current.Result = &TaskResult{
		Type:    TaskResultFailure,
		Reason:  reason,
		Message: message,
		Payload: payload,
	}
	current.FinishedAt = &now

	if t.Status.History == nil {
		t.Status.History = []TaskRecord{}
	}
	t.Status.History = append(t.Status.History, *current)
	if len(t.Status.History) > historyLengthLimit {
		t.Status.History = t.Status.History[len(t.Status.History)-historyLengthLimit:]
	}

	t.Status.FailureCount = t.Status.FailureCount + 1

	requeue := true
	t.Status.Phase = TaskPhasePending
	// no requeue because retry exceeded
	if t.Spec.RetryLimit >= 0 && t.Status.FailureCount > t.Spec.RetryLimit {
		requeue = false
		t.Status.Phase = TaskPhaseFailed
	}

	return requeue, nil
}

func (t *Task) SetSalvagedByOn(by uuid.UUID, on time.Time) bool {
	if t.Status.Phase == TaskPhaseFailed || t.Status.Phase == TaskPhaseSucceeded {
		return false
	}

	if t.Status.Phase == TaskPhasePending {
		return true
	}

	// Received or Processing
	current := t.Status.CurrentRecord
	t.Status.CurrentRecord = nil
	current.SalvagedBy = &by
	current.SalvagedAt = &on
	t.Status.SalvageCount = t.Status.SalvageCount + 1
	if t.Status.History == nil {
		t.Status.History = []TaskRecord{}
	}
	t.Status.History = append(t.Status.History, *current)
	t.Status.Phase = TaskPhasePending

	return true
}
