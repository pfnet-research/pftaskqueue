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

package iface

import (
	"context"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	"github.com/google/uuid"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
	"github.com/pkg/errors"
)

var (
	TaskQueueNotFound         = errors.New("Queue not found")
	TaskQueueExisted          = errors.New("Queue already exists")
	TaskQueueEmptyError       = errors.New("Queue is empty")
	TaskQueueIsTooLarge       = errors.New("Queue have many tasks so we need --without_transaction option")
	TaskSuspendedError        = errors.New("Queue is suspended")
	WorkerNotFound            = errors.New("Worker not found")
	WorkerExitedError         = errors.New("Worker already exists")
	WorkerSalvationNotAllowed = errors.New("Worker salvation not allowed")
)

type Backend interface {
	CreateQueue(ctx context.Context, queueSpec taskqueue.TaskQueueSpec) (*taskqueue.TaskQueue, error)
	GetAllQueues(ctx context.Context) ([]*taskqueue.TaskQueue, error)
	GetQueueByName(ctx context.Context, queueName string) (*taskqueue.TaskQueue, error)
	UpdateQueue(ctx context.Context, queueSpec taskqueue.TaskQueueSpec) (*taskqueue.TaskQueue, error)
	DeleteQueue(ctx context.Context, queueName string) error

	RegisterWorker(ctx context.Context, queueUID uuid.UUID, workerSpec worker.WorkerSpec) (*worker.Worker, error)
	GetAllWorkers(ctx context.Context, queueUID uuid.UUID) ([]*worker.Worker, error)
	GetWorker(ctx context.Context, queueUID, workerUID uuid.UUID) (*worker.Worker, error)
	GetRunningWorkers(ctx context.Context, queueUID uuid.UUID) ([]*worker.Worker, error)
	GetSucceededWorkers(ctx context.Context, queueUID uuid.UUID) ([]*worker.Worker, error)
	GetFailedWorkers(ctx context.Context, queueUID uuid.UUID) ([]*worker.Worker, error)
	GetLostWorker(ctx context.Context, queueUID uuid.UUID) ([]*worker.Worker, error)
	GetWorkersToSalvage(ctx context.Context, queueUID uuid.UUID) ([]*worker.Worker, error)
	SendWorkerHeartBeat(ctx context.Context, queueUID, workerUID uuid.UUID) (*worker.Worker, error)
	SetWorkerSucceeded(ctx context.Context, queueUID, workerUID uuid.UUID) (*worker.Worker, error)
	SetWorkerFailed(ctx context.Context, queueUID, workerUID uuid.UUID) (*worker.Worker, error)
	SalvageWorker(ctx context.Context, queueUID, salvagingWorkerUID, salvageTargetWorkerUID uuid.UUID) (*worker.Worker, []*task.Task, error)

	AddTasks(ctx context.Context, queueName string, specs []task.TaskSpec) ([]*task.Task, error)
	NextTask(ctx context.Context, queueUID, workerUID uuid.UUID) (*task.Task, error)
	GetAllTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	GetProcessingTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	GetPendingTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	GetReceivedTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	GetCompletedTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	GetSucceededTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	GetFailedTasks(ctx context.Context, queueName string) ([]*task.Task, error)
	SetProcessing(ctx context.Context, queueUID, workerUID uuid.UUID, task *task.Task) error
	SetSucceeded(ctx context.Context, queueUID, workerUID uuid.UUID, task *task.Task, resultPayload *string, message *string, onSuccessSpecs []task.TaskSpec) error
	RecordFailure(ctx context.Context, queueUID, workerUID uuid.UUID, task *task.Task, resultPayload *string, message *string, reason task.TaskResultReason, onFailureSpecs []task.TaskSpec) error

	GetDeadLetter(ctx context.Context, queueName string) ([]taskqueue.TaskToDeadletterError, error)
}
