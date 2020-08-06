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
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/worker"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
	"github.com/pfnet-research/pftaskqueue/pkg/util"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

const (
	KB                 = 1 << 10
	PayloadMaxSizeInKB = 1
	MessageMaxSizeInKB = 1
	RetryLimitMax      = 10
	MaxNameLength      = 1024
)

func (b *Backend) ensureQueueAndWorkerExists(queueUID, workerUID uuid.UUID) (*taskqueue.TaskQueue, *worker.Worker, error) {
	queue, err := b.ensureQueueExistsByUID(b.Client, queueUID.String())
	if err != nil {
		return nil, nil, err
	}
	worker, err := b.ensureWorkerExistsByUID(b.Client, queue, workerUID.String())
	if err != nil {
		return nil, nil, err
	}
	return queue, worker, nil
}

func (b *Backend) AddTask(ctx context.Context, queueName string, spec task.TaskSpec) (*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	if err := b.validateTaskSpec(spec); err != nil {
		return nil, err
	}

	newTask := task.NewTask(spec, nil)
	marshaled, err := json.Marshal(newTask)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshal")
	}

	// WATCH {tasks_key} {task_key}
	// MULTI
	// SADD {tasks_key} {taskUID}
	// LPUSH {pending_queue_key} {taskUID}
	// SET {task_key} {task}
	// EXEC
	watchingKeys := []string{
		b.tasksKey(queue.UID.String()),
		b.taskKey(queue.UID.String(), newTask.UID),
	}
	logger := b.Logger.With().
		Str("queue", queueName).
		Str("queueUID", queue.UID.String()).
		Str("taskUID", newTask.UID).
		Str("operation", "AddTask").
		Logger()
	txf := func(tx *redis.Tx) error {
		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.SAdd(b.tasksKey(queue.UID.String()), newTask.UID)
			pipe.LPush(b.pendingTaskQueueKey(queue.UID.String()), newTask.UID)
			pipe.Set(b.taskKey(queue.UID.String(), newTask.UID), string(marshaled), -1)
			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(ctx, logger, txf, watchingKeys...)
	if err != nil {
		return nil, err
	}
	return newTask, nil
}

func (b *Backend) getTasksByUIDs(queueUID string, taskUIDs []string, filter func(*task.Task) bool, lggr zerolog.Logger) ([]*task.Task, error) {
	if len(taskUIDs) == 0 {
		return []*task.Task{}, nil
	}
	taskKeys := []string{}
	for _, uid := range taskUIDs {
		taskKeys = append(taskKeys, b.taskKey(queueUID, uid))
	}
	rawTasks, err := b.Client.MGet(taskKeys...).Result()
	if err != nil {
		return nil, err
	}
	tasks := []*task.Task{}
	for _, rawTask := range rawTasks {
		if rawTask == nil {
			lggr.Error().
				Interface("rawData", nil).
				Msg("Internal error. rawData is null. Skipped.")
			continue
		}
		var t task.Task
		rawTaskStr, ok := rawTask.(string)
		if !ok {
			lggr.Error().
				Interface("rawData", rawTask).
				Str("type", reflect.TypeOf(rawTask).String()).
				Msg("Internal error. rawData should be string. Skipped.")
			continue
		}
		if err := json.Unmarshal([]byte(rawTaskStr), &t); err != nil {
			lggr.Error().Str("data", rawTaskStr).Msg("Failed to unmarshal to Task. Skipped.")
			continue
		}
		if filter(&t) {
			tasks = append(tasks, &t)
		}
	}
	return tasks, nil
}

func (b *Backend) getTasks(queueUID string, filter func(*task.Task) bool, lggr zerolog.Logger) ([]*task.Task, error) {
	taskUIDs, err := b.Client.SMembers(b.tasksKey(queueUID)).Result()
	if err == redis.Nil {
		return []*task.Task{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(taskUIDs) == 0 {
		return []*task.Task{}, nil
	}

	return b.getTasksByUIDs(queueUID, taskUIDs, filter, lggr)
}

func (b *Backend) GetAllTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return true
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetAllTasks").
			Logger(),
	)
}

func (b *Backend) GetPendingTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return t.Status.Phase == task.TaskPhasePending
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetPendingTasks").
			Logger(),
	)
}

func (b *Backend) GetReceivedTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return t.Status.Phase == task.TaskPhaseReceived
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetReceivedTasks").
			Logger(),
	)
}

func (b *Backend) GetProcessingTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return t.Status.Phase == task.TaskPhaseProcessing
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetProcessingTasks").
			Logger(),
	)
}

func (b *Backend) GetCompletedTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return t.Status.Phase == task.TaskPhaseSucceeded || t.Status.Phase == task.TaskPhaseFailed
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetCompletedTasks").
			Logger(),
	)
}

func (b *Backend) GetSucceededTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return t.Status.Phase == task.TaskPhaseSucceeded
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetSucceededTasks").
			Logger(),
	)
}

func (b *Backend) GetFailedTasks(ctx context.Context, queueName string) ([]*task.Task, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	return b.getTasks(
		queue.UID.String(),
		func(t *task.Task) bool {
			return t.Status.Phase == task.TaskPhaseFailed
		},
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "GetFailedTasks").
			Logger(),
	)
}

func (b *Backend) GetDeadLetter(ctx context.Context, queueName string) ([]taskqueue.TaskToDeadletterError, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return nil, err
	}
	logger := b.Logger.With().
		Str("queueName", queue.Spec.Name).
		Str("queueUID", queue.UID.String()).
		Str("operation", "GetDeadLetter").
		Logger()

	values, err := b.Client.LRange(b.deadletterQueueKey(queue.UID.String()), 0, -1).Result()
	if err == redis.Nil {
		return []taskqueue.TaskToDeadletterError{}, nil
	}
	if err != nil {
		return nil, err
	}
	dlErrors := []taskqueue.TaskToDeadletterError{}
	for _, value := range values {
		var dlError taskqueue.TaskToDeadletterError
		if err := json.Unmarshal([]byte(value), &dlError); err != nil {
			logger.Error().Err(err).Str("bytes", value).Msg("can't unmarshal dead letter item")
		}
		dlErrors = append(dlErrors, dlError)
	}
	return dlErrors, nil
}

func (b *Backend) NextTask(ctx context.Context, queueUID, workerUID uuid.UUID) (*task.Task, error) {
	queue, worker, err := b.ensureQueueAndWorkerExists(queueUID, workerUID)
	if err != nil {
		return nil, err
	}
	logger := b.Logger.With().
		Str("queueName", queue.Spec.Name).
		Str("queueUID", queue.UID.String()).
		Str("workerName", worker.Spec.Name).
		Str("workerUID", worker.UID.String()).
		Str("operation", "NextTask").Logger()

	queueKey := b.queueKey(queue.UID.String())
	pendingQueueKey := b.pendingTaskQueueKey(queue.UID.String())
	workerPendingQueueKey := b.workerPendingTaskQueueKey(queue.UID.String(), worker.UID.String())
	queueTasksKey := b.tasksKey(queue.UID.String())
	workerTasksKey := b.workerTasksKey(queue.UID.String(), worker.UID.String())

	// ---------------------------------------
	// Next Task performs 2 transactions
	// ---------------------------------------
	// 1st transaction
	// # fetch from queue-level pending tasks if worker-level pending queue was empty
	// # this is queue-level transaction
	// WATCH {queue_key} {worker_pending_key}
	// GET {queue_key}
	// .. abort if suspended
	// LLEN {worker_pending_key}
	// .. if 0
	//   MULTI
	//   RPOPLPHSH pending worker_pending
	//   EXEC
	err = b.runTxWithBackOff(ctx, logger, func(tx *redis.Tx) error {
		_queue, err := b.ensureQueueExistsByUID(tx, queue.UID.String())
		if err != nil {
			return backoff.Permanent(err)
		}
		if _queue.Spec.State != taskqueue.TaskQueueStateActive {
			return backoff.Permanent(iface.TaskSuspendedError)
		}
		toPop := false
		numWorkerPending, err := tx.LLen(workerPendingQueueKey).Result()
		if err == redis.Nil {
			toPop = true
		}
		if err != nil {
			return err
		}
		if numWorkerPending == 0 {
			toPop = true
		}
		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			if toPop {
				pipe.RPopLPush(pendingQueueKey, workerPendingQueueKey)
			}
			return nil
		})
		if err == redis.Nil {
			return backoff.Permanent(iface.TaskQueueEmptyError)
		}
		return err
	}, queueKey, workerPendingQueueKey)
	if err != nil {
		return nil, err
	}

	// # 2nd transaction (will happen if 1st succeeded)
	// # this is closed to specific workerUID and queue_state
	// WATCH {worker_pending_key} {worker_tasks_key}
	// LRANGE {worker_pending_key} -1 -1
	// .. return empty error if 0
	// WATCH {task_key}
	// GET {task_key}
	// .. set received
	// .... if error happened deliver to deadletter
	// MULTI
	// RPOP {worker_pending_key}
	// SADD {worker_tasks_key} {taskUID}
	// SET {taskUID} {task}
	// EXEC
	var t *task.Task
	err = b.runTxWithBackOff(ctx, logger,
		func(tx *redis.Tx) error {
			preWatchDeliverDeadLetter := func() {
				tx.Watch(workerPendingQueueKey)
			}
			preCmdDeliverDeadLetter := func(taskUID *string) func(pipe redis.Pipeliner) {
				return func(pipe redis.Pipeliner) {
					pipe.RPop(workerPendingQueueKey)
					if taskUID != nil {
						pipe.SRem(queueTasksKey, t.UID)
						pipe.SRem(workerTasksKey, t.UID)
						pipe.Del(b.taskKey(queue.UID.String(), t.UID))
					}
				}
			}
			// peak worker pending queue
			taskUIDs, err := tx.LRange(workerPendingQueueKey, -1, -1).Result()
			if err == redis.Nil {
				return backoff.Permanent(iface.TaskQueueEmptyError)
			}
			if err != nil {
				return err
			}
			if len(taskUIDs) == 0 {
				return backoff.Permanent(iface.TaskQueueEmptyError)
			}

			// get Task and transit to Received
			raw, err := tx.Get(b.taskKey(queue.UID.String(), taskUIDs[0])).Result()
			if err != nil {
				return err
			}
			t = &task.Task{}
			if err := json.Unmarshal([]byte(raw), t); err != nil {
				preWatchDeliverDeadLetter()
				return b.deliverToDeadLetter(tx, queue.UID.String(),
					b.notUnMarshalableDLError(err, raw),
					preCmdDeliverDeadLetter(nil),
				)
			}
			if err := t.SetReceived(worker.Spec.Name, worker.UID.String()); err != nil {
				preWatchDeliverDeadLetter()
				return b.deliverToDeadLetter(tx, queue.UID.String(),
					b.invalidMessageDLError(errors.Wrap(err, "can't mark Received"), t),
					preCmdDeliverDeadLetter(&t.UID),
				)
			}
			marshaled, err := json.Marshal(t)
			if err != nil {
				preWatchDeliverDeadLetter()
				return b.deliverToDeadLetter(tx, queue.UID.String(),
					b.notMarshalableDLError(err, t),
					preCmdDeliverDeadLetter(&t.UID),
				)
			}

			// commit
			_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
				pipe.RPop(workerPendingQueueKey)
				pipe.SAdd(workerTasksKey, t.UID)
				pipe.Set(b.taskKey(queue.UID.String(), t.UID), string(marshaled), -1)
				return nil
			})
			return err
		},
		workerPendingQueueKey, workerTasksKey,
	)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (b *Backend) SetProcessing(ctx context.Context, queueUID, workerUID uuid.UUID, t *task.Task) error {
	queue, worker, err := b.ensureQueueAndWorkerExists(queueUID, workerUID)
	if err != nil {
		return err
	}
	logger := b.Logger.With().
		Str("queueName", queue.Spec.Name).
		Str("queueUID", queue.UID.String()).
		Str("workerName", worker.Spec.Name).
		Str("workerUID", worker.UID.String()).
		Str("operation", "SetProcessing").Logger()

	workerPendingQueueKey := b.workerPendingTaskQueueKey(queue.UID.String(), worker.UID.String())
	queueTasksKey := b.tasksKey(queue.UID.String())
	workerTasksKey := b.workerTasksKey(queue.UID.String(), worker.UID.String())
	taskKey := b.taskKey(queue.UID.String(), t.UID)

	// WATCH {task_key}
	// GET {task_key}
	// ...set processing state...
	// ...deliver to deadletter if error
	// MULTI
	// SET {task_key} {task}
	// EXEC
	watchingKeys := []string{taskKey}
	txf := func(tx *redis.Tx) error {
		preWatchDeliverDeadLetter := func() {
			tx.Watch(workerPendingQueueKey)
		}
		preCmdDeliverDeadLetter := func(taskUID *string) func(pipe redis.Pipeliner) {
			return func(pipe redis.Pipeliner) {
				pipe.RPop(workerPendingQueueKey)
				if taskUID != nil {
					pipe.SRem(queueTasksKey, t.UID)
					pipe.SRem(workerTasksKey, t.UID)
					pipe.Del(b.taskKey(queue.UID.String(), t.UID))
				}
			}
		}

		// TODO: check raw == t
		raw, err := tx.Get(taskKey).Result()
		if err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			preWatchDeliverDeadLetter()
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				b.notUnMarshalableDLError(err, raw),
				preCmdDeliverDeadLetter(nil),
			)
		}
		if err := t.SetProcessing(); err != nil {
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				b.invalidMessageDLError(errors.Wrap(err, "can't mark Processing"), t),
				preCmdDeliverDeadLetter(&t.UID),
			)
		}
		marshaled, err := json.Marshal(t)
		if err != nil {
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				b.notMarshalableDLError(err, t),
				preCmdDeliverDeadLetter(&t.UID),
			)
		}

		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.Set(taskKey, string(marshaled), 0)
			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(ctx, logger, txf, watchingKeys...)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) SetSucceeded(ctx context.Context, queueUID, workerUID uuid.UUID, t *task.Task, resultPayload *string, message *string, onSuccessSpecs []task.TaskSpec) error {
	queue, worker, err := b.ensureQueueAndWorkerExists(queueUID, workerUID)
	if err != nil {
		return err
	}
	logger := b.Logger.With().
		Str("queueName", queue.Spec.Name).
		Str("queueUID", queue.UID.String()).
		Str("workerName", worker.Spec.Name).
		Str("workerUID", worker.UID.String()).
		Str("operation", "SetSucceeded").Logger()

	workerPendingQueueKey := b.workerPendingTaskQueueKey(queue.UID.String(), worker.UID.String())
	queueTasksKey := b.tasksKey(queue.UID.String())
	workerTasksKey := b.workerTasksKey(queue.UID.String(), worker.UID.String())
	taskKey := b.taskKey(queue.UID.String(), t.UID)

	// WATCH  {worker_tasks_key} {task_key}
	// MULTI
	// SET {task_key} {updated_task}
	// SREM {worker_tasks_key} {taskUID}
	// if hooks
	//   SADD {hookTaskUIDs}
	//   SET {hookTaskUIDs} {hookTasks}
	//   LPUSH {hookTaskUIDs}
	// EXEC
	watchingKeys := []string{
		workerTasksKey, taskKey,
	}

	var deadletterError error
	txf := func(tx *redis.Tx) error {
		deadletterError = nil
		preWatchDeliverDeadLetter := func() {
			tx.Watch(workerPendingQueueKey)
		}
		preCmdDeliverDeadLetter := func(pipe redis.Pipeliner) {
			pipe.RPop(workerPendingQueueKey)
			pipe.SRem(queueTasksKey, t.UID)
			pipe.SRem(workerTasksKey, t.UID)
			pipe.Del(b.taskKey(queue.UID.String(), t.UID))
		}

		// TODO: check raw == t
		raw, err := tx.Get(b.taskKey(queue.UID.String(), t.UID)).Result()
		if err != nil {
			return err
		}

		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			dlerr := b.notUnMarshalableDLError(err, raw)
			deadletterError = dlerr
			preWatchDeliverDeadLetter()
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				dlerr,
				preCmdDeliverDeadLetter,
			)
		}

		err = t.SetSuccess(
			util.Truncate(resultPayload, PayloadMaxSizeInKB*KB),
			util.Truncate(message, MessageMaxSizeInKB*KB),
		)
		if err != nil {
			dlerr := b.invalidMessageDLError(
				errors.Wrapf(err, "can't mark Success: payload=%v message=%v", resultPayload, message),
				t,
			)
			deadletterError = dlerr
			preWatchDeliverDeadLetter()
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				dlerr,
				preCmdDeliverDeadLetter,
			)
		}

		marshaledTask, err := json.Marshal(t)
		if err != nil {
			dlerr := b.notMarshalableDLError(err, t)
			deadletterError = dlerr
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				dlerr,
				preCmdDeliverDeadLetter,
			)
		}

		// generate success hook tasks
		marshaledOnSuccessUIDs := []string{}
		marshaledOnSuccess := []string{}
		onSuccessDL := []interface{}{}
		for _, spec := range onSuccessSpecs {
			if err := b.validateTaskSpec(spec); err != nil {
				spec.Payload = *util.Truncate(&spec.Payload, PayloadMaxSizeInKB)
				onSuccessDL = append(onSuccessDL, b.invalidMessageDLError(
					errors.Wrapf(err, "validation error"),
					task.NewTask(spec, t),
				).DLItem())
				continue
			}

			newTask := task.NewTask(spec, t)
			marshaled, err := json.Marshal(newTask)
			if err != nil {
				onSuccessDL = append(onSuccessDL, b.notMarshalableDLError(err, newTask).DLItem())
				continue
			}
			marshaledOnSuccess = append(marshaledOnSuccess, string(marshaled))
			marshaledOnSuccessUIDs = append(marshaledOnSuccessUIDs, newTask.UID)
		}

		// del & set & lpush
		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.SRem(workerTasksKey, t.UID)
			pipe.Set(taskKey, string(marshaledTask), -1)

			for i, marshaled := range marshaledOnSuccess {
				uid := marshaledOnSuccessUIDs[i]
				pipe.SAdd(queueTasksKey, uid)
				pipe.Set(b.taskKey(queue.UID.String(), uid), marshaled, -1)
				pipe.LPush(b.pendingTaskQueueKey(queue.UID.String()), uid)
			}

			if len(onSuccessDL) > 0 {
				pipe.LPush(b.deadletterQueueKey(queue.UID.String()), onSuccessDL...)
			}

			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(ctx, logger, txf, watchingKeys...)
	if err != nil {
		return err
	}
	if deadletterError != nil {
		return deadletterError
	}
	return nil
}

func (b *Backend) RecordFailure(ctx context.Context, queueUID, workerUID uuid.UUID, t *task.Task, resultPayload *string, message *string, reason task.TaskResultReason, onFailureSpecs []task.TaskSpec) error {
	queue, worker, err := b.ensureQueueAndWorkerExists(queueUID, workerUID)
	if err != nil {
		return err
	}
	logger := b.Logger.With().
		Str("queueName", queue.Spec.Name).
		Str("queueUID", queue.UID.String()).
		Str("workerName", worker.Spec.Name).
		Str("workerUID", worker.UID.String()).
		Str("operation", "RecordFailure").Logger()

	workerPendingQueueKey := b.workerPendingTaskQueueKey(queue.UID.String(), worker.UID.String())
	queueTasksKey := b.tasksKey(queue.UID.String())
	workerTasksKey := b.workerTasksKey(queue.UID.String(), worker.UID.String())
	taskKey := b.taskKey(queue.UID.String(), t.UID)

	// WATCH  {worker_tasks_key} {task_key}
	// MULTI
	// SET {task_key} {updated_task}
	// SREM {worker_tasks_key} {taskUID}
	// if hooks
	//   SADD {hookTaskUIDs}
	//   SET {hookTaskUIDs} {hookTasks}
	//   LPUSH {hookTaskUIDs}
	// EXEC
	watchingKeys := []string{
		workerTasksKey, taskKey,
	}

	var deadletterError error
	txf := func(tx *redis.Tx) error {
		deadletterError = nil
		preWatchDeliverDeadLetter := func() {
			tx.Watch(workerPendingQueueKey)
		}
		preCmdDeliverDeadLetter := func(pipe redis.Pipeliner) {
			pipe.RPop(workerPendingQueueKey)
			pipe.SRem(queueTasksKey, t.UID)
			pipe.SRem(workerTasksKey, t.UID)
			pipe.Del(b.taskKey(queue.UID.String(), t.UID))
		}

		// TODO: check raw == t
		raw, err := tx.Get(b.taskKey(queue.UID.String(), t.UID)).Result()
		if err != nil {
			return err
		}

		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			dlerr := b.notUnMarshalableDLError(err, raw)
			deadletterError = dlerr
			preWatchDeliverDeadLetter()
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				dlerr,
				preCmdDeliverDeadLetter,
			)
		}

		requeue, err := t.RecordFailure(
			reason,
			util.Truncate(resultPayload, PayloadMaxSizeInKB*KB),
			util.Truncate(message, MessageMaxSizeInKB*KB),
		)
		if err != nil {
			dlerr := b.invalidMessageDLError(
				errors.Wrapf(err, "can't record Failure: payload=%v message=%v", resultPayload, message),
				t,
			)
			deadletterError = dlerr
			preWatchDeliverDeadLetter()
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				dlerr,
				preCmdDeliverDeadLetter,
			)
		}

		marshaledTask, err := json.Marshal(t)
		if err != nil {
			dlerr := b.notMarshalableDLError(err, t)
			deadletterError = dlerr
			return b.deliverToDeadLetter(tx, queue.UID.String(),
				dlerr,
				preCmdDeliverDeadLetter,
			)
		}

		// generate success hook tasks
		marshaledOnFailureUIDs := []string{}
		marshaledOnFailure := []string{}
		onFailureDL := []interface{}{}
		for _, spec := range onFailureSpecs {
			if err := b.validateTaskSpec(spec); err != nil {
				spec.Payload = *util.Truncate(&spec.Payload, PayloadMaxSizeInKB)
				onFailureDL = append(onFailureDL, b.invalidMessageDLError(
					errors.Wrapf(err, "validation error"),
					task.NewTask(spec, t),
				).DLItem())
				continue
			}

			newTask := task.NewTask(spec, t)
			marshaled, err := json.Marshal(newTask)
			if err != nil {
				onFailureDL = append(onFailureDL, b.notMarshalableDLError(err, newTask).DLItem())
				continue
			}
			marshaledOnFailure = append(marshaledOnFailure, string(marshaled))
			marshaledOnFailureUIDs = append(marshaledOnFailureUIDs, newTask.UID)
		}

		// del & set & lpush
		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.SRem(workerTasksKey, t.UID)
			pipe.Set(taskKey, string(marshaledTask), -1)
			if requeue {
				logger.Info().Int("retryLimit", t.Spec.RetryLimit).Int("failureCount", t.Status.FailureCount).Msg("Requeue to retry")
				pipe.LPush(b.pendingTaskQueueKey(queue.UID.String()), t.UID)
			} else {
				logger.Info().Int("retryLimit", t.Spec.RetryLimit).Int("failureCount", t.Status.FailureCount).Msg("Completed As Failure because retry exhausted")
			}

			for i, marshaled := range marshaledOnFailure {
				uid := marshaledOnFailureUIDs[i]
				pipe.SAdd(queueTasksKey, uid)
				pipe.Set(b.taskKey(queue.UID.String(), uid), marshaled, -1)
				pipe.LPush(b.pendingTaskQueueKey(queue.UID.String()), uid)
			}

			if len(onFailureDL) > 0 {
				pipe.LPush(b.deadletterQueueKey(queue.UID.String()), onFailureDL...)
			}

			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(ctx, logger, txf, watchingKeys...)
	if err != nil {
		return err
	}
	if deadletterError != nil {
		return deadletterError
	}
	return nil
}

func (b *Backend) notUnMarshalableDLError(unMarshalError error, raw string) taskqueue.TaskToDeadletterError {
	return taskqueue.NewTaskToDeadletterError(
		raw,
		errors.Wrap(unMarshalError, "can't unmarshal"),
	)
}
func (b *Backend) notMarshalableDLError(marshalError error, t *task.Task) taskqueue.TaskToDeadletterError {
	return taskqueue.NewTaskToDeadletterError(
		fmt.Sprintf("%#v", t),
		errors.Wrap(marshalError, "can't marshal"),
	)
}
func (b *Backend) invalidMessageDLError(err error, t *task.Task) taskqueue.TaskToDeadletterError {
	return taskqueue.NewTaskToDeadletterError(
		fmt.Sprintf("%#v", t),
		err,
	)
}

func (b *Backend) deliverToDeadLetter(tx *redis.Tx, queueUID string, e taskqueue.TaskToDeadletterError, preCmds func(pipe redis.Pipeliner)) error {
	b.Logger.Error().RawJSON("error", []byte(e.DLItem())).Msg("Sending a task to deadletter")
	_, perr := tx.TxPipelined(func(pipe redis.Pipeliner) error {
		preCmds(pipe)
		pipe.LPush(b.deadletterQueueKey(queueUID), e.DLItem())
		return nil
	})
	if perr == redis.Nil {
		return nil
	}
	if perr != nil {
		b.Logger.Error().Err(perr).Msg("failed to send a task to deadletter")
		return perr
	}
	return e
}

func (b *Backend) validateTaskSpec(s task.TaskSpec) error {
	var vErrors *multierror.Error
	maxBytes := PayloadMaxSizeInKB * KB
	if len(s.Name) > MaxNameLength {
		vErrors = multierror.Append(vErrors,
			errors.Errorf("TaskSpec.Name max length is %d (actual=%d Bytes)", MaxNameLength, len(s.Name)),
		)
	}
	if len(s.Payload) > maxBytes {
		vErrors = multierror.Append(vErrors,
			errors.Errorf("TaskSpec.Payload max size is %d Bytes (actual=%d Bytes)", maxBytes, len(s.Payload)),
		)
	}
	if s.RetryLimit > RetryLimitMax {
		vErrors = multierror.Append(vErrors,
			errors.Errorf("TaskSpec.retryLimit max is %d (actual=%d)", RetryLimitMax, s.RetryLimit),
		)
	}
	if vErrors != nil {
		return (*util.ValidationError)(vErrors)
	}
	return nil
}

func (b *Backend) allTasksKeysForDeleteQueue(rds redis.Cmdable, queueUID string) ([]string, error) {
	keysToDelete := []string{
		b.tasksKey(queueUID),
		b.deadletterQueueKey(queueUID),
		b.pendingTaskQueueKey(queueUID),
	}
	taskUIDs, err := rds.SMembers(b.tasksKey(queueUID)).Result()
	if err == redis.Nil {
		return []string{}, nil
	}
	if err != nil {
		return []string{}, err
	}
	for _, taskUID := range taskUIDs {
		keysToDelete = append(keysToDelete, b.taskKey(queueUID, taskUID))
	}
	return keysToDelete, nil
}
