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
	"reflect"
	"time"

	apiworker "github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	"github.com/cenkalti/backoff/v4"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"

	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
)

func (b *Backend) RegisterWorker(ctx context.Context, queueUID uuid.UUID, workerSpec apiworker.WorkerSpec) (*apiworker.Worker, error) {
	if err := workerSpec.Validate(); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByUID(b.Client, queueUID.String())
	if err != nil {
		return nil, err
	}

	newWorker := apiworker.NewWorker(workerSpec, queue.UID)
	marshaled, err := json.Marshal(newWorker)
	if err != nil {
		return nil, err
	}

	// WATCH {workers_key} {worker_key}
	// MULTI
	// SADD {workers_key} {workerUID}
	// SET {worker_key} {worker}
	// EXEC
	watchKey := []string{b.workersKey(queueUID.String()), b.workerKey(queueUID.String(), newWorker.UID.String())}
	txf := func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.SAdd(b.workersKey(queueUID.String()), newWorker.UID.String())
			pipe.Set(b.workerKey(queueUID.String(), newWorker.UID.String()), string(marshaled), -1)
			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(
		ctx,
		b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("workerUID", newWorker.UID.String()).
			Str("workerName", newWorker.Spec.Name).
			Str("operation", "RegisterWorker").
			Logger(),
		txf,
		watchKey...,
	)
	if err != nil {
		return nil, err
	}
	return newWorker, nil
}

func (b *Backend) GetAllWorkers(ctx context.Context, queueUID uuid.UUID) ([]*apiworker.Worker, error) {
	return b.getAllWorkers(b.Client, queueUID.String())
}

func (b *Backend) getAllWorkers(rds redis.Cmdable, queueUID string) ([]*apiworker.Worker, error) {
	queue, err := b.ensureQueueExistsByUID(rds, queueUID)
	if err != nil {
		return nil, err
	}
	workerUIDs, err := rds.SMembers(b.workersKey(queueUID)).Result()
	if err == redis.Nil {
		return []*apiworker.Worker{}, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get all workers")
	}
	if len(workerUIDs) == 0 {
		return []*apiworker.Worker{}, nil
	}

	keysToMGet := []string{}
	for _, uid := range workerUIDs {
		keysToMGet = append(keysToMGet, b.workerKey(queueUID, uid))
	}
	rawWorkers, err := rds.MGet(keysToMGet...).Result()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get all workers")
	}

	workers := []*apiworker.Worker{}
	now := time.Now()
	for i, rawWorker := range rawWorkers {
		lggr := b.Logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("workerUID", workerUIDs[i]).Logger()
		if rawWorker == nil {
			lggr.Error().
				Interface("rawData", nil).
				Msg("Internal error. rawData is null. Skipped.")
			continue
		}
		var worker apiworker.Worker
		rawWorkerStr, ok := rawWorker.(string)
		if !ok {
			lggr.Error().
				Interface("rawData", rawWorker).
				Str("type", reflect.TypeOf(rawWorker).String()).
				Msg("Internal error. rawData should be string. Skipped.")
			continue
		}
		if err := json.Unmarshal([]byte(rawWorkerStr), &worker); err != nil {
			lggr.Error().Str("data", rawWorkerStr).Msg("Failed to unmarshal to Worker. Skipped.")
			continue
		}
		worker.UpdateStatusIfLostOn(now)
		workers = append(workers, &worker)
	}

	return workers, nil
}

func (b *Backend) GetWorker(ctx context.Context, queueUID, workerUID uuid.UUID) (*apiworker.Worker, error) {
	queue, err := b.ensureQueueExistsByUID(b.Client, queueUID.String())
	if err != nil {
		return nil, err
	}
	worker, err := b.ensureWorkerExistsByUID(b.Client, queue, workerUID.String())
	if err != nil {
		return nil, err
	}
	worker.UpdateStatusIfLostOn(time.Now())
	return worker, nil
}

func (b *Backend) GetRunningWorkers(ctx context.Context, queueUID uuid.UUID) ([]*apiworker.Worker, error) {
	allworkers, err := b.GetAllWorkers(ctx, queueUID)
	if err != nil {
		return nil, err
	}
	workers := []*apiworker.Worker{}
	now := time.Now()
	for _, w := range allworkers {
		w.UpdateStatusIfLostOn(now)
		if w.Status.Phase == apiworker.WorkerPhaseRunning {
			workers = append(workers, w)
		}
	}
	return workers, nil
}

func (b *Backend) GetSucceededWorkers(ctx context.Context, queueUID uuid.UUID) ([]*apiworker.Worker, error) {
	allworkers, err := b.GetAllWorkers(ctx, queueUID)
	if err != nil {
		return nil, err
	}
	workers := []*apiworker.Worker{}
	now := time.Now()
	for _, w := range allworkers {
		w.UpdateStatusIfLostOn(now)
		if w.Status.Phase == apiworker.WorkerPhaseSucceeded {
			workers = append(workers, w)
		}
	}
	return workers, nil
}

func (b *Backend) GetFailedWorkers(ctx context.Context, queueUID uuid.UUID) ([]*apiworker.Worker, error) {
	allworkers, err := b.GetAllWorkers(ctx, queueUID)
	if err != nil {
		return nil, err
	}
	workers := []*apiworker.Worker{}
	now := time.Now()
	for _, w := range allworkers {
		w.UpdateStatusIfLostOn(now)
		if w.Status.Phase == apiworker.WorkerPhaseFailed {
			workers = append(workers, w)
		}
	}
	return workers, nil
}

func (b *Backend) GetLostWorker(ctx context.Context, queueUID uuid.UUID) ([]*apiworker.Worker, error) {
	allworkers, err := b.GetAllWorkers(ctx, queueUID)
	if err != nil {
		return nil, err
	}
	lostWorkers := []*apiworker.Worker{}
	now := time.Now()
	for _, w := range allworkers {
		w.UpdateStatusIfLostOn(now)
		if w.IsLostOn(now) {
			lostWorkers = append(lostWorkers, w)
		}
	}
	return lostWorkers, nil
}

func (b *Backend) GetWorkersToSalvage(ctx context.Context, queueUID uuid.UUID) ([]*apiworker.Worker, error) {
	return b.getWorkersToSalvage(b.Client, queueUID.String())
}

func (b *Backend) getWorkersToSalvage(rds redis.Cmdable, queueUID string) ([]*apiworker.Worker, error) {
	allworkers, err := b.getAllWorkers(rds, queueUID)
	if err != nil {
		return nil, err
	}
	workersToSalvage := []*apiworker.Worker{}
	now := time.Now()
	for _, w := range allworkers {
		if w.AllowToSalvageOn(now) {
			workersToSalvage = append(workersToSalvage, w)
		}
	}
	return workersToSalvage, nil
}

func (b *Backend) SendWorkerHeartBeat(ctx context.Context, queueUID, workerUID uuid.UUID) (*apiworker.Worker, error) {
	return b.updateWorker(ctx, queueUID, workerUID,
		func(worker *apiworker.Worker) error {
			return worker.SetLastHeartBeatAt(time.Now())
		},
		false,
		b.Logger.With().Str("operation", "SendWorkerHeartBeat").Logger(),
	)
}

func (b *Backend) SetWorkerSucceeded(ctx context.Context, queueUID, workerUID uuid.UUID) (*apiworker.Worker, error) {
	return b.updateWorker(ctx, queueUID, workerUID,
		func(worker *apiworker.Worker) error {
			return worker.SetSucceeded(time.Now())
		},
		true,
		b.Logger.With().Str("operation", "SetWorkerSucceeded").Logger(),
	)
}

func (b *Backend) SetWorkerFailed(ctx context.Context, queueUID, workerUID uuid.UUID) (*apiworker.Worker, error) {
	return b.updateWorker(ctx, queueUID, workerUID,
		func(worker *apiworker.Worker) error {
			return worker.SetFailed(time.Now(), apiworker.WorkerReasonFailure)
		},
		true,
		b.Logger.With().Str("operation", "SetWorkerFailed").Logger(),
	)
}

func (b *Backend) SalvageWorker(ctx context.Context, queueUID, salvagingWorkerUID, salvageTargetWorkerUID uuid.UUID) (*apiworker.Worker, []*task.Task, error) {
	queue, salvagingWorker, err := b.ensureQueueAndWorkerExists(queueUID, salvagingWorkerUID)
	if err != nil {
		return nil, nil, err
	}
	salvageTargetWorker, err := b.ensureWorkerExistsByUID(b.Client, queue, salvageTargetWorkerUID.String())
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	if !salvageTargetWorker.AllowToSalvageOn(now) {
		return nil, nil, iface.WorkerSalvationNotAllowed
	}

	logger := b.Logger.With().
		Str("queueName", queue.Spec.Name).
		Str("queueUID", queue.UID.String()).
		Str("salvagingWorkerName", salvagingWorker.Spec.Name).
		Str("salvagingWorkerUID", salvagingWorker.UID.String()).
		Str("salvageTargetWorkerName", salvageTargetWorker.Spec.Name).
		Str("salvageTargetWorkerUID", salvageTargetWorker.UID.String()).
		Str("operation", "SalvageWorker").Logger()

	salvageTargetWorker.SetSalvagedByAndOn(salvagingWorker.UID, now)
	marshaledSalvageTargetWorker, err := json.Marshal(salvageTargetWorker)
	if err != nil {
		logger.Error().Err(err).Msg("Failed targetWorker to marshal to Worker")
		return nil, nil, err
	}

	var salvagedTasks []*task.Task
	watchKeys := []string{
		b.workerPendingTaskQueueKey(queue.UID.String(), salvageTargetWorker.UID.String()),
		b.workerTasksKey(queue.UID.String(), salvageTargetWorker.UID.String()),
	}
	txf := func(tx *redis.Tx) error {
		salvagedTasks = []*task.Task{}

		// salvaging tasks
		taskUIDsToSalvage := []string{}
		pendingTaskUIDs, err := tx.LRange(
			b.workerPendingTaskQueueKey(queue.UID.String(), salvageTargetWorker.UID.String()), 0, -1).
			Result()
		if err != nil && err != redis.Nil {
			logger.Error().Err(err).Msg("Failed to get to worker pending tasks.")
			return err
		} else if err == nil && len(pendingTaskUIDs) > 0 {
			taskUIDsToSalvage = append(taskUIDsToSalvage, pendingTaskUIDs...)
		}
		workerTaskUIDs, err := tx.SMembers(b.workerTasksKey(queue.UID.String(), salvageTargetWorker.UID.String())).Result()
		if err != nil && err != redis.Nil {
			logger.Error().Err(err).
				Msg("Failed to get to worker pending tasks")
			return err
		} else if err == nil && len(workerTaskUIDs) > 0 {
			taskUIDsToSalvage = append(taskUIDsToSalvage, workerTaskUIDs...)
		}

		tasksToSalvage, err := b.getTasksByUIDs(queue.UID.String(), taskUIDsToSalvage, func(_t *task.Task) bool {
			return _t.SetSalvagedByOn(salvagingWorker.UID, now)
		}, logger)
		if err != nil {
			return err
		}

		marshaledTasksToSalvage := make([]string, len(tasksToSalvage))
		marshalOK := make([]bool, len(tasksToSalvage))
		dlItems := []interface{}{}
		for i, taskToSalvage := range tasksToSalvage {
			tx.Watch(b.taskKey(queue.UID.String(), taskToSalvage.UID))
			bytes, err := json.Marshal(&taskToSalvage)
			if err != nil {
				logger.Error().Err(err).
					Str("taskUIDToSalvage", taskToSalvage.UID).
					Msg("Failed to marshal. Sending To DeadLetter")
				marshalOK[i] = false
				marshaledTasksToSalvage[i] = ""
				dlItems = append(dlItems, b.notMarshalableDLError(err, taskToSalvage).DLItem())
				continue
			}
			marshalOK[i] = true
			marshaledTasksToSalvage[i] = string(bytes)
			salvagedTasks = append(salvagedTasks, taskToSalvage)
		}

		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.Del(b.workerPendingTaskQueueKey(queue.UID.String(), salvageTargetWorker.UID.String()))
			pipe.Set(b.workerKey(queue.UID.String(), salvageTargetWorker.UID.String()), marshaledSalvageTargetWorker, -1)
			for i, t := range tasksToSalvage {
				if len(t.Status.History) > 0 && t.Status.History[len(t.Status.History)-1].SalvagedBy != nil &&
					*t.Status.History[len(t.Status.History)-1].SalvagedBy == salvagingWorker.UID {
					workerUID := t.Status.History[len(t.Status.History)-1].WorkerUID
					pipe.SRem(b.workerTasksKey(queue.UID.String(), workerUID), t.UID)
				}
				if marshalOK[i] {
					pipe.LPush(b.pendingTaskQueueKey(queue.UID.String()), t.UID)
					pipe.Set(b.taskKey(queue.UID.String(), t.UID), marshaledTasksToSalvage[i], -1)
				} else {
					pipe.Del(b.tasksKey(queue.UID.String()))
					pipe.Del(b.taskKey(queue.UID.String(), t.UID))
				}
			}
			if len(dlItems) > 0 {
				pipe.LPush(b.deadletterQueueKey(queue.UID.String()), dlItems...)
			}
			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(ctx, logger, txf, watchKeys...)
	if err != nil {
		return nil, nil, err
	}
	return salvageTargetWorker, salvagedTasks, nil
}

func (b *Backend) updateWorker(
	ctx context.Context,
	queueUID, workerUID uuid.UUID,
	updateFn func(spec *apiworker.Worker) error,
	successOrFailed bool,
	logger zerolog.Logger,
) (*apiworker.Worker, error) {
	queue, err := b.ensureQueueExistsByUID(b.Client, queueUID.String())
	if err != nil {
		return nil, err
	}

	var worker *apiworker.Worker
	watchKeys := []string{
		b.workerKey(queue.UID.String(), workerUID.String()),
	}

	// WATCH {worker_key}
	// MULTI
	// SET {worker_key} {updated_worker} -1
	// EXEC
	txf := func(tx *redis.Tx) error {
		var err error
		worker, err = b.ensureWorkerExistsByUID(tx, queue, workerUID.String())
		if err != nil {
			return backoff.Permanent(err)
		}

		// if this update was succeeded/failed, check tasks remained under the worker.
		// if remained, it can't transit to succeeded/failed because this will need to be salvaged later.
		if successOrFailed {
			remainedTasks := []string{}
			pendings, err := tx.LRange(b.workerPendingTaskQueueKey(queue.UID.String(), workerUID.String()), 0, -1).Result()
			if err != nil {
				return backoff.Permanent(err)
			}
			remainedTasks = append(remainedTasks, pendings...)
			receivedOrRunnings, err := tx.SMembers(b.workerTasksKey(queue.UID.String(), workerUID.String())).Result()
			if err != nil {
				return backoff.Permanent(err)
			}
			remainedTasks = append(remainedTasks, receivedOrRunnings...)
			if len(remainedTasks) > 0 {
				return backoff.Permanent(errors.Errorf("Can't update Succeeded/Failed because some tasks are incomplete: %v", remainedTasks))
			}
		}

		if err := updateFn(worker); err != nil {
			return backoff.Permanent(err)
		}

		marshaled, err := json.Marshal(worker)
		if err != nil {
			return backoff.Permanent(errors.Wrap(err, "can't marshal"))
		}

		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.Set(b.workerKey(queue.UID.String(), worker.UID.String()), string(marshaled), -1)
			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(
		ctx,
		logger.With().
			Str("queueName", queue.Spec.Name).
			Str("queueUID", queue.UID.String()).
			Str("workerUID", workerUID.String()).
			Logger(),
		txf,
		watchKeys...,
	)
	if err != nil {
		return nil, err
	}
	return worker, nil
}

func (b *Backend) ensureWorkerExistsByUID(rds redis.Cmdable, queue *taskqueue.TaskQueue, uid string) (*apiworker.Worker, error) {
	rawWorker, err := rds.Get(b.workerKey(queue.UID.String(), uid)).Result()
	switch {
	case err == redis.Nil:
		return nil, iface.WorkerNotFound
	case err != nil:
		return nil, err
	}

	var worker apiworker.Worker
	if err := json.Unmarshal([]byte(rawWorker), &worker); err != nil {
		return nil, errors.Wrap(err, "Can't unmarshal to Worker")
	}

	return &worker, nil
}

func (b *Backend) allWorkersKeysForDeleteQueue(rds redis.Cmdable, queueUID string) ([]string, error) {
	keysToDelete := []string{}
	workerUIDs, err := rds.SMembers(b.workersKey(queueUID)).Result()
	if err == redis.Nil {
		return []string{}, nil
	}
	if err != nil {
		return []string{}, err
	}
	for _, workerUID := range workerUIDs {
		keysToDelete = append(keysToDelete,
			b.workerKey(queueUID, workerUID),
			b.workerPendingTaskQueueKey(queueUID, workerUID),
			b.workerTasksKey(queueUID, workerUID))
	}

	// If you are not using transactions, you need to delete the wokers key at the end.
	keysToDelete = append(keysToDelete, b.workersKey(queueUID))
	return keysToDelete, nil
}
