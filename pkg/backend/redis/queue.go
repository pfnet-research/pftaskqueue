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
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-redis/redis/v7"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	"github.com/pkg/errors"
)

func (b *Backend) GetAllQueues(ctx context.Context) ([]*taskqueue.TaskQueue, error) {
	queuesHash, err := b.Client.HGetAll(b.allQueuesKey()).Result()
	if err == redis.Nil {
		return []*taskqueue.TaskQueue{}, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get all queues")
	}

	if len(queuesHash) == 0 {
		return []*taskqueue.TaskQueue{}, nil
	}

	uids := []string{}
	names := []string{}
	queueKeys := []string{}
	for k, v := range queuesHash {
		uids = append(uids, v)
		names = append(names, k)
		queueKeys = append(queueKeys, b.queueKey(v))
	}

	rawQueues, err := b.Client.MGet(queueKeys...).Result()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get all queues")
	}

	queues := []*taskqueue.TaskQueue{}
	for i, rawQueue := range rawQueues {
		lggr := b.Logger.With().
			Str("queueName", names[i]).
			Str("queueUID", uids[i]).Logger()
		if rawQueue == nil {
			lggr.Error().
				Interface("rawData", nil).
				Msg("Internal error. rawData is null. Skipped.")
			continue
		}

		var queue taskqueue.TaskQueue
		rawQueueStr, ok := rawQueue.(string)
		if !ok {
			lggr.Error().
				Interface("rawData", rawQueue).
				Str("type", reflect.TypeOf(rawQueue).String()).
				Msg("Internal error. rawData should be string. Skipped.")
			continue
		}
		if err := json.Unmarshal([]byte(rawQueueStr), &queue); err != nil {
			lggr.Error().Str("data", rawQueueStr).Msg("Failed to unmarshal to TaskQueue. Skipped.")
			continue
		}
		queues = append(queues, &queue)
	}

	return queues, nil
}

func (b *Backend) ensureQueueDoesNotExist(rds redis.Cmdable, queueName string) error {
	// HEXISTS {all_queue_key} queueName
	exists, err := rds.HExists(b.allQueuesKey(), queueName).Result()
	if err != nil {
		return err
	}
	if exists {
		return iface.TaskQueueExisted
	}
	return nil
}

func (b *Backend) CreateQueue(ctx context.Context, queueSpec taskqueue.TaskQueueSpec) (*taskqueue.TaskQueue, error) {
	if err := queueSpec.Validate(); err != nil {
		return nil, err
	}

	newQueue := taskqueue.NewTaskQueue(queueSpec)
	watchKeys := []string{
		b.allQueuesKey(),
		b.queueKey(newQueue.UID.String()),
	}

	// WATCH {all_queues_key} {queue_key}
	// HEXISTS {all_queue_key} queueName
	// ..error if exists
	// MULTI
	// HSET {all_queue_keys} {queueName} {queueUID}
	// SET {queue_key} {queue} -1
	// EXEC
	txf := func(tx *redis.Tx) error {
		err := b.ensureQueueDoesNotExist(tx, newQueue.Spec.Name)
		switch {
		case err == iface.TaskQueueExisted:
			return backoff.Permanent(err)
		case err != nil:
			return err
		}
		marshaledQueue, err := json.Marshal(newQueue)
		if err != nil {
			return backoff.Permanent(errors.Wrap(err, "can't marshal"))
		}

		// no exists
		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.HSet(b.allQueuesKey(), newQueue.Spec.Name, newQueue.UID.String())
			pipe.Set(b.queueKey(newQueue.UID.String()), marshaledQueue, -1)
			return nil
		})
		return err
	}
	err := b.runTxWithBackOff(
		ctx,
		b.Logger.With().
			Str("queueName", newQueue.Spec.Name).
			Str("queueUID", newQueue.UID.String()).
			Str("operation", "CreateQueue").
			Logger(),
		txf,
		watchKeys...,
	)
	if err != nil {
		return nil, err
	}
	return newQueue, nil
}

func (b *Backend) GetQueueByName(ctx context.Context, queueName string) (*taskqueue.TaskQueue, error) {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	return b.ensureQueueExistsByName(b.Client, queueName)
}

func (b *Backend) UpdateQueue(ctx context.Context, queueSpec taskqueue.TaskQueueSpec) (*taskqueue.TaskQueue, error) {
	if err := queueSpec.Validate(); err != nil {
		return nil, err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueSpec.Name)
	if err != nil {
		return nil, err
	}
	// WATCH {queue_key}
	// MULTI
	// SET {queue_key} {queue} -1
	// EXEC
	txf := func(tx *redis.Tx) error {
		queue.Spec = queueSpec
		queue.Status.UpdatedAt = time.Now()
		marshaledQueue, err := json.Marshal(queue)
		if err != nil {
			return backoff.Permanent(errors.Wrap(err, "can't marshal"))
		}

		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.Set(b.queueKey(queue.UID.String()), marshaledQueue, -1)
			return nil
		})
		return err
	}
	err = b.runTxWithBackOff(
		ctx,
		b.Logger.With().
			Str("queueName", queueSpec.Name).
			Str("queueUID", queue.UID.String()).
			Str("operation", "UpdateQueue").
			Logger(),
		txf,
		b.queueKey(queue.UID.String()),
	)
	if err != nil {
		return nil, err
	}
	return queue, nil
}

func (b *Backend) DeleteQueue(ctx context.Context, queueName string) error {
	if err := taskqueue.ValidateQueueName(queueName); err != nil {
		return err
	}
	queue, err := b.ensureQueueExistsByName(b.Client, queueName)
	if err != nil {
		return err
	}
	// WATCH {all_queues_key} {queue_key}
	// .. worker_keys = collect worker keys
	// WATCH worker_keys
	// .. task_keys = collect task keys
	// WATCh task_keys
	// MULTI
	// DEL {queue_key} worker_keys task_keys
	// HDEL {all_queues_key} {queueName}
	// EXEC
	txf := func(tx *redis.Tx) error {
		keysToDelete := []string{
			b.queueKey(queue.UID.String()),
		}

		workerKeysToDelete, err := b.allWorkersKeysForDeleteQueue(tx, queue.UID.String())
		if err != nil {
			return err
		}
		tx.Watch(workerKeysToDelete...)
		keysToDelete = append(keysToDelete, workerKeysToDelete...)

		taskKeysToDelete, err := b.allTasksKeysForDeleteQueue(tx, queue.UID.String())
		if err != nil {
			return err
		}
		tx.Watch(taskKeysToDelete...)
		keysToDelete = append(keysToDelete, taskKeysToDelete...)

		_, err = tx.TxPipelined(func(pipe redis.Pipeliner) error {
			pipe.Del(keysToDelete...)
			pipe.HDel(b.allQueuesKey(), queue.Spec.Name)
			return nil
		})
		return err
	}
	return b.runTxWithBackOff(
		ctx,
		b.Logger.With().
			Str("queueName", queueName).
			Str("queueUID", queue.UID.String()).
			Str("operation", "DeleteQueue").
			Logger(),
		txf,
		b.allQueuesKey(), b.queueKey(queue.UID.String()),
	)
}

func (b *Backend) ensureQueueExistsByName(rds redis.Cmdable, queueName string) (*taskqueue.TaskQueue, error) {
	uid, err := b.lookupQueueUID(rds, queueName)
	if err != nil {
		return nil, err
	}
	return b.ensureQueueExistsByUID(rds, uid)
}

func (b *Backend) ensureQueueExistsByUID(rds redis.Cmdable, uid string) (*taskqueue.TaskQueue, error) {
	rawQueue, err := rds.Get(b.queueKey(uid)).Result()
	switch {
	case err == redis.Nil:
		b.Logger.Error().Stack().Err(err).Str("redis cmd", fmt.Sprintf("GET %s", b.queueKey(uid))).Msg("debug-next-task")
		return nil, iface.TaskQueueNotFound
	case err != nil:
		b.Logger.Error().Stack().Err(err).Str("redis cmd", fmt.Sprintf("GET %s", b.queueKey(uid))).Msg("debug-next-task")
		return nil, err
	}

	var queue taskqueue.TaskQueue
	if err := json.Unmarshal([]byte(rawQueue), &queue); err != nil {
		return nil, errors.Wrap(err, "Can't unmarshal to TaskQueue")
	}

	return &queue, nil
}

func (b *Backend) lookupQueueUID(rds redis.Cmdable, queueName string) (string, error) {
	uid, err := rds.HGet(b.allQueuesKey(), queueName).Result()
	switch {
	case err == redis.Nil:
		return "", iface.TaskQueueNotFound
	case err != nil:
		return "", err
	}
	return uid, nil
}
