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

func (b *Backend) keyPrefix() string {
	return GlobalKeyPrefix + b.KeyPrefix
}
func (b *Backend) allQueuesKey() string {
	return b.keyPrefix() + ":queues"
}
func (b *Backend) queueKey(uid string) string {
	return b.keyPrefix() + ":queue:" + uid
}

func (b *Backend) tasksKey(queueUID string) string {
	return b.queueKey(queueUID) + ":tasks"
}
func (b *Backend) taskKeyPrefix(queueUID string) string {
	return b.keyPrefix() + ":queue:" + queueUID + ":task"
}
func (b *Backend) pendingTaskQueueKey(uid string) string {
	return b.taskKeyPrefix(uid) + ":pending"
}
func (b *Backend) deadletterQueueKey(queueUID string) string {
	return b.taskKeyPrefix(queueUID) + ":deadletter"
}
func (b *Backend) taskKey(queueUID, taskUID string) string {
	return b.taskKeyPrefix(queueUID) + ":" + taskUID
}

func (b *Backend) workersKey(queueUID string) string {
	return b.queueKey(queueUID) + ":workers"
}
func (b *Backend) workerKeyPrefix(queueUID, workerUID string) string {
	return b.queueKey(queueUID) + ":worker"
}
func (b *Backend) workerKey(queueUID, workerUID string) string {
	return b.workerKeyPrefix(queueUID, workerUID) + ":" + workerUID
}
func (b *Backend) workerPendingTaskQueueKey(queueUID, workerUID string) string {
	return b.workerKey(queueUID, workerUID) + ":pending"
}
func (b *Backend) workerTasksKey(queueUID, workerUID string) string {
	return b.workerKey(queueUID, workerUID) + ":tasks"
}
