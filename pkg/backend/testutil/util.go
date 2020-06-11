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

package testutil

import (
	"context"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	. "github.com/onsi/gomega"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
)

func MustCreateQueue(backend iface.Backend, spec taskqueue.TaskQueueSpec) *taskqueue.TaskQueue {
	queue, err := backend.CreateQueue(context.Background(), spec)
	Expect(err).NotTo(HaveOccurred())
	return queue
}

func MustQueueWithState(backend iface.Backend, spec taskqueue.TaskQueueSpec) {
	_, err := backend.UpdateQueue(context.Background(), spec)
	Expect(err).NotTo(HaveOccurred())
	updated, err := backend.GetQueueByName(context.Background(), spec.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(updated.Spec).Should(Equal(spec))
}

func MustCreateQueueAndRegisterWorker(backend iface.Backend, queueSpec taskqueue.TaskQueueSpec, workerSpec worker.WorkerSpec) (*taskqueue.TaskQueue, *worker.Worker) {
	queue, err := backend.CreateQueue(context.Background(), queueSpec)
	Expect(err).NotTo(HaveOccurred())
	worker, err := backend.RegisterWorker(context.Background(), queue.UID, workerSpec)
	Expect(err).NotTo(HaveOccurred())
	return queue, worker
}
