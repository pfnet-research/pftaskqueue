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

package worker_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	apiworker "github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	"github.com/MakeNowJust/heredoc/v2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	backend "github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/testutil"

	backendconfig "github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	backendfactory "github.com/pfnet-research/pftaskqueue/pkg/backend/factory"
	queueworker "github.com/pfnet-research/pftaskqueue/pkg/worker"
	queueworkerconfig "github.com/pfnet-research/pftaskqueue/pkg/worker/config"

	"time"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
)

const (
	WorkerName = "test-worker"
	QueueName  = "test-queue"
)

var (
	SampleTaskQueueSpec = taskqueue.TaskQueueSpec{
		Name:  QueueName,
		State: taskqueue.TaskQueueStateActive,
	}
)

var _ = Describe("Worker", func() {
	var bcknd backend.Backend
	var worker *queueworker.Worker
	var workerWorkDir string

	mkSampleSpec := func(i int) task.TaskSpec {
		return task.TaskSpec{
			Payload:        fmt.Sprintf("%d", i),
			RetryLimit:     0,
			TimeoutSeconds: 60,
		}
	}

	BeforeEach(func() {
		var err error
		backendConfig := backendconfig.DefaultBackoffConfig()
		backendConfig.MaxRetry = 5
		bcknd, err = backendfactory.NewBackend(logger, backendconfig.Config{
			BackendType: "redis",
			Redis: &backendconfig.RedisConfig{
				Client:  client,
				Backoff: backendConfig,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		workerWorkDir, err = ioutil.TempDir("", "pftq_worker_test_")
		Expect(err).NotTo(HaveOccurred())
		worker, err = queueworker.NewWorker(context.Background(), logger, bcknd, queueworkerconfig.WorkerConfig{
			QueueName:                 QueueName,
			NumWorkerSalvageOnStartup: -1,
			WorkerSpec: apiworker.WorkerSpec{
				Name:        WorkerName,
				Concurrency: 4,
				TaskHandler: apiworker.TaskHandlerSpec{
					DefaultCommandTimeout: 1 * time.Minute,
					Commands: []string{
						"sh",
						"-c",
						heredoc.Doc(`
							DIR=$(cat)
							ls -lR ${DIR}/input
							PAYLOAD=$(cat ${DIR}/input/payload)
							if [ "${PAYLOAD}" == "hook" ]; then
								echo "hook success" > ${DIR}/output/payload
								echo "hook message" > ${DIR}/output/message
							else
								echo "${PAYLOAD} success" > ${DIR}/output/payload
								echo "${PAYLOAD} message" > ${DIR}/output/message
								echo '[{"payload": "hook", "timeoutSeconds": 5}]' > ${DIR}/output/postHooks.json
							fi
							ls -alR ${DIR}/output 1>&2
						`),
					},
				},
				HeartBeat: apiworker.HeartBeatSpec{
					SalvageDuration:    10 * time.Minute,
					ExpirationDuration: 5 * time.Minute,
					Interval:           30 * time.Second,
				},
				ExitOnSuspend: true,
				ExitOnEmpty:   false,
				NumTasks:      -1,
				WorkDir:       workerWorkDir,
			},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := bcknd.DeleteQueue(context.Background(), QueueName)
		Expect(err).NotTo(HaveOccurred())
		err = os.RemoveAll(workerWorkDir)
		Expect(err).NotTo(HaveOccurred())
	})

	When("Queue is suspended", func() {
		It("should exit immediately", func() {
			testutil.MustCreateQueue(bcknd, SampleTaskQueueSpec)
			suspend := SampleTaskQueueSpec
			suspend.State = taskqueue.TaskQueueStateSuspend
			testutil.MustQueueWithState(bcknd, suspend)

			err := worker.Start()
			Expect(err).To(BeNil())
		})
	})

	When("Queue is active", func() {
		It("should process and success", func() {
			testutil.MustCreateQueue(bcknd, SampleTaskQueueSpec)

			numTasks := 50
			for i := 0; i < numTasks; i++ {
				spec := mkSampleSpec(i)
				_, err := bcknd.AddTask(context.Background(), QueueName, spec)
				Expect(err).NotTo(HaveOccurred())
			}

			workerDone := make(chan error)
			go func() {
				workerDone <- worker.Start()
			}()

			Eventually(func() int {
				ts, err := bcknd.GetCompletedTasks(context.Background(), QueueName)
				if err != nil {
					logger.Error().Err(err).Msg("Error in get completed tasks")
					return -1
				}
				numSucceeded := 0
				for _, t := range ts {
					if t.Status.Phase == task.TaskPhaseSucceeded {
						numSucceeded++
					}
				}
				return numSucceeded
			}, 100*time.Second, 1*time.Second).Should(Equal(2 * numTasks))

			suspend := SampleTaskQueueSpec
			suspend.State = taskqueue.TaskQueueStateSuspend
			testutil.MustQueueWithState(bcknd, suspend)
			Eventually(workerDone, 30*time.Second).Should(Receive(BeNil()))
		})
	})
})
