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
	"io/ioutil"
	"strings"
	"time"

	apiworker "github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	"github.com/creasty/defaults"
	"github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pfnet-research/pftaskqueue/pkg/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"
	backendconfig "github.com/pfnet-research/pftaskqueue/pkg/backend/config"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	"github.com/pfnet-research/pftaskqueue/pkg/backend/testutil"
)

const (
	WorkerName = "test-worker"
	QueueName  = "test-queue"
)

var (
	SampleQueueSpec = taskqueue.TaskQueueSpec{
		Name:  QueueName,
		State: taskqueue.TaskQueueStateActive,
	}
	SampleTaskHandlerSpec = func() apiworker.TaskHandlerSpec {
		var s = apiworker.TaskHandlerSpec{}
		err := defaults.Set(&s)
		if err != nil {
			panic(err)
		}
		return s
	}()
	SampleHeartBeatSpec = func() apiworker.HeartBeatSpec {
		var s = apiworker.HeartBeatSpec{}
		err := defaults.Set(&s)
		if err != nil {
			panic(err)
		}
		return s
	}()
	WorkDir = func() string {
		path, err := ioutil.TempDir("", "pftaskqueue_redis_test_")
		if err != nil {
			panic(err)
		}
		return path
	}()
	SampleWorkerSpec = apiworker.WorkerSpec{
		Name:          WorkerName,
		Concurrency:   1,
		TaskHandler:   SampleTaskHandlerSpec,
		HeartBeat:     SampleHeartBeatSpec,
		ExitOnSuspend: false,
		ExitOnEmpty:   false,
		NumTasks:      -1,
		WorkDir:       WorkDir,
	}
	SampleTaskSpec = task.TaskSpec{
		Payload:        "payload",
		RetryLimit:     1,
		TimeoutSeconds: 60,
	}
	SampleInvalidTaskSpec = task.TaskSpec{
		Name:           strings.Repeat("a", MaxNameLength+1),
		Payload:        strings.Repeat("x", PayloadMaxSizeInKB*KB+1),
		RetryLimit:     100,
		TimeoutSeconds: 0,
	}
)

var _ = Describe("Backend", func() {
	//var workerUID uuid.UUID
	var backend *Backend
	BeforeEach(func() {
		//workerUID = uuid.New()
		var err error
		backoffConfig := backendconfig.DefaultBackoffConfig()
		backoffConfig.MaxRetry = 0
		ibackend, err := NewBackend(logger, backendconfig.Config{
			BackendType: "redis",
			Redis: &backendconfig.RedisConfig{
				KeyPrefix:         "test",
				Client:            client,
				Backoff:           backoffConfig,
				ChunkSizeInGet:    1000,
				ChunkSizeInDelete: 1000,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		backend, _ = ibackend.(*Backend)
	})

	AfterEach(func() {
		keys, err := client.Keys("*").Result()
		Expect(err).NotTo(HaveOccurred())
		if len(keys) > 0 {
			num, err := client.Del(keys...).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(num).To(Equal(int64(len(keys))))
		}
	})

	assertKeyContents := func(key string, expect interface{}) {
		expectedBytes, err := json.Marshal(expect)
		Expect(err).NotTo(HaveOccurred())
		raw, err := client.Get(key).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(raw).To(Equal(string(expectedBytes)))
	}
	mustPendingQueueLength := func(uid string, length int) []string {
		raws, err := client.LRange(backend.pendingTaskQueueKey(uid), 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(raws)).To(Equal(length))
		return raws
	}
	mustTasksSetSize := func(uid string, length int) []string {
		raws, err := client.SMembers(backend.tasksKey(uid)).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(raws)).To(Equal(length))
		return raws
	}
	mustWorkerPendingQueueLength := func(queueUID, workerUID string, length int) []string {
		raws, err := client.LRange(backend.workerPendingTaskQueueKey(queueUID, workerUID), 0, -1).Result()
		if err != nil {
			Expect(err).To(Equal(redis.Nil))
		} else {
			Expect(len(raws)).To(Equal(length))
		}
		return raws
	}
	mustWorkerTasksSetSize := func(queueUID, workerUID string, length int) []string {
		raws, err := client.SMembers(backend.workerTasksKey(queueUID, workerUID)).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(raws)).To(Equal(length))
		return raws
	}
	mustDeadletterLength := func(queueUID string, length int) []string {
		raws, err := client.LRange(backend.deadletterQueueKey(queueUID), 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(raws)).To(Equal(length))
		return raws
	}

	Context("Queue Operation", func() {
		Context("CreateQueue", func() {
			When("the queue does not exist", func() {
				It("can create queue", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)

					queuesHash, err := client.HGetAll(backend.allQueuesKey()).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(queuesHash)).To(Equal(1))
					Expect(queuesHash).To(HaveKey(SampleQueueSpec.Name))

					rawSavedQueue, err := client.Get(backend.queueKey(queue.UID.String())).Result()
					Expect(err).NotTo(HaveOccurred())
					rawQueue, err := json.Marshal(queue)
					Expect(err).NotTo(HaveOccurred())
					Expect(rawSavedQueue).To(Equal(string(rawQueue)))
				})
			})
			When("the queue does exist", func() {
				It("should raise TaskQueueExisted", func() {
					testutil.MustCreateQueue(backend, SampleQueueSpec)
					_, err := backend.CreateQueue(context.Background(), SampleQueueSpec)
					Expect(err).To(Equal(iface.TaskQueueExisted))
				})
			})
		})
		Context("GetAllQueue", func() {
			When("some queues exist", func() {
				It("should return all registered queues", func() {
					testutil.MustCreateQueue(backend, SampleQueueSpec)
					queues, err := backend.GetAllQueues(context.Background())
					Expect(err).NotTo(HaveOccurred())
					Expect(queues).To(HaveLen(1))
					Expect(queues[0].Spec.Name).To(Equal(SampleQueueSpec.Name))
				})
			})
			When("no queue exist", func() {
				It("should return empty list", func() {
					queues, err := backend.GetAllQueues(context.Background())
					Expect(err).NotTo(HaveOccurred())
					Expect(queues).To(HaveLen(0))
				})
			})
		})
		Context("GetQueue", func() {
			When("the queue exists", func() {
				It("can get TaskQueue", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					savedQueue, err := backend.GetQueueByName(context.Background(), SampleQueueSpec.Name)
					rawQueue, err := json.Marshal(queue)
					Expect(err).NotTo(HaveOccurred())
					rawSavedQueue, err := json.Marshal(savedQueue)
					Expect(rawSavedQueue).To(Equal(rawQueue))
				})
			})
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFoundError", func() {
					_, err := backend.GetQueueByName(context.Background(), SampleQueueSpec.Name)
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
		})

		Context("UpdateQueue", func() {
			When("the queue exists", func() {
				Context("with the same UID", func() {
					It("can update TaskQueue", func() {
						queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
						queue.Spec.State = taskqueue.TaskQueueStateSuspend
						_, err := backend.UpdateQueue(context.Background(), queue.Spec)
						Expect(err).NotTo(HaveOccurred())
						rQueue, err := backend.GetQueueByName(context.Background(), queue.Spec.Name)
						Expect(err).NotTo(HaveOccurred())
						Expect(rQueue.Spec.Name).To(Equal(queue.Spec.Name))
						Expect(rQueue.UID).To(Equal(queue.UID))
						Expect(rQueue.Spec.State).To(Equal(taskqueue.TaskQueueStateSuspend))
						Expect(rQueue.Status.CreatedAt.Format(time.RFC3339)).To(Equal(queue.Status.CreatedAt.Format(time.RFC3339)))
						Expect(rQueue.Status.UpdatedAt).To(BeTemporally(">", queue.Status.UpdatedAt))
					})
				})
			})
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFoundError", func() {
					_, err := backend.UpdateQueue(context.Background(), SampleQueueSpec)
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
		})

		Context("DeleteQueue", func() {
			When("the queue exists", func() {
				It("can delete the queue", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					Expect(backend.DeleteQueue(context.Background(), SampleQueueSpec.Name)).NotTo(HaveOccurred())

					queuesHash, err := client.HGetAll(backend.allQueuesKey()).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(queuesHash)).To(Equal(0))
					keys, err := client.Keys(backend.queueKey(queue.UID.String()) + "*").Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(keys)).To(Equal(0))
				})
			})
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFoundError", func() {
					err := backend.DeleteQueue(context.Background(), SampleQueueSpec.Name)
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the large queue exists", func() {
				It("can delete the queue", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					// numOfTasks % chunkSize != 0 && numOfTasks > chunkSize
					numOfTasks := 12345
					for i := 0; i < numOfTasks; i++ {
						_, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
						Expect(err).NotTo(HaveOccurred())
					}

					Expect(backend.DeleteQueue(context.Background(), SampleQueueSpec.Name)).NotTo(HaveOccurred())

					queuesHash, err := client.HGetAll(backend.allQueuesKey()).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(queuesHash)).To(Equal(0))
					keys, err := client.Keys(backend.queueKey(queue.UID.String()) + "*").Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(len(keys)).To(Equal(0))
				})
			})
		})
	})

	Context("Worker Operation", func() {
		Context("RegisterWorker", func() {
			When("the queue doesn't exists", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.RegisterWorker(context.Background(), uuid.New(), SampleWorkerSpec)
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				It("should register worker with new uuid", func() {
					queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

					Expect(worker.QueueUID).To(Equal(queue.UID))
					Expect(worker.Spec).To(Equal(SampleWorkerSpec))
					Expect(worker.Status.Phase).To(Equal(apiworker.WorkerPhaseRunning))
					Expect(worker.Status.Reason).To(Equal(apiworker.WorkerReason("")))

					isMember, err := client.SIsMember(backend.workersKey(queue.UID.String()), worker.UID.String()).Result()
					Expect(err).NotTo(HaveOccurred())
					Expect(isMember).To(BeTrue())

					rawSavedWorker, err := client.Get(backend.workerKey(queue.UID.String(), worker.UID.String())).Result()
					Expect(err).NotTo(HaveOccurred())

					marshaled, err := json.Marshal(worker)
					Expect(err).NotTo(HaveOccurred())
					Expect(rawSavedWorker).To(Equal(string(marshaled)))
				})
			})
		})

		Context("GetAllWorkers", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.GetAllWorkers(context.Background(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				When("no worker registered", func() {
					It("should return empty list", func() {
						queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
						workers, err := backend.GetAllWorkers(context.Background(), queue.UID)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(workers)).To(Equal(0))
					})
				})
				When("some registers exist", func() {
					It("should return all the workers registered", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

						workers, err := backend.GetAllWorkers(context.Background(), queue.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(len(workers)).To(Equal(1))
						Expect(workers[0].Spec.Name).To(Equal(SampleWorkerSpec.Name))
						Expect(workers[0].QueueUID).To(Equal(queue.UID))
						Expect(workers[0].UID).To(Equal(worker.UID))
					})
				})
			})
		})

		Context("GetWorker", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.GetWorker(context.Background(), uuid.New(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				It("return WorkerNotFound error for not registered uid", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					_, err := backend.GetWorker(context.Background(), queue.UID, uuid.New())
					Expect(err).To(Equal(iface.WorkerNotFound))
				})
				It("return Worker for registered uid", func() {
					queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

					returnedWorker, err := backend.GetWorker(context.Background(), queue.UID, worker.UID)

					Expect(err).NotTo(HaveOccurred())
					Expect(returnedWorker.Spec).To(Equal(SampleWorkerSpec))
					Expect(returnedWorker.QueueUID).To(Equal(worker.QueueUID))
					Expect(returnedWorker.UID).To(Equal(worker.UID))
				})
			})
		})

		Context("GetLostWorker", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.GetLostWorker(context.Background(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				When("no worker registered", func() {
					It("should return empty list", func() {
						queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
						workers, err := backend.GetLostWorker(context.Background(), queue.UID)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(workers)).To(Equal(0))
					})
				})
				When("some registers exist", func() {
					It("should return only  'workerlost' workers", func() {
						shortHeartBeatSpec := SampleWorkerSpec
						shortHeartBeatSpec.HeartBeat = apiworker.HeartBeatSpec{
							SalvageDuration:    1 * time.Second,
							ExpirationDuration: 1 * time.Second,
							Interval:           1 * time.Second,
						}
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, shortHeartBeatSpec)

						time.Sleep(1 * time.Second)

						workers, err := backend.GetLostWorker(context.Background(), queue.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(len(workers)).To(Equal(1))
						Expect(workers[0].Spec).To(Equal(shortHeartBeatSpec))
						Expect(workers[0].QueueUID).To(Equal(queue.UID))
						Expect(workers[0].UID).To(Equal(worker.UID))
						Expect(workers[0].Status.Phase).To(Equal(apiworker.WorkerPhaseFailed))
						Expect(workers[0].Status.Reason).To(Equal(apiworker.WorkerReasonLost))
						Expect(workers[0].Status.StartedAt).To(BeTemporally("==", worker.Status.StartedAt))
						Expect(workers[0].Status.FinishedAt).To(BeTemporally("==", worker.Status.FinishedAt))
						Expect(workers[0].Status.LastHeartBeatAt).To(BeTemporally("==", worker.Status.LastHeartBeatAt))
					})
				})
			})
		})

		Context("GetWorkersToSalvage", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.GetWorkersToSalvage(context.Background(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				When("no worker registered", func() {
					It("should return empty list", func() {
						queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
						workers, err := backend.GetWorkersToSalvage(context.Background(), queue.UID)
						Expect(err).NotTo(HaveOccurred())
						Expect(len(workers)).To(Equal(0))
					})
				})
				When("some registers exist", func() {
					It("should return only  'workerlost' workers", func() {
						shortHeartBeatSpec := SampleWorkerSpec
						shortHeartBeatSpec.HeartBeat = apiworker.HeartBeatSpec{
							SalvageDuration:    2 * time.Second,
							ExpirationDuration: 1 * time.Second,
							Interval:           1 * time.Second,
						}
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, shortHeartBeatSpec)

						time.Sleep(3 * time.Second) // expiration + salvage duration

						workers, err := backend.GetWorkersToSalvage(context.Background(), queue.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(len(workers)).To(Equal(1))
						Expect(workers[0].Spec).To(Equal(shortHeartBeatSpec))
						Expect(workers[0].QueueUID).To(Equal(queue.UID))
						Expect(workers[0].UID).To(Equal(worker.UID))
						Expect(workers[0].Status.Phase).To(Equal(apiworker.WorkerPhaseFailed))
						Expect(workers[0].Status.Reason).To(Equal(apiworker.WorkerReasonLost))
						Expect(workers[0].Status.StartedAt).To(BeTemporally("==", worker.Status.StartedAt))
						Expect(workers[0].Status.FinishedAt).To(BeTemporally("==", worker.Status.FinishedAt))
						Expect(workers[0].Status.LastHeartBeatAt).To(BeTemporally("==", worker.Status.LastHeartBeatAt))
					})
				})
			})
		})

		Context("SendHeartBeat", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.SendWorkerHeartBeat(context.Background(), uuid.New(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				It("should raise WorkerNotFound error for non-registered workerUID ", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					_, err := backend.SendWorkerHeartBeat(context.Background(), queue.UID, uuid.New())
					Expect(err).To(Equal(iface.WorkerNotFound))
				})
				When("the worker is Running Phase", func() {
					It("should update only LastHeartBeatAt", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

						updatedWorker, err := backend.SendWorkerHeartBeat(context.Background(), queue.UID, worker.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(updatedWorker.Spec).To(Equal(SampleWorkerSpec))
						Expect(updatedWorker.UID).To(Equal(worker.UID))
						Expect(updatedWorker.QueueUID).To(Equal(queue.UID))
						Expect(updatedWorker.Status.Phase).To(Equal(worker.Status.Phase))
						Expect(updatedWorker.Status.Reason).To(Equal(worker.Status.Reason))
						Expect(updatedWorker.Status.StartedAt).To(BeTemporally("==", worker.Status.StartedAt))
						Expect(updatedWorker.Status.LastHeartBeatAt).To(BeTemporally(">", worker.Status.LastHeartBeatAt))
						Expect(updatedWorker.Status.FinishedAt).To(BeTemporally("==", worker.Status.FinishedAt))
					})
				})
				When("the worker is non-Running Phase", func() {
					It("should raise error", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
						_, err := backend.SetWorkerSucceeded(context.Background(), queue.UID, worker.UID)
						Expect(err).NotTo(HaveOccurred())
						_, err = backend.SendWorkerHeartBeat(context.Background(), queue.UID, worker.UID)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(ContainSubstring("Worker must be Running phase to set HeartBeat"))
					})
				})
			})
		})

		Context("SetWorkerSucceeded", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.SetWorkerSucceeded(context.Background(), uuid.New(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				It("should raise WorkerNotFound error for non-registered workerUID ", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					_, err := backend.SetWorkerSucceeded(context.Background(), queue.UID, uuid.New())
					Expect(err).To(Equal(iface.WorkerNotFound))
				})
				When("the worker is Running Phase", func() {
					It("should update phase, reason, finishedAt, lastHeartBeatAt", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

						updatedWorker, err := backend.SetWorkerSucceeded(context.Background(), queue.UID, worker.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(updatedWorker.Spec).To(Equal(SampleWorkerSpec))
						Expect(updatedWorker.UID).To(Equal(worker.UID))
						Expect(updatedWorker.QueueUID).To(Equal(queue.UID))
						Expect(updatedWorker.Status.Phase).To(Equal(apiworker.WorkerPhaseSucceeded))
						Expect(updatedWorker.Status.Reason).To(Equal(apiworker.WorkerReasonSuccess))
						Expect(updatedWorker.Status.StartedAt).To(BeTemporally("==", worker.Status.StartedAt))
						Expect(updatedWorker.Status.LastHeartBeatAt).To(BeTemporally(">", worker.Status.LastHeartBeatAt))
						Expect(updatedWorker.Status.FinishedAt).To(BeTemporally(">", worker.Status.FinishedAt))
						Expect(updatedWorker.Status.FinishedAt).To(BeTemporally("==", updatedWorker.Status.LastHeartBeatAt))
					})
				})
				When("the worker is non-Running Phase", func() {
					It("should raise error", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
						_, err := backend.SetWorkerSucceeded(context.Background(), queue.UID, worker.UID)
						Expect(err).NotTo(HaveOccurred())
						_, err = backend.SetWorkerSucceeded(context.Background(), queue.UID, worker.UID)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(ContainSubstring("Worker must be Running phase to set Succeeded"))
					})
				})
			})
		})

		Context("SetWorkerFailed", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.SetWorkerFailed(context.Background(), uuid.New(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				It("should raise WorkerNotFound error for non-registered workerUID ", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					_, err := backend.SetWorkerFailed(context.Background(), queue.UID, uuid.New())
					Expect(err).To(Equal(iface.WorkerNotFound))
				})
				When("the worker is Running Phase", func() {
					It("should update phase, reason, finishedAt, lastHeartBeatAt", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

						updatedWorker, err := backend.SetWorkerFailed(context.Background(), queue.UID, worker.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(updatedWorker.Spec).To(Equal(SampleWorkerSpec))
						Expect(updatedWorker.UID).To(Equal(worker.UID))
						Expect(updatedWorker.QueueUID).To(Equal(queue.UID))
						Expect(updatedWorker.Status.Phase).To(Equal(apiworker.WorkerPhaseFailed))
						Expect(updatedWorker.Status.Reason).To(Equal(apiworker.WorkerReasonFailure))
						Expect(updatedWorker.Status.StartedAt).To(BeTemporally("==", worker.Status.StartedAt))
						Expect(updatedWorker.Status.LastHeartBeatAt).To(BeTemporally(">", worker.Status.LastHeartBeatAt))
						Expect(updatedWorker.Status.FinishedAt).To(BeTemporally(">", worker.Status.FinishedAt))
						Expect(updatedWorker.Status.FinishedAt).To(BeTemporally("==", updatedWorker.Status.LastHeartBeatAt))
					})
				})
				When("the worker is non-Running Phase", func() {
					It("should raise error", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
						_, err := backend.SetWorkerSucceeded(context.Background(), queue.UID, worker.UID)
						Expect(err).NotTo(HaveOccurred())
						_, err = backend.SetWorkerFailed(context.Background(), queue.UID, worker.UID)
						Expect(err).To(HaveOccurred())
						Expect(err.Error()).To(ContainSubstring("Worker must be Running phase to set Failed"))
					})
				})
			})
		})

		Context("SalvageWorkers", func() {
			When("the queue does not exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, _, err := backend.SalvageWorker(context.Background(), uuid.New(), uuid.New(), uuid.New())
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("the queue exists", func() {
				It("should raise WorkerNotFound error for non-registered workerUID ", func() {
					queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
					_, _, err := backend.SalvageWorker(context.Background(), queue.UID, uuid.New(), uuid.New())
					Expect(err).To(Equal(iface.WorkerNotFound))
				})
				It("should raise WorkerSalvationNotAllowed error for non-expired worker", func() {
					queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
					_, _, err := backend.SalvageWorker(context.Background(), queue.UID, worker.UID, worker.UID)
					Expect(err).To(Equal(iface.WorkerSalvationNotAllowed))
				})
				It("can salvage worker for expired worker", func() {
					// register salvaging worker (nonExpired)
					nonExpiredWorkerSpec := SampleWorkerSpec
					nonExpiredWorkerSpec.HeartBeat = apiworker.HeartBeatSpec{
						SalvageDuration:    1 * time.Hour,
						ExpirationDuration: 1 * time.Hour,
						Interval:           1 * time.Hour,
					}
					queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, nonExpiredWorkerSpec)
					// register salvage target worker (shourtExpired)
					shortExpiredWorkerSpec := SampleWorkerSpec
					shortExpiredWorkerSpec.HeartBeat = apiworker.HeartBeatSpec{
						SalvageDuration:    1 * time.Millisecond,
						ExpirationDuration: 1 * time.Millisecond,
						Interval:           1 * time.Millisecond,
					}
					targetWorker, err := backend.RegisterWorker(context.Background(), queue.UID, shortExpiredWorkerSpec)
					Expect(err).NotTo(HaveOccurred())

					// push 3 tasks
					_, err = backend.AddTask(context.Background(), queue.Spec.Name, SampleTaskSpec)
					Expect(err).NotTo(HaveOccurred())
					_, err = backend.AddTask(context.Background(), queue.Spec.Name, SampleTaskSpec)
					Expect(err).NotTo(HaveOccurred())
					_, err = backend.AddTask(context.Background(), queue.Spec.Name, SampleTaskSpec)
					Expect(err).NotTo(HaveOccurred())

					// fetch 3 tasks by target ( 1 received(=r), 1 processing(=p), 1 succeeded(=s))
					s, err := backend.NextTask(context.Background(), queue.UID, targetWorker.UID)
					Expect(err).NotTo(HaveOccurred())
					err = backend.SetProcessing(context.Background(), queue.UID, targetWorker.UID, s)
					Expect(err).NotTo(HaveOccurred())
					err = backend.SetSucceeded(context.Background(), queue.UID, targetWorker.UID, s, util.StringP(""), util.StringP(""), nil)
					Expect(err).NotTo(HaveOccurred())

					p, err := backend.NextTask(context.Background(), queue.UID, targetWorker.UID)
					Expect(err).NotTo(HaveOccurred())
					err = backend.SetProcessing(context.Background(), queue.UID, targetWorker.UID, p)
					Expect(err).NotTo(HaveOccurred())

					r, err := backend.NextTask(context.Background(), queue.UID, targetWorker.UID)
					Expect(err).NotTo(HaveOccurred())

					targetWorkers, err := backend.GetWorkersToSalvage(context.Background(), queue.UID)
					Expect(err).NotTo(HaveOccurred())
					Expect(targetWorkers[0].UID).To(Equal(targetWorker.UID))

					now := time.Now()
					salvagedWorker, salvagedTasks, err := backend.SalvageWorker(context.Background(), queue.UID, worker.UID, targetWorker.UID)
					Expect(err).NotTo(HaveOccurred())
					Expect(salvagedWorker.UID).To(Equal(targetWorker.UID))
					Expect(salvagedWorker.Spec).To(Equal(targetWorker.Spec))
					Expect(salvagedWorker.Status.Phase).To(Equal(apiworker.WorkerPhaseFailed))
					Expect(salvagedWorker.Status.Reason).To(Equal(apiworker.WorkerReasonSalvaged))
					Expect(*salvagedWorker.Status.SalvagedBy).To(Equal(worker.UID))
					Expect(*salvagedWorker.Status.SalvagedAt).To(BeTemporally(">", now))
					assertKeyContents(backend.workerKey(queue.UID.String(), salvagedWorker.UID.String()), salvagedWorker)

					salvagedTasksUIDs := []string{}
					salvagedTaskMap := map[string]*task.Task{}
					for _, t := range salvagedTasks {
						salvagedTasksUIDs = append(salvagedTasksUIDs, t.UID)
						salvagedTaskMap[t.UID] = t
					}
					Expect(salvagedTasksUIDs).To(HaveLen(2))
					Expect(salvagedTasksUIDs).To(ContainElements(p.UID, r.UID))
					assertSalvaged := func(st task.TaskStatus) {
						Expect(st.Phase).To(Equal(task.TaskPhasePending))
						Expect(st.SalvageCount).To(Equal(1))
						Expect(st.FailureCount).To(Equal(0))
						Expect(st.History).To(HaveLen(1))
						Expect(*st.History[0].SalvagedBy).To(Equal(worker.UID))
						Expect(*st.History[0].SalvagedAt).To(BeTemporally(">=", now))
					}
					assertSalvaged(salvagedTaskMap[r.UID].Status)
					assertSalvaged(salvagedTaskMap[p.UID].Status)
					mustTasksSetSize(queue.UID.String(), 3)
					pendingUids := mustPendingQueueLength(queue.UID.String(), 2)
					Expect(pendingUids).To(ContainElements(salvagedTasksUIDs[0], salvagedTasksUIDs[1]))
					assertKeyContents(backend.taskKey(queue.UID.String(), r.UID), salvagedTaskMap[r.UID])
					assertKeyContents(backend.taskKey(queue.UID.String(), p.UID), salvagedTaskMap[p.UID])
					mustWorkerPendingQueueLength(queue.UID.String(), salvagedWorker.UID.String(), 0)
					mustWorkerTasksSetSize(queue.UID.String(), salvagedWorker.UID.String(), 0)
				})
			})
		})
	})

	Context("Task Operation", func() {
		Context("AddTask", func() {
			When("queue doesn't exist", func() {
				It("should raise TaskQueueNotFound error", func() {
					_, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
					Expect(err).To(Equal(iface.TaskQueueNotFound))
				})
			})
			When("queue and worker exist", func() {
				When("Spec is invalid", func() {
					It("should fail immediately", func() {
						_ = testutil.MustCreateQueue(backend, SampleQueueSpec)
						_, err := backend.AddTask(context.Background(), QueueName, SampleInvalidTaskSpec)
						Expect(err).To(HaveOccurred())
						vErr, ok := err.(*util.ValidationError)
						Expect(ok).To(Equal(true))
						Expect(len(vErr.Errors)).To(Equal(2))
						Expect(vErr.Error()).To(ContainSubstring("TaskSpec.Name max length"))
						Expect(vErr.Error()).To(ContainSubstring("TaskSpec.Payload max size is"))
					})
				})
				When("Spec is valid", func() {
					It("can create a Task", func() {
						queue := testutil.MustCreateQueue(backend, SampleQueueSpec)
						t, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
						Expect(err).NotTo(HaveOccurred())
						Expect(t.UID).NotTo(Equal(""))
						Expect(t.ParentUID).To(BeNil())
						Expect(t.Status.Phase).To(Equal(task.TaskPhasePending))

						pending := mustPendingQueueLength(queue.UID.String(), 1)
						Expect(pending[0]).To(Equal(t.UID))
						tasks := mustTasksSetSize(queue.UID.String(), 1)
						Expect(tasks[0]).To(Equal(t.UID))
						assertKeyContents(backend.taskKey(queue.UID.String(), t.UID), t)
					})
				})
			})
		})

		Context("NextTask", func() {
			Context("queue is suspended", func() {
				It("returns TaskSuspendedError even when queue has some pending tasks", func() {
					// make queue suspended
					queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
					suspendSpec := SampleQueueSpec
					suspendSpec.State = taskqueue.TaskQueueStateSuspend
					testutil.MustQueueWithState(backend, suspendSpec)

					// when queue is empty
					_ = mustPendingQueueLength(queue.UID.String(), 0)
					t, err := backend.NextTask(context.Background(), queue.UID, worker.UID)
					Expect(err).To(Equal(iface.TaskSuspendedError))
					Expect(t).To(BeNil())

					// add task to the queue
					t, err = backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
					Expect(err).NotTo(HaveOccurred())
					Expect(t).NotTo(BeNil())
					_ = mustPendingQueueLength(queue.UID.String(), 1)

					// try to pop from pending queue with 1 task
					t, err = backend.NextTask(context.Background(), queue.UID, worker.UID)
					Expect(err).To(Equal(iface.TaskSuspendedError))
					Expect(t).To(BeNil())
				})
			})
			Context("queue is active", func() {
				When("queue is empty", func() {
					It("should return TaskQueueEmpty Error", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

						// when queue is empty
						_ = mustPendingQueueLength(queue.UID.String(), 0)

						t, err := backend.NextTask(context.Background(), queue.UID, worker.UID)
						Expect(err).To(Equal(iface.TaskQueueEmptyError))
						Expect(t).To(BeNil())
					})
				})
				When("queue is non empty", func() {
					It("should pop pending the task and move it pending key atomically", func() {
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)

						t, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
						Expect(err).NotTo(HaveOccurred())
						Expect(t).NotTo(BeNil())
						_ = mustPendingQueueLength(queue.UID.String(), 1)

						now := time.Now()
						popped, err := backend.NextTask(context.Background(), queue.UID, worker.UID)

						Expect(err).NotTo(HaveOccurred())
						Expect(popped.UID).To(Equal(t.UID))
						Expect(popped.Spec).To(Equal(SampleTaskSpec))
						Expect(popped.Status.Phase).To(Equal(task.TaskPhaseReceived))
						Expect(popped.Status.CurrentRecord.WorkerName).To(Equal(worker.Spec.Name))
						Expect(popped.Status.CurrentRecord.WorkerUID).To(Equal(worker.UID.String()))
						Expect(popped.Status.CurrentRecord.ReceivedAt).To(BeTemporally(">=", now))
						Expect(popped.Status.CurrentRecord.StartedAt).To(BeNil())
						Expect(popped.Status.CurrentRecord.FinishedAt).To(BeNil())
						Expect(popped.Status.CurrentRecord.SalvagedBy).To(BeNil())
						Expect(popped.Status.CurrentRecord.SalvagedAt).To(BeNil())
						Expect(popped.Status.CurrentRecord.Result).To(BeNil())
						Expect(popped.Status.FailureCount).To(Equal(0))
						Expect(popped.Status.SalvageCount).To(Equal(0))
						Expect(popped.Status.History).To(HaveLen(0))
						mustPendingQueueLength(queue.UID.String(), 0)
						mustTasksSetSize(queue.UID.String(), 1)
						mustWorkerPendingQueueLength(queue.UID.String(), worker.UID.String(), 0)
						mustWorkerTasksSetSize(queue.UID.String(), worker.UID.String(), 1)
						assertKeyContents(backend.taskKey(queue.UID.String(), t.UID), popped)
					})
				})
			})
		})

		Context("SetProcessing", func() {
			It("should update phase and currentRecord", func() {
				// setup: add --> pop --> set processing
				queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
				t, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
				Expect(err).NotTo(HaveOccurred())
				now := time.Now()
				s, err := backend.NextTask(context.Background(), queue.UID, worker.UID)
				Expect(err).NotTo(HaveOccurred())

				err = backend.SetProcessing(context.Background(), queue.UID, worker.UID, s)

				Expect(err).NotTo(HaveOccurred())
				Expect(s.UID).To(Equal(t.UID))
				Expect(s.Spec).To(Equal(SampleTaskSpec))
				Expect(s.Status.Phase).To(Equal(task.TaskPhaseProcessing))
				Expect(s.Status.CurrentRecord.WorkerName).To(Equal(worker.Spec.Name))
				Expect(s.Status.CurrentRecord.WorkerUID).To(Equal(worker.UID.String()))
				Expect(s.Status.CurrentRecord.ReceivedAt).To(BeTemporally(">=", now))
				Expect(*s.Status.CurrentRecord.StartedAt).To(BeTemporally(">=", now))
				Expect(*s.Status.CurrentRecord.StartedAt).To(BeTemporally(">", s.Status.CurrentRecord.ReceivedAt))
				Expect(s.Status.CurrentRecord.FinishedAt).To(BeNil())
				Expect(s.Status.CurrentRecord.SalvagedBy).To(BeNil())
				Expect(s.Status.CurrentRecord.SalvagedAt).To(BeNil())
				Expect(s.Status.CurrentRecord.Result).To(BeNil())
				Expect(s.Status.FailureCount).To(Equal(0))
				Expect(s.Status.SalvageCount).To(Equal(0))
				Expect(s.Status.History).To(HaveLen(0))

				mustPendingQueueLength(queue.UID.String(), 0)
				mustTasksSetSize(queue.UID.String(), 1)
				mustWorkerPendingQueueLength(queue.UID.String(), worker.UID.String(), 0)
				mustWorkerTasksSetSize(queue.UID.String(), worker.UID.String(), 1)
				assertKeyContents(backend.taskKey(queue.UID.String(), s.UID), s)
			})
		})

		Context("SetSucceeded", func() {
			It("should delete processing, set completed and push onsuccess atomically", func() {
				// setup: add --> pop --> set processing --> set succeded
				queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
				_, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
				Expect(err).NotTo(HaveOccurred())
				now := time.Now()
				t, err := backend.NextTask(context.Background(), queue.UID, worker.UID)
				Expect(err).NotTo(HaveOccurred())
				err = backend.SetProcessing(context.Background(), queue.UID, worker.UID, t)
				Expect(err).NotTo(HaveOccurred())

				payload := "test_result"
				msg := "test"
				onSuccessSpecs := []task.TaskSpec{SampleTaskSpec, SampleInvalidTaskSpec}
				err = backend.SetSucceeded(context.Background(), queue.UID, worker.UID, t, &payload, &msg, onSuccessSpecs)

				// assertions
				Expect(err).NotTo(HaveOccurred())
				Expect(t.UID).To(Equal(t.UID))
				Expect(t.Spec).To(Equal(SampleTaskSpec))
				Expect(t.Status.Phase).To(Equal(task.TaskPhaseSucceeded))
				Expect(t.Status.CurrentRecord).To(BeNil())
				Expect(t.Status.FailureCount).To(Equal(0))
				Expect(t.Status.SalvageCount).To(Equal(0))
				Expect(t.Status.History).To(HaveLen(1))
				Expect(t.Status.History[0].WorkerName).To(Equal(worker.Spec.Name))
				Expect(t.Status.History[0].WorkerUID).To(Equal(worker.UID.String()))
				Expect(t.Status.History[0].ReceivedAt).To(BeTemporally(">=", now))
				Expect(*t.Status.History[0].StartedAt).To(BeTemporally(">=", now))
				Expect(*t.Status.History[0].StartedAt).To(BeTemporally(">", t.Status.History[0].ReceivedAt))
				Expect(*t.Status.History[0].FinishedAt).To(BeTemporally(">", *t.Status.History[0].StartedAt))
				Expect(t.Status.History[0].SalvagedBy).To(BeNil())
				Expect(t.Status.History[0].SalvagedAt).To(BeNil())
				Expect(t.Status.History[0].Result.Type).To(Equal(task.TaskResultSuccess))
				Expect(t.Status.History[0].Result.Reason).To(Equal(task.TaskResultReasonSucceded))
				Expect(*t.Status.History[0].Result.Payload).To(Equal(payload))
				Expect(*t.Status.History[0].Result.Message).To(Equal(msg))
				// hook should be in pending queue
				pUIDs := mustPendingQueueLength(queue.UID.String(), 1)
				raw, err := client.Get(backend.taskKey(queue.UID.String(), pUIDs[0])).Result()
				Expect(err).NotTo(HaveOccurred())
				var hookTask task.Task
				Expect(json.Unmarshal([]byte(raw), &hookTask)).NotTo(HaveOccurred())
				Expect(hookTask.Spec).To(Equal(SampleTaskSpec))
				// task set increased to 2
				mustTasksSetSize(queue.UID.String(), 2)
				// the worker queue,tasks set are empty
				mustWorkerPendingQueueLength(queue.UID.String(), worker.UID.String(), 0)
				mustWorkerTasksSetSize(queue.UID.String(), worker.UID.String(), 0)
				// deadletter has 1 element
				mustDeadletterLength(queue.UID.String(), 1)
			})
		})

		Context("RecordFailure", func() {
			When("it will retry", func() {
				It("should requeue the task and hooks to pending", func() {
					// setup: add --> pop --> set processing --> set record
					queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
					_, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
					Expect(err).NotTo(HaveOccurred())
					now := time.Now()
					t, err := backend.NextTask(context.Background(), queue.UID, worker.UID)
					Expect(err).NotTo(HaveOccurred())
					err = backend.SetProcessing(context.Background(), queue.UID, worker.UID, t)
					Expect(err).NotTo(HaveOccurred())

					payload := "test_result"
					msg := "failure"
					onFailureSpecs := []task.TaskSpec{SampleTaskSpec, SampleInvalidTaskSpec}
					err = backend.RecordFailure(context.Background(), queue.UID, worker.UID, t, &payload, &msg, task.TaskResultReasonFailed, onFailureSpecs)
					Expect(err).NotTo(HaveOccurred())

					// assertion
					Expect(err).NotTo(HaveOccurred())
					Expect(t.UID).To(Equal(t.UID))
					Expect(t.Spec).To(Equal(SampleTaskSpec))
					Expect(t.Status.Phase).To(Equal(task.TaskPhasePending))
					Expect(t.Status.CurrentRecord).To(BeNil())
					Expect(t.Status.FailureCount).To(Equal(1))
					Expect(t.Status.SalvageCount).To(Equal(0))
					Expect(t.Status.History).To(HaveLen(1))
					Expect(t.Status.History[0].WorkerName).To(Equal(worker.Spec.Name))
					Expect(t.Status.History[0].WorkerUID).To(Equal(worker.UID.String()))
					Expect(t.Status.History[0].ReceivedAt).To(BeTemporally(">=", now))
					Expect(*t.Status.History[0].StartedAt).To(BeTemporally(">=", now))
					Expect(*t.Status.History[0].StartedAt).To(BeTemporally(">", t.Status.History[0].ReceivedAt))
					Expect(*t.Status.History[0].FinishedAt).To(BeTemporally(">", *t.Status.History[0].StartedAt))
					Expect(t.Status.History[0].SalvagedBy).To(BeNil())
					Expect(t.Status.History[0].Result.Type).To(Equal(task.TaskResultFailure))
					Expect(t.Status.History[0].Result.Reason).To(Equal(task.TaskResultReasonFailed))
					Expect(*t.Status.History[0].Result.Payload).To(Equal(payload))
					Expect(*t.Status.History[0].Result.Message).To(Equal(msg))
					// hook and retried task should be in pending queue
					pUIDs := mustPendingQueueLength(queue.UID.String(), 2)
					raw, err := client.Get(backend.taskKey(queue.UID.String(), pUIDs[0])).Result()
					Expect(err).NotTo(HaveOccurred())
					var hookTask task.Task
					Expect(json.Unmarshal([]byte(raw), &hookTask)).NotTo(HaveOccurred())
					Expect(hookTask.Spec).To(Equal(SampleTaskSpec))
					Expect(pUIDs[1]).To(Equal(t.UID))
					assertKeyContents(backend.taskKey(queue.UID.String(), t.UID), t)
					// task set increased to 2
					mustTasksSetSize(queue.UID.String(), 2)
					// the worker queue,tasks set are empty
					mustWorkerPendingQueueLength(queue.UID.String(), worker.UID.String(), 0)
					mustWorkerTasksSetSize(queue.UID.String(), worker.UID.String(), 0)
					// deadletter has 1 element
					mustDeadletterLength(queue.UID.String(), 1)
				})
				When("excceeds retry limit", func() {
					It("completes the task as failure and requeue hooks to pending", func() {
						// setup: add --> (pop --> set processing --> set recordFailure) x 2
						queue, worker := testutil.MustCreateQueueAndRegisterWorker(backend, SampleQueueSpec, SampleWorkerSpec)
						_, err := backend.AddTask(context.Background(), QueueName, SampleTaskSpec)
						Expect(err).NotTo(HaveOccurred())

						now := time.Now()
						t, err := backend.NextTask(context.Background(), queue.UID, worker.UID)
						Expect(err).NotTo(HaveOccurred())
						err = backend.SetProcessing(context.Background(), queue.UID, worker.UID, t)
						Expect(err).NotTo(HaveOccurred())
						payload := "test_result"
						msg := "failure"
						err = backend.RecordFailure(context.Background(), queue.UID, worker.UID, t, &payload, &msg, task.TaskResultReasonFailed, nil)
						Expect(err).NotTo(HaveOccurred())

						t, err = backend.NextTask(context.Background(), queue.UID, worker.UID)
						Expect(err).NotTo(HaveOccurred())
						err = backend.SetProcessing(context.Background(), queue.UID, worker.UID, t)
						Expect(err).NotTo(HaveOccurred())
						onFailureSpecs := []task.TaskSpec{SampleTaskSpec, SampleInvalidTaskSpec}
						err = backend.RecordFailure(context.Background(), queue.UID, worker.UID, t, &payload, &msg, task.TaskResultReasonFailed, onFailureSpecs)
						Expect(err).NotTo(HaveOccurred())

						// assertion
						Expect(err).NotTo(HaveOccurred())
						Expect(t.UID).To(Equal(t.UID))
						Expect(t.Spec).To(Equal(SampleTaskSpec))
						Expect(t.Status.Phase).To(Equal(task.TaskPhaseFailed))
						Expect(t.Status.CurrentRecord).To(BeNil())
						Expect(t.Status.FailureCount).To(Equal(2))
						Expect(t.Status.SalvageCount).To(Equal(0))
						Expect(t.Status.History).To(HaveLen(2))
						assertHistory := func(r task.TaskRecord) {
							Expect(r.WorkerName).To(Equal(worker.Spec.Name))
							Expect(r.WorkerUID).To(Equal(worker.UID.String()))
							Expect(r.ReceivedAt).To(BeTemporally(">=", now))
							Expect(*r.StartedAt).To(BeTemporally(">=", now))
							Expect(*r.StartedAt).To(BeTemporally(">", r.ReceivedAt))
							Expect(*r.FinishedAt).To(BeTemporally(">", *r.StartedAt))
							Expect(r.SalvagedBy).To(BeNil())
							Expect(r.SalvagedAt).To(BeNil())
							Expect(r.Result.Type).To(Equal(task.TaskResultFailure))
							Expect(r.Result.Reason).To(Equal(task.TaskResultReasonFailed))
							Expect(*r.Result.Payload).To(Equal(payload))
							Expect(*r.Result.Message).To(Equal(msg))
						}
						assertHistory(t.Status.History[0])
						assertHistory(t.Status.History[1])
						assertKeyContents(backend.taskKey(queue.UID.String(), t.UID), t)
						// hook should be in pending queue
						pUIDs := mustPendingQueueLength(queue.UID.String(), 1)
						raw, err := client.Get(backend.taskKey(queue.UID.String(), pUIDs[0])).Result()
						Expect(err).NotTo(HaveOccurred())
						var hookTask task.Task
						Expect(json.Unmarshal([]byte(raw), &hookTask)).NotTo(HaveOccurred())
						Expect(hookTask.Spec).To(Equal(SampleTaskSpec))
						// task set increased to 2
						mustTasksSetSize(queue.UID.String(), 2)
						// the worker queue,tasks set are empty
						mustWorkerPendingQueueLength(queue.UID.String(), worker.UID.String(), 0)
						mustWorkerTasksSetSize(queue.UID.String(), worker.UID.String(), 0)
						// deadletter has 1 element
						mustDeadletterLength(queue.UID.String(), 1)
					})
				})
			})
		})
	})
})
