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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	"github.com/pfnet-research/pftaskqueue/pkg/worker/config"

	"github.com/google/uuid"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	backend "github.com/pfnet-research/pftaskqueue/pkg/backend/iface"
	"github.com/pfnet-research/pftaskqueue/pkg/util"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"
)

const (
	TaskHandlerWorkdirEnvVarName = "PFTQ_TASK_HANDLER_WORKSPACE_DIR"
	TaskHandlerInputEnvVarPrefix = "PFTQ_TASK_HANDLER_INPUT_"
)

func NewWorker(ctx context.Context, logger zerolog.Logger, backend backend.Backend, cfg config.WorkerConfig) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	workerCtx, workerCtxCancel := context.WithCancel(ctx)
	heartbeatCtx, heartbeatCtxCancel := context.WithCancel(workerCtx)
	wkr := &Worker{
		config:             cfg,
		parentLogger:       logger,
		backend:            backend,
		fetchLock:          sync.Mutex{},
		wg:                 sync.WaitGroup{},
		ctx:                workerCtx,
		ctxCancel:          workerCtxCancel,
		heartbeatCtx:       heartbeatCtx,
		heartbeatCtxCancel: heartbeatCtxCancel,
		heatBeatStopped:    make(chan struct{}),
		smphr:              semaphore.NewWeighted(int64(cfg.Concurrency)),
		logger: logger.With().
			Str("component", "worker").
			Str("workerName", cfg.Name).
			Str("queueName", cfg.QueueName).Logger(),
	}

	return wkr, nil
}

type Worker struct {
	uid          uuid.UUID
	config       config.WorkerConfig
	parentLogger zerolog.Logger
	logger       zerolog.Logger
	backend      backend.Backend

	// target queue uuid
	queueUID uuid.UUID

	// make fetch serially
	fetchLock sync.Mutex

	// to graceful shutdown
	ctx                context.Context
	ctxCancel          context.CancelFunc
	heartbeatCtx       context.Context
	heartbeatCtxCancel context.CancelFunc
	heatBeatStopped    chan struct{}
	wg                 sync.WaitGroup

	// limit concurrency
	smphr *semaphore.Weighted
}

func (w *Worker) Start() error {
	defer w.ctxCancel()
	if err := w.init(); err != nil {
		return err
	}

	w.startHeatBeat()

	w.salvageLostWorkers()

	err := w.startProcessTasks()

	w.stop(err)
	return err
}

func (w *Worker) startProcessTasks() error {
	var queueEmptyDetectedAt *time.Time
L:
	for i := w.config.NumTasks; i != 0; i-- {
		select {
		case <-w.ctx.Done():
			break L
		default:
			err := w.smphr.Acquire(context.Background(), 1)
			if err != nil {
				w.logger.Error().Err(err).Msg("Can't acquire semaphore.  retrying in 5 seconds")
				util.SleepContext(w.ctx, 5*time.Second)
				continue
			}

			w.logger.Debug().Msg("Fetching next task from the queue")
			taskFetched, err := w.fetchTask(w.ctx)
			if err != nil {
				if _, ok := err.(*util.ValidationError); ok {
					w.logger.Info().Msg("Validation error. Stopping worker")
					w.smphr.Release(1)
					return err
				}
				switch err {
				case backend.TaskQueueNotFound:
					w.logger.Warn().Msg("Queue is not found. Stopping worker.")
					w.smphr.Release(1)
					return err
				case backend.TaskSuspendedError:
					if w.config.ExitOnSuspend {
						w.logger.Info().Bool("exitOnSuspend", w.config.ExitOnSuspend).Msg("Queue is suspended. Stopping worker")
						w.smphr.Release(1)
						return nil
					}
					w.logger.Debug().Bool("exitOnSuspend", w.config.ExitOnSuspend).Msg("Queue is suspended. retrying in 5 seconds.")
					util.SleepContext(w.ctx, 5*time.Second)
					w.smphr.Release(1)
					continue
				case backend.TaskQueueEmptyError:
					if queueEmptyDetectedAt == nil {
						now := time.Now()
						queueEmptyDetectedAt = &now
					}
					logger := w.logger.With().
						Bool("exitOnEmpty", w.config.ExitOnEmpty).
						Dur("exitOnEmptyGracePeriod", w.config.ExitOnEmptyGracePeriod).
						Time("detectedQueueEmptyAt", *queueEmptyDetectedAt).Logger()
					shouldExitNow := !time.Now().Before(queueEmptyDetectedAt.Add(w.config.ExitOnEmptyGracePeriod))
					if w.config.ExitOnEmpty && shouldExitNow {
						logger.Info().Msg("Queue is empty. Stopping worker")
						w.smphr.Release(1)
						return nil
					}
					logger.Info().Msg("Queue is empty. retrying in 5 seconds.")
					util.SleepContext(w.ctx, 5*time.Second)
					w.smphr.Release(1)
					continue
				default:
					w.logger.Error().Err(err).Msg("Error happened in fetching tasks from queue. retrying in 5 seconds")
					util.SleepContext(w.ctx, 5*time.Second)
					w.smphr.Release(1)
					continue
				}
			}
			w.logger.Debug().Interface("task", taskFetched).Msg("Task fetched")
			queueEmptyDetectedAt = nil
			w.wg.Add(1)
			go func() {
				defer w.smphr.Release(1)
				defer w.wg.Done()
				w.processOneTask(taskFetched)
			}()
		}
	}

	w.logger.Info().Int("numTasks", w.config.NumTasks).Msg("Completed processing configured number of tasks")
	return nil
}

func (w *Worker) processOneTask(taskFetched *task.Task) {
	logger := w.logger.With().
		Str("taskUID", taskFetched.UID).
		Interface("taskSpec", taskFetched.Spec).
		Str("processUID", taskFetched.Status.CurrentRecord.ProcessUID).Logger()

	result, hooks := w.runCommand(logger, taskFetched)

	// Use context.Background() instead of worker.ctx
	// because we should make sure record task handler result
	// even when the worker context was canceled.
	switch result.Type {
	case task.TaskResultSuccess:
		if err := w.backend.SetSucceeded(context.Background(),
			w.queueUID, w.uid, taskFetched, result.Payload, result.Message, hooks,
		); err != nil {
			if _, ok := err.(taskqueue.TaskToDeadletterError); ok {
				logger.Error().Err(err).Msg("the task was sent to dead-letter")
				return
			}
			logger.Error().Err(err).Msg("Failed to set task Succeeded, the task will be salvaged later")
			return
		}
		logger.Info().
			Interface("taskResult", result).
			Msg("Task marked Succeeded")

	default:
		if err := w.backend.RecordFailure(context.Background(),
			w.queueUID, w.uid, taskFetched, result.Payload, result.Message, result.Reason, hooks,
		); err != nil {
			if _, ok := err.(taskqueue.TaskToDeadletterError); ok {
				logger.Error().Err(err).Msg("the task was sent to dead-letter")
				return
			}
			logger.Error().Err(err).Msg("Failed to record task Failure, the task will be salvaged later")
			return
		}
		logger.Info().
			Interface("taskResult", result).
			Msg("Task marked Failure")
	}
}
func (w *Worker) stop(err error) {
	w.logger.Info().Msg("Waiting for all the ongoing tasks finished")
	w.heartbeatCtxCancel()
	<-w.heatBeatStopped
	w.wg.Wait()
	if err != nil {
		_, err := w.backend.SetWorkerFailed(context.Background(), w.queueUID, w.uid)
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to set the worker failed. The worker will salvaged later")
		}
	} else {
		_, err := w.backend.SetWorkerSucceeded(context.Background(), w.queueUID, w.uid)
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to set the worker succeeded. The worker will salvaged later")
		}
	}
	w.logger.Info().Msg("Worker stopped")
}

func (w *Worker) fetchTask(ctx context.Context) (*task.Task, error) {
	w.fetchLock.Lock()
	defer w.fetchLock.Unlock()
	return w.backend.NextTask(ctx, w.queueUID, w.uid)
}

func (w *Worker) runCommand(logger zerolog.Logger, t *task.Task) (task.TaskResult, []task.TaskSpec) {
	logger = logger.With().
		Strs("taskHandlerCommands", w.config.TaskHandler.Commands).
		Logger()

	logger.Info().Msg("Start processing a task")

	err := w.backend.SetProcessing(w.ctx, w.queueUID, w.uid, t)
	if err != nil {
		msg := "Can't mark Processing in queue"
		result := task.TaskResult{
			Type:    task.TaskResultFailure,
			Reason:  task.TaskResultReasonInternalError,
			Message: util.StringP(errors.Wrapf(err, msg).Error()),
		}
		logger.Error().Err(err).Interface("taskResult", result).Msg(msg)
		return result, []task.TaskSpec{}
	}

	workspacePath, envvars, err := w.prepareTaskHandlerDirAndEnvvars(t)
	if err != nil {
		msg := "Can't prepare workspace dir for task handler process"
		result := task.TaskResult{
			Type:    task.TaskResultFailure,
			Reason:  task.TaskResultReasonInternalError,
			Message: util.StringP(errors.Wrapf(err, msg).Error()),
		}
		logger.Error().Err(err).Interface("taskResult", result).Msg(msg)
		return result, []task.TaskSpec{}
	}

	cmdCtx, cmdCtxCancel := context.WithTimeout(w.ctx, t.Spec.ActualTimeout(w.config.TaskHandler.DefaultCommandTimeout))
	defer cmdCtxCancel()

	cmd := exec.Command(w.config.TaskHandler.Commands[0], w.config.TaskHandler.Commands[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	cmd.Env = append(os.Environ(), envvars...)

	// Inject workspace path to stdin
	rStdout, wStdout := io.Pipe()
	rStderr, wStderr := io.Pipe()
	cmd.Stdin = strings.NewReader(workspacePath)
	cmd.Stdout = wStdout
	cmd.Stderr = wStderr

	streamWg := sync.WaitGroup{}
	streamLogger := w.parentLogger.With().Str("taskUID", t.UID).Str("processUID", t.Status.CurrentRecord.ProcessUID).Logger()
	w.startStreamToLogger(
		cmdCtx, &streamWg, rStdout,
		streamLogger.With().Str("pipe", "stdout").Logger(),
	)
	w.startStreamToLogger(
		cmdCtx, &streamWg, rStderr,
		streamLogger.With().Str("pipe", "stderr").Logger(),
	)

	// Run
	cmdDone := make(chan error)
	var cmdErr error
	go func() {
		cmdDone <- cmd.Run()
	}()
	select {
	case <-cmdCtx.Done():
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			streamLogger.Error().Int("pid", cmd.Process.Pid).Err(err).Msg("Failed to kill the process and its descendants")
		}
		cmdErr = cmdCtx.Err()
	case err := <-cmdDone:
		cmdErr = err
		cmdCtxCancel()
	}
	_ = wStdout.Close()
	_ = wStderr.Close()
	streamWg.Wait()

	resultPayload, resultMessage, postHooks, err := w.fetchResultContents(logger, t)
	if err != nil {
		return task.TaskResult{
			Type:    task.TaskResultFailure,
			Reason:  task.TaskResultReasonInternalError,
			Message: util.StringP(err.Error()),
		}, postHooks
	}

	if cmdErr != nil {
		failureResult := task.TaskResult{
			Type:    task.TaskResultFailure,
			Message: resultMessage,
			Payload: resultPayload,
		}
		if cmdErr == context.DeadlineExceeded {
			failureResult.Reason = task.TaskResultReasonTimeout
			logger.Error().Err(cmdErr).Interface("taskResult", failureResult).Interface("postHooks", postHooks).Msg("Task handler process timeout")
			return failureResult, postHooks
		}
		if cmdErr == context.Canceled {
			failureResult.Reason = task.TaskResultReasonSignaled
			logger.Err(cmdErr).Interface("taskResult", failureResult).Interface("postHooks", postHooks).Msg("Task handler process signaled")
			return failureResult, postHooks
		}
		if exerr, ok := cmdErr.(*exec.ExitError); ok {
			failureResult.Reason = task.TaskResultReasonFailed
			logger.Err(exerr).Interface("taskResult", failureResult).Interface("postHooks", postHooks).Msg("Task handler process exited with error")
			return failureResult, postHooks
		}
		// something bad happened
		failureResult.Reason = task.TaskResultReasonFailed
		logger.Err(cmdErr).Interface("taskResult", failureResult).Interface("postHooks", postHooks).Msg("Error happened in task handler process")
		return failureResult, postHooks
	}

	// command success
	result := task.TaskResult{
		Type:    task.TaskResultSuccess,
		Reason:  task.TaskResultReasonSucceded,
		Message: resultMessage,
		Payload: resultPayload,
	}
	logger.Debug().Interface("taskResult", result).Interface("postHooks", postHooks).Msg("Finished processing a task")
	return result, postHooks
}

func (w *Worker) thisWorkerWorkDir() string {
	return filepath.Join(w.config.WorkDir, w.uid.String())
}
func (w *Worker) workspacePath(t *task.Task) string {
	return filepath.Join(w.thisWorkerWorkDir(), t.Status.CurrentRecord.ProcessUID)
}

//	 {task workspace path}/
//	   input/
//	     # taskspec info
//	     payload             # also exported as PFTQ_TASK_HANDLER_INPUT_PAYLOAD
//	     retryLimit          # also exported as PFTQ_TASK_HANDLER_INPUT_RETRY_LIMIT
//	     timeoutSeconds      # also exported as PFTQ_TASK_HANDLER_INPUT_TIMEOUT_SECONDS
//	     meta/
//			  taskUID           # also exported as PFTQ_TASK_HANDLER_INPUT_TASK_UID
//	       task.json         # also exported as PFTQ_TASK_HANDLER_INPUT_TASK_JSON
//	       processUID        # also exported as PFTQ_TASK_HANDLER_INPUT_PROCESS_UID
//	       # worker info
//	       workerName        # also exported as PFTQ_TASK_HANDLER_INPUT_WORKER_NAME
//	       workerUID         # also exported as PFTQ_TASK_HANDLER_INPUT_WORKER_UID
//	       workerConfig.json # also exported as PFTQ_TASK_HANDLER_INPUT_WORKER_CONFIG_JSON
//	   output/
func (w *Worker) prepareTaskHandlerDirAndEnvvars(t *task.Task) (string, []string, error) {
	inputOutputDirPermission := os.FileMode(0750)
	inputFilePermission := os.FileMode(0440)
	workspacePath := w.workspacePath(t)
	inputPath := filepath.Join(workspacePath, "input")
	inputMetaPath := filepath.Join(workspacePath, "input", "meta")

	envvars := []string{
		fmt.Sprintf("%s=%s", TaskHandlerWorkdirEnvVarName, workspacePath),
	}

	if err := os.MkdirAll(inputMetaPath, inputOutputDirPermission); err != nil {
		return "", nil, err
	}
	outputPath := filepath.Join(workspacePath, "output")
	if err := os.MkdirAll(outputPath, inputOutputDirPermission); err != nil {
		return "", nil, err
	}

	payloadPath := filepath.Join(inputPath, "payload")
	if err := ioutil.WriteFile(payloadPath, []byte(t.Spec.Payload), inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"PAYLOAD", t.Spec.Payload))

	retryLimitPath := filepath.Join(inputPath, "retryLimit")
	if err := ioutil.WriteFile(retryLimitPath, []byte(strconv.Itoa(t.Spec.RetryLimit)), inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"RETRY_LIMIT", strconv.Itoa(t.Spec.RetryLimit)))

	timeoutSecondsPath := filepath.Join(inputPath, "timeoutSeconds")
	if err := ioutil.WriteFile(timeoutSecondsPath, []byte(strconv.Itoa(t.Spec.TimeoutSeconds)), inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"TIMEOUT_SECONDS", strconv.Itoa(t.Spec.TimeoutSeconds)))

	taskUIDPath := filepath.Join(inputMetaPath, "taskUID")
	if err := ioutil.WriteFile(taskUIDPath, []byte(t.UID), 0400); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"TASK_UID", t.UID))

	processUIDPath := filepath.Join(inputMetaPath, "processUID")
	if err := ioutil.WriteFile(processUIDPath, []byte(t.Status.CurrentRecord.ProcessUID), inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"PROCESS_UID", t.Status.CurrentRecord.ProcessUID))

	taskJsonPath := filepath.Join(inputMetaPath, "task.json")
	taskJsonBytes, err := json.Marshal(t)
	if err != nil {
		return "", nil, err
	}
	if err := ioutil.WriteFile(taskJsonPath, taskJsonBytes, inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"TASK_JSON", string(taskJsonBytes)))

	workerNamePath := filepath.Join(inputMetaPath, "workerName")
	if err := ioutil.WriteFile(workerNamePath, []byte(w.config.Name), inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"WORKER_NAME", w.config.Name))

	workerUIDPath := filepath.Join(inputMetaPath, "workerUID")
	if err := ioutil.WriteFile(workerUIDPath, []byte(w.uid.String()), inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"WORKER_UID", w.uid.String()))

	workerConfigJson := filepath.Join(inputMetaPath, "workerConfig.json")
	workerConfigJsonBytes, err := json.Marshal(&(w.config))
	if err != nil {
		return "", nil, err
	}
	if err := ioutil.WriteFile(workerConfigJson, workerConfigJsonBytes, inputFilePermission); err != nil {
		return "", nil, err
	}
	envvars = append(envvars, fmt.Sprintf("%s=%s", TaskHandlerInputEnvVarPrefix+"WORKER_CONFIG_JSON", string(workerConfigJsonBytes)))

	return workspacePath, envvars, nil
}

// {task workspace path}/
//
//	output/
//	  message
//	  payload
//	  postHooks.json
func (w *Worker) fetchResultContents(logger zerolog.Logger, t *task.Task) (*string, *string, []task.TaskSpec, error) {
	workspaceDir := w.workspacePath(t)
	postHooksPath := filepath.Join(workspaceDir, "output", "postHooks.json")
	resultPayloadPath := filepath.Join(workspaceDir, "output", "payload")
	messagePath := filepath.Join(workspaceDir, "output", "message")

	// read task success/failure postHooks
	postHooks := []task.TaskSpec{}
	found, err := readJsonFromFile(postHooksPath, &postHooks)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "Failed to read postHooks.json")
	}
	if !found {
		logger.Debug().Str("postHooksPath", postHooksPath).Msg("output/postHooks.json not found. Skipped")
	}

	// read output/payload
	found, resultPayload, err := readStringFromFile(resultPayloadPath)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "Failed to read output/payload")
	}
	if !found {
		logger.Debug().Str("payloadPath", resultPayloadPath).Msg("output/payload not found. Skipped")
	}

	found, resultMessage, err := readStringFromFile(messagePath)
	if err != nil {
		return nil, nil, nil, err
	}
	if !found {
		logger.Debug().Str("messagePath", messagePath).Msg("output/message not found. Skipped")
	}

	return resultPayload, resultMessage, postHooks, nil
}

func readStringFromFile(pathToRead string) (bool, *string, error) {
	found := util.IsPathExists(pathToRead)
	if found {
		bytes, err := ioutil.ReadFile(pathToRead)
		if err != nil {
			return true, nil, err
		}
		return true, util.StringP(string(bytes)), nil
	}
	return false, nil, nil
}

func readJsonFromFile(pathToRead string, v interface{}) (bool, error) {
	if util.IsPathExists(pathToRead) {
		bytes, err := ioutil.ReadFile(pathToRead)
		if err != nil {
			return true, err
		} else {
			if err := json.Unmarshal(bytes, v); err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
				return true, err
			}
			return true, nil
		}
	}
	return false, nil
}

func (w *Worker) startStreamToLogger(cmdCtx context.Context, wg *sync.WaitGroup, reader io.Reader, logger zerolog.Logger) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		reader := bufio.NewReader(reader)
		for {
			select {
			case <-cmdCtx.Done():
				return
			default:
				line, err := reader.ReadString('\n')
				if err != nil && err != io.EOF {
					logger.Error().Err(err).Msg("Reader error. Stop reading")
					return
				}
				allLinesProcessed := err == io.EOF && len(line) == 0
				if allLinesProcessed {
					return
				}
				logger.Info().Msg(strings.TrimRight(line, "\n"))
			}
		}
	}()
}

func (w *Worker) startHeatBeat() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.logger.Debug().Msg("Start sending heartbeat")
		var updatedWorker *worker.Worker
	LOOP:
		for {
			var err error
			updatedWorker, err = w.backend.SendWorkerHeartBeat(w.ctx, w.queueUID, w.uid)
			if err != nil && updatedWorker == nil {
				w.logger.Error().Err(err).Msg("Failed to initial heartbeat. Aborting")
				break LOOP
			} else if err != nil {
				w.logger.Warn().Err(err).
					Time("lastHeartBeatAt", updatedWorker.Status.LastHeartBeatAt).
					Msg("Failed to heartbeat.  If lastHeartBeat expired, the worker will stop.")
			}

			if updatedWorker.Status.LastHeartBeatAt.Add(w.config.HeartBeat.ExpirationDuration).Before(time.Now()) {
				w.logger.Error().
					Time("lastHeartBeatAt", updatedWorker.Status.LastHeartBeatAt).
					Dur("expirationDuration", w.config.HeartBeat.ExpirationDuration).
					Msg("HeartBeat expired. The worker will stop")
				// this will break in the below select clause
				w.ctxCancel()
			} else {
				w.logger.Debug().
					Time("lastHeartBeatAt", updatedWorker.Status.LastHeartBeatAt).
					Dur("expirationDuration", w.config.HeartBeat.ExpirationDuration).
					Msg("HeartBeat sent")
				w.logger.Debug().
					Time("lastHeartBeatAt", updatedWorker.Status.LastHeartBeatAt).
					Dur("expirationDuration", w.config.HeartBeat.ExpirationDuration).
					Dur("interval", w.config.HeartBeat.Interval).
					Msg("HeartBeat will be sent after duration")
			}

			select {
			case <-w.heartbeatCtx.Done():
				w.logger.Debug().Msg("Stopping to send heartbeat to queue backend")
				break LOOP
			case <-time.After(w.config.HeartBeat.Interval):
			}
		}
		close(w.heatBeatStopped)
	}()
}

func (w *Worker) salvageLostWorkers() {
	if w.config.NumWorkerSalvageOnStartup == 0 {
		w.logger.Warn().
			Int("NumWorkerSalvageOnStartup", w.config.NumWorkerSalvageOnStartup).
			Msg("Skip salvaging lost workers")
	}
L:
	for i := w.config.NumWorkerSalvageOnStartup; i != 0; i-- {
		targetWorkers, err := w.backend.GetWorkersToSalvage(w.ctx, w.queueUID)
		if err != nil {
			w.logger.Warn().Err(err).Msg("Failed to get salvage target workers. Retrying")
		}
		if len(targetWorkers) == 0 {
			if i != w.config.NumWorkerSalvageOnStartup { // no log when no targets in the first loop
				w.logger.Info().Msg("No more lost workers to salvage")
			}
			break
		}
		w.logger.Info().Msg(fmt.Sprintf("%d lost workers to salvage found", len(targetWorkers)))

		targetWorker := targetWorkers[rand.Intn(len(targetWorkers))]
		w.logger.Info().Str("salvageTargetUID", targetWorker.UID.String()).Msg("Salvaging a lost worker")

		salvaged, salvagedTasks, err := w.backend.SalvageWorker(w.ctx, w.queueUID, w.uid, targetWorker.UID)
		if err != nil {
			w.logger.Warn().Err(err).Msg("Failed to salvage a worker. Retrying")
			continue
		}
		w.logger.Info().
			Str("salvageTargetUID", salvaged.UID.String()).
			Int("numTasksSalvaged", len(salvagedTasks)).
			Msg("Salvaged a lost worker successfully")

		select {
		case <-w.ctx.Done():
			w.logger.Info().Msg("Stopping to salvage workers")
			break L
		case <-time.After(1 * time.Second):
		}
	}
}

func (w *Worker) init() error {
	w.logger.Debug().Msg("Resolving queueUID")
	queue, err := w.backend.GetQueueByName(w.ctx, w.config.QueueName)
	if err != nil {
		w.logger.Error().Err(err).Msg("Can't get queue UID. abort")
		return err
	}

	w.queueUID = queue.UID
	w.logger = w.logger.With().
		Str("queueUID", queue.UID.String()).Logger()

	w.logger.Debug().Msg("Registering worker")
	worker, err := w.backend.RegisterWorker(w.ctx, queue.UID, w.config.WorkerSpec)
	if err != nil {
		w.logger.Error().Err(err).Msg("Can't register worker. abort")
		return err
	}
	w.uid = worker.UID
	w.logger = w.logger.With().Str("workerUID", worker.UID.String()).Logger()

	return nil
}
