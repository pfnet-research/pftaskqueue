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

package cmd

import (
	"strconv"

	workerconfig "github.com/pfnet-research/pftaskqueue/pkg/worker/config"

	"github.com/go-playground/validator/v10"
	"github.com/hashicorp/go-multierror"
	"github.com/pfnet-research/pftaskqueue/pkg/worker"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	queueWorker *worker.Worker
)

// startWorkerCmd represents the startWorker command
var startWorkerCmd = &cobra.Command{
	Use:   "start-worker [task handler commands]",
	Short: "Start TaskQueue Worker",
	Long:  `Start TaskQueue Worker.  This worker never stop until queue is suspended.`,
	Args:  cobra.ArbitraryArgs,
	PreRun: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			cmdOpts.Worker.TaskHandler.Commands = args
		}
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()

		var err error
		err = workerconfig.Validator.Struct(&cmdOpts.Worker)
		if err != nil {
			var verrs *multierror.Error
			for _, err := range err.(validator.ValidationErrors) {
				fieldName := err.Field()
				switch fieldName {
				case "Name":
					verrs = multierror.Append(verrs, errors.New("worker-name must not be empty"))
				case "QueueName":
					verrs = multierror.Append(verrs, errors.New("worker-queue-name can't contain ':'"))
				case "Concurrency":
					verrs = multierror.Append(verrs, errors.New("worker-concurrency must be positive integer"))
				case "WorkDir":
					verrs = multierror.Append(verrs, errors.New("worker work dir must be existed and writable"))
				default:
					verrs = multierror.Append(verrs, errors.New("unknown validation error"))
				}
			}
			logger.Fatal().Errs("errors", verrs.Errors).Msg("Validation failed")
		}

		if queueWorker, err = worker.NewWorker(cmdContext, logger, queueBackend, cmdOpts.Worker); err != nil {
			logger.Fatal().Err(err).Msg("Can't initialize worker")
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := queueWorker.Start(); err != nil {
			logger.Fatal().Err(err).Msg("Worker failed")
		}
	},
}

func init() {
	rootCmd.AddCommand(startWorkerCmd)

	flag := startWorkerCmd.Flags()

	flag.String("name", cmdOpts.Worker.Name, "worker name to record")
	viperBindPFlag("Worker.Name", cmdOpts.Worker.Name, flag.Lookup("name"))

	flag.String("queue-name", cmdOpts.Worker.QueueName, "queue name to process by the worker")
	viperBindPFlag("Worker.QueueName", cmdOpts.Worker.QueueName, flag.Lookup("queue-name"))

	flag.Int("concurrency", cmdOpts.Worker.Concurrency, "concurrency for processing tasks")
	viperBindPFlag("Worker.Concurrency", strconv.Itoa(cmdOpts.Worker.Concurrency), flag.Lookup("concurrency"))

	flag.Duration("default-command-timeout", cmdOpts.Worker.TaskHandler.DefaultCommandTimeout, "default timeout for executing command for tasks. the value will be used when the taskspec has no timeout spec")
	viperBindPFlag("Worker.TaskHandler.DefaultCommandTimeout", cmdOpts.Worker.TaskHandler.DefaultCommandTimeout.String(), flag.Lookup("default-command-timeout"))

	flag.Bool("exit-on-suspend", cmdOpts.Worker.ExitOnSuspend, "if set, worker exits when queue is suspended")
	viperBindPFlag("Worker.ExitOnSuspend", strconv.FormatBool(cmdOpts.Worker.ExitOnSuspend), flag.Lookup("exit-on-suspend"))

	flag.Bool("exit-on-empty", cmdOpts.Worker.ExitOnEmpty, "if set, worker exits when queue is empty")
	viperBindPFlag("Worker.ExitOnEmpty", strconv.FormatBool(cmdOpts.Worker.ExitOnEmpty), flag.Lookup("exit-on-empty"))

	flag.Int("num-tasks", cmdOpts.Worker.NumTasks, "if set positive value, worker exits after processing the number of tasks")
	viperBindPFlag("Worker.NumTasks", strconv.Itoa(cmdOpts.Worker.NumTasks), flag.Lookup("num-tasks"))

	flag.String("work-dir", cmdOpts.Worker.WorkDir, "worker's working directory.  the dir must be writable")
	viperBindPFlag("Worker.WorkDir", cmdOpts.Worker.WorkDir, flag.Lookup("work-dir"))

	flag.Int("num-worker-salvage-on-startup", cmdOpts.Worker.NumWorkerSalvageOnStartup, "The number of worker to salvage on worker startup. -1 means all")
	viperBindPFlag("Worker.NumWorkerSalvageOnStartup", strconv.Itoa(cmdOpts.Worker.NumWorkerSalvageOnStartup), flag.Lookup("num-worker-salvage-on-startup"))

	flag.Duration("heartbeat-interval", cmdOpts.Worker.HeartBeat.Interval, "heart beat interval")
	viperBindPFlag("Worker.HeartBeat.Interval", cmdOpts.Worker.HeartBeat.Interval.String(), flag.Lookup("heartbeat-interval"))

	flag.Duration("heartbeat-expiration-duration", cmdOpts.Worker.HeartBeat.ExpirationDuration, "heart beat expiration duration")
	viperBindPFlag("Worker.HeartBeat.ExpirationDuration", cmdOpts.Worker.HeartBeat.ExpirationDuration.String(), flag.Lookup("heartbeat-expiration-duration"))

	flag.Duration("heartbeat-salvage-duration", cmdOpts.Worker.HeartBeat.SalvageDuration, "The duration which worker become salvage target after heart beat expiration")
	viperBindPFlag("Worker.HeartBeat.SalvageDuration", cmdOpts.Worker.HeartBeat.SalvageDuration.String(), flag.Lookup("heartbeat-salvage-duration"))

}
