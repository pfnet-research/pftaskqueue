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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/worker"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	possibleWorkerState = []string{
		"all",
		"running",
		"succeeded",
		"failed",
		"lost",
		"tosalvage",
	}
	wrongWorkerStateError error = errors.Errorf("state must be one of %s", strings.Join(possibleWorkerState, ","))
)

// getWorkerCmd represents the getTask command
var getWorkerCmd = &cobra.Command{
	Use:   "get-worker [queue]",
	Short: "Get Workers",
	Long:  `Get Workers working on the queue`,
	Args:  cobra.ExactArgs(1),
	PreRun: func(cmd *cobra.Command, args []string) {
		mustValidateOutputFlag(cmd.Flags())
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()
		state, _ := cmd.Flags().GetString("state")
		for _, s := range possibleWorkerState {
			if state == s {
				return
			}
		}
		logger.Fatal().Err(wrongWorkerStateError).Str("state", state).Msg("Validation error")
	},
	Run: func(cmd *cobra.Command, args []string) {
		queueName := args[0]
		state, _ := cmd.Flags().GetString("state")

		queue, err := queueBackend.GetQueueByName(cmdContext, queueName)
		if err != nil {
			logger.Fatal().Str("queueName", queueName).Err(err).Msg("Failed to get queue")
		}

		var ws []*worker.Worker
		switch state {
		case "all":
			ws, err = queueBackend.GetAllWorkers(cmdContext, queue.UID)
		case "running":
			ws, err = queueBackend.GetRunningWorkers(cmdContext, queue.UID)
		case "succeeded":
			ws, err = queueBackend.GetSucceededWorkers(cmdContext, queue.UID)
		case "failed":
			ws, err = queueBackend.GetFailedWorkers(cmdContext, queue.UID)
		case "lost":
			ws, err = queueBackend.GetLostWorker(cmdContext, queue.UID)
		case "tosalvage":
			ws, err = queueBackend.GetWorkersToSalvage(cmdContext, queue.UID)
		default:
			logger.Fatal().Err(wrongTaskStateError).Str("state", state).Msg("Internal error")
		}

		if err != nil {
			logger.Fatal().Err(err).
				Str("queueName", queue.Spec.Name).
				Str("queueUID", queue.UID.String()).
				Msg("Failed to get workers")
		}

		output(cmd.Flags(), outputFunc{
			yamlFunc: func() interface{} {
				return ws
			},
			jsonFunc: func() interface{} {
				return ws
			},
			tableFunc: func() (header []string, rows [][]string) {
				header = []string{"UID", "Name", "Phase", "Reason", "Lost", "Salvage Target", "Age"}
				for _, w := range ws {
					rows = append(rows, []string{
						w.UID.String(), w.Spec.Name, string(w.Status.Phase), string(w.Status.Reason),
						strconv.FormatBool(w.IsLostOn(time.Now())), strconv.FormatBool(w.AllowToSalvageOn(time.Now())),
						humanize.Time(w.Status.StartedAt),
					})
				}
				return
			},
		})
	},
}

func init() {
	rootCmd.AddCommand(getWorkerCmd)
	getWorkerCmd.Flags().String(
		"state",
		"all",
		fmt.Sprintf("state filtering tasks. one of %s", strings.Join(possibleWorkerState, ",")),
	)
	installOutputFlag(getWorkerCmd.Flags())
}
