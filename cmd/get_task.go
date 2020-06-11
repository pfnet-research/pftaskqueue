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

	"github.com/dustin/go-humanize"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var (
	possibleTaskState = []string{
		"all",
		"pending",
		"completed",
		"succeeded",
		"failed",
		"deadletter",
	}
	wrongTaskStateError error = errors.Errorf("state must be one of %s", strings.Join(possibleTaskState, ","))
)

// getTaskCmd represents the getTask command
var getTaskCmd = &cobra.Command{
	Use:   "get-task [queue]",
	Short: "Get Tasks",
	Long:  `Get tasks from the queue`,
	Args:  cobra.ExactArgs(1),
	PreRun: func(cmd *cobra.Command, args []string) {
		mustValidateOutputFlag(cmd.Flags())
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()
		state, _ := cmd.Flags().GetString("state")
		for _, s := range possibleTaskState {
			if state == s {
				return
			}
		}
		logger.Fatal().Err(wrongTaskStateError).Str("state", state).Msg("Validation error")
	},
	Run: func(cmd *cobra.Command, args []string) {
		queueName := args[0]
		state, _ := cmd.Flags().GetString("state")

		var ts []*task.Task
		var err error

		switch state {
		case "all":
			ts, err = queueBackend.GetAllTasks(cmdContext, queueName)
		case "pending":
			ts, err = queueBackend.GetPendingTasks(cmdContext, queueName)
		case "completed":
			ts, err = queueBackend.GetCompletedTasks(cmdContext, queueName)
		case "succeeded":
			ts, err = queueBackend.GetSucceededTasks(cmdContext, queueName)
		case "failed":
			ts, err = queueBackend.GetFailedTasks(cmdContext, queueName)
		case "deadletter":
			dl, err := queueBackend.GetDeadLetter(cmdContext, queueName)
			if err != nil {
				logger.Fatal().Err(err).Str("queueName", queueName).Msg("Failed to get tasks")
			}
			output(cmd.Flags(), outputFunc{
				yamlFunc: func() interface{} {
					return dl
				},
				jsonFunc: func() interface{} {
					return dl
				},
				tableFunc: func() (header []string, rows [][]string) {
					header = []string{"Body", "Error"}
					for _, d := range dl {
						rows = append(rows, []string{d.Body, d.Err})
					}
					return
				},
			})
			return
		default:
			logger.Fatal().Err(wrongTaskStateError).Str("state", state).Msg("Internal error")
		}

		if err != nil {
			logger.Fatal().Err(err).Str("queueName", queueName).Msg("Failed to get tasks")
		}
		output(cmd.Flags(), outputFunc{
			yamlFunc: func() interface{} {
				return ts
			},
			jsonFunc: func() interface{} {
				return ts
			},
			tableFunc: func() (header []string, rows [][]string) {
				header = []string{"Name", "Phase", "WorkerName", "FailureCount", "RetryLimit", "Age", "UID"}
				for _, t := range ts {
					workerName := ""
					if t.Status.CurrentRecord != nil {
						workerName = t.Status.CurrentRecord.WorkerName
					} else if t.Status.Phase != task.TaskPhasePending && len(t.Status.History) > 0 {
						workerName = t.Status.History[len(t.Status.History)-1].WorkerName
					}
					rows = append(rows, []string{
						t.Spec.Name, string(t.Status.Phase), workerName,
						strconv.Itoa(t.Status.FailureCount), strconv.Itoa(t.Spec.RetryLimit), humanize.Time(t.Status.CreatedAt),
						t.UID,
					})
				}
				return
			},
		})
	},
}

func init() {
	rootCmd.AddCommand(getTaskCmd)
	getTaskCmd.Flags().String(
		"state",
		"all",
		fmt.Sprintf("state filtering tasks. one of %s", strings.Join(possibleTaskState, ",")),
	)
	installOutputFlag(getTaskCmd.Flags())
}
