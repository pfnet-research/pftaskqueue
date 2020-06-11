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
	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"

	"github.com/spf13/cobra"
)

// suspendQueueStateCmd represents the suspend-queue command
var suspendQueueStateCmd = &cobra.Command{
	Use:   "suspend-queue [queue]",
	Short: "Suspend Queue",
	Long:  `Suspend queue.  Suspending queue sets queue state suepend.  It cuases all the workers processing the queue will stop.`,
	PreRun: func(cmd *cobra.Command, args []string) {
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()
	},
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queueName := args[0]
		queueSpec := taskqueue.NewTaskQueueSpec(queueName, taskqueue.TaskQueueStateSuspend)
		queue, err := queueBackend.UpdateQueue(cmdContext, queueSpec)
		if err != nil {
			logger.Fatal().Err(err).Str("queueName", queueName).Msg("Failed to set queue state")
		}

		logger.Info().
			Str("queueName", queueSpec.Name).
			Str("queueState", string(queueSpec.State)).
			Str("queueUID", queue.UID.String()).
			Msg("Queue suspended successfully")
	},
}

func init() {
	rootCmd.AddCommand(suspendQueueStateCmd)
}
