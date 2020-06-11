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
	"github.com/spf13/cobra"
)

// deleteQueueCmd represents the deleteQueue command
var deleteQueueCmd = &cobra.Command{
	Use:   "delete-queue [queue]",
	Short: "Delete Queue",
	Long:  `Delete queue`,
	PreRun: func(cmd *cobra.Command, args []string) {
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()
	},
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queueName := args[0]
		if err := queueBackend.DeleteQueue(cmdContext, queueName); err != nil {
			logger.Fatal().Err(err).Str("queueName", queueName).Msg("Failed to delete queue")
		}

		logger.Info().Str("queueName", queueName).Msg("Queue deleted successfully")
	},
}

func init() {
	rootCmd.AddCommand(deleteQueueCmd)
}
