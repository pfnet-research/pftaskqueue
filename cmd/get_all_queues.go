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
	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
)

// getAllQueuesCmd represents the get-all-queues command
var getAllQueuesCmd = &cobra.Command{
	Use:   "get-all-queues",
	Short: "Get All Queues",
	Long:  `Get All Queues`,
	PreRun: func(cmd *cobra.Command, args []string) {
		mustValidateOutputFlag(cmd.Flags())
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()
	},
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		queues, err := queueBackend.GetAllQueues(cmdContext)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to get queues")
		}
		output(cmd.Flags(), outputFunc{
			yamlFunc: func() interface{} { return queues },
			jsonFunc: func() interface{} { return queues },
			tableFunc: func() (header []string, rows [][]string) {
				header = []string{"Name", "State", "Age"}
				for _, q := range queues {
					rows = append(rows, []string{
						q.Spec.Name, string(q.Spec.State), humanize.Time(q.Status.CreatedAt),
					})
				}
				return
			},
		})
	},
}

func init() {
	rootCmd.AddCommand(getAllQueuesCmd)
	installOutputFlag(getAllQueuesCmd.Flags())
}
