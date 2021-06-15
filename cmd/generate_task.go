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
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/pfnet-research/pftaskqueue/pkg/util"
	"gopkg.in/yaml.v2"

	"github.com/spf13/cobra"
)

// generateTaskCmd represents the generate-task command
var generateTaskCmd = &cobra.Command{
	Use:   "generate-task",
	Short: "Generate Task Yamls From Payloads Text File",
	Long: `This command generates task yamls from payloads text file.  Task yamls are output into its stdout. Payloads file are expected to be a text file and "one payload per line". All the generated tasks is unnamed.  If you need to name each task, prepare yaml file by yourself and feed it to add-task command.

Assume you have payloads:
$ cat payload
param-1
param-2

Then, you can use the command:
$ pftaskqueue generate-task --retry-limit 3 --timeout 10m --file payloads
retryLimit: 3
payload: payload-1
timeoutSeconds: 600
---
retryLimit: 3
payload: payload-1
timeoutSeconds: 600

This command would compose well with add-task command as below:
$ pftaskqueue generate-task --retry-limit 3 --timeout 10m --file payload | pftaskqueueu add-task [queue-name]

'--file -' consumes stdin instead of file:
$ cat << EOT | pftaskqueue generate-task --retry-limit 3 --timeout 10m --file -
payload-1
payload-2
EOT
`,
	Args: cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		var payloads *bufio.Scanner
		f, _ := cmd.Flags().GetString("file")
		log := logger
		if f == "-" {
			log = log.With().Str("file", "stdin").Logger()
			payloads = bufio.NewScanner(os.Stdin)
		} else {
			resolvedPath, err := util.ResolveRealAbsPath(f)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to resolve real path")
			}
			if util.IsDir(resolvedPath) {
				log.Fatal().Err(err).Msg("Must be a file")
			}
			file, err := os.Open(resolvedPath)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to open file")
			}
			log = log.With().Str("file", f).Str("resolvedPath", resolvedPath).Logger()
			payloads = bufio.NewScanner(file)
		}

		retryLimit, _ := cmd.Flags().GetInt("retry-limit")
		timeout, _ := cmd.Flags().GetDuration("timeout")
		log = log.With().Int("retry-limit", retryLimit).Dur("timeout", timeout).Logger()

		count := int64(1)
		for payloads.Scan() {
			log = log.With().Int64("line", count).Logger()
			taskSpec := &task.TaskSpec{
				RetryLimit:     retryLimit,
				TimeoutSeconds: int(timeout.Seconds()),
				Payload:        payloads.Text(),
			}
			bytes, err := yaml.Marshal(taskSpec)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to marshal taskspec to a yaml")
			}
			log.Debug().Msg("payload loaded")

			fmt.Printf("---\n%s", string(bytes))

			count = count + 1
		}
		if payloads.Err() != nil {
			logger.Fatal().Err(payloads.Err()).Msg("Error in reading payloads text file")
		}
	},
}

func init() {
	rootCmd.AddCommand(generateTaskCmd)
	generateTaskCmd.Flags().StringP("file", "f", "-", "path to read payload text file. if set '-', it reads form stdin.")
	generateTaskCmd.Flags().Int("retry-limit", 3, "retry limit to set tasks to be generated")
	generateTaskCmd.Flags().Duration("timeout", 1*time.Minute, "timeout to set tasks to be generated")
}
