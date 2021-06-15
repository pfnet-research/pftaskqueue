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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/taskqueue"

	"github.com/rs/zerolog"

	"github.com/pfnet-research/pftaskqueue/pkg/util"

	"github.com/pfnet-research/pftaskqueue/pkg/apis/task"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// addTaskCmd represents the add-tasks command
var addTaskCmd = &cobra.Command{
	Use:   "add-task [queue]",
	Short: "Add Tasks To Queue",
	Long: `Add Tasks To Queue. This commands reads YAML formatted 'TaskSpec's (not array) from Stdin.
cat << EOT | pftaskqueue add-task [queue-name]
payload: "foo"
retryLimit: 0
timeoutSeconds: 100
---
payload: "bar"
retryLimit: 0
timeoutSeconds: 100
EOT
`,
	PreRun: func(cmd *cobra.Command, args []string) {
		displayCmdOptsIfEnabled()
		mustInitializeQueueBackend()
	},
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queueName := args[0]
		queue, err := queueBackend.GetQueueByName(cmdContext, queueName)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to get queue")
		}

		f, _ := cmd.Flags().GetString("file")
		if f == "-" {
			addTaskFromReader(logger.With().Str("pipe", "stdin").Logger(), queue, os.Stdin)
			return
		}
		if !util.IsPathExists(f) {
			logger.Fatal().Str("path", f).Msg("path is not found")
		}

		resolved, err := util.ResolveRealAbsPath(f)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to resolve real path")
		}
		if !util.IsDir(resolved) {
			file, err := os.Open(resolved)
			if err != nil {
				logger.Fatal().Err(err).Str("path", resolved).Msg("Failed to open")
			}
			defer file.Close()
			addTaskFromReader(logger.With().Str("path", resolved).Logger(), queue, bufio.NewReader(file))
			return
		}

		recursive, _ := cmd.Flags().GetBool("recursive")
		if !recursive {
			files, err := ioutil.ReadDir(resolved)
			if err != nil {
				logger.Fatal().Err(err).Str("path", resolved).Msg("Failed to read directory")
			}
			for _, file := range files {
				if file.IsDir() || !(filepath.Ext(file.Name()) == ".yaml" || filepath.Ext(file.Name()) == ".yml") {
					continue
				}
				addTaskFromFile(queue, resolved, file)
			}
			return
		}
		_ = filepath.Walk(resolved, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				logger.Fatal().Err(err).Str("path", filepath.Join(path, info.Name())).Msg("Failed during traversing directory")
			}
			if info.IsDir() || !(filepath.Ext(info.Name()) == ".yaml" || filepath.Ext(info.Name()) == ".yml") {
				return nil
			}
			addTaskFromFile(queue, path, info)
			return nil
		})
	},
}

func init() {
	rootCmd.AddCommand(addTaskCmd)
	addTaskCmd.Flags().StringP("file", "f", "-", "path to read TaskSpec yaml files. It can be directory. if set '-', it reads form stdin.")
	addTaskCmd.Flags().BoolP("recursive", "R", false, "recursive read. the value is used only when path is directory.")
}

func addTaskFromFile(queue *taskqueue.TaskQueue, dirName string, fileinfo os.FileInfo) {
	path := filepath.Join(dirName, fileinfo.Name())
	logger.Debug().Str("path", path).Msg("Reading path")
	file, err := os.Open(path)
	if err != nil {
		logger.Fatal().Err(err).Str("path", dirName).Msg("Failed to open")
	}
	defer file.Close()
	addTaskFromReader(logger.With().Str("path", path).Logger(), queue, bufio.NewReader(file))
}

func addTaskFromReader(lggr zerolog.Logger, queue *taskqueue.TaskQueue, reader io.Reader) {
	decoder := yaml.NewDecoder(reader)
	decoder.SetStrict(true)
LOOP:
	for {
		var taskSpec task.TaskSpec
		err := decoder.Decode(&taskSpec)
		switch {
		case err == io.EOF:
			break LOOP
		case err != nil:
			lggr.Error().Err(err).Msg("Failed to unmarshal task spec. Skipped")
		default:
			task, err := queueBackend.AddTask(cmdContext, queue.Spec.Name, taskSpec)
			if err != nil {
				lggr.Error().Err(err).
					Str("queueName", queue.Spec.Name).
					Str("queueUID", queue.UID.String()).
					Interface("taskSpec", &taskSpec).
					Msg("Failed to add task to queue. Skipped")
			} else {
				lggr.Info().
					Str("queueName", queue.Spec.Name).
					Str("queueUID", queue.UID.String()).
					Interface("task", task).
					Msg("Task added to queue")
			}
		}
	}
}
