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

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// printDefaultConfigCmd represents the printDefaultConfig command
var printDefaultConfigCmd = &cobra.Command{
	Use:   "print-default-config",
	Short: "Print default config",
	Long:  `Print default config. The command generate config schema which can use with '--config' option.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		bytes, _ := yaml.Marshal(&defaultCmdOpts)
		fmt.Print(string(bytes))
	},
}

func init() {
	rootCmd.AddCommand(printDefaultConfigCmd)
}
