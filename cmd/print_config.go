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

/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// printConfigCmd represents the print-config command
var printConfigCmd = &cobra.Command{
	Use:   "print-config",
	Short: "Print effective config",
	Long: `Print effective configuration in YAML format.
You can confirm the effective configuration in this command even if you configured pftaskqueue by multiple means(cli flags, env vars, config file).`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		bytes, _ := yaml.Marshal(&cmdOpts)
		fmt.Print(string(bytes))
	},
}

func init() {
	rootCmd.AddCommand(printConfigCmd)
}
