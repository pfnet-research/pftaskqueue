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
	"encoding/json"
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v2"
)

type outputFunc struct {
	yamlFunc  func() interface{}
	jsonFunc  func() interface{}
	tableFunc func() ([]string, [][]string)
}

func output(fs *pflag.FlagSet, f outputFunc) {
	outputFlag, err := fs.GetString("output")
	if err != nil {
		logger.Fatal().Err(err).Msg("Internal Error")
	}
	switch outputFlag {
	case "yaml":
		bytes, _ := yaml.Marshal(f.yamlFunc())
		fmt.Print(string(bytes))
	case "json":
		bytes, _ := json.MarshalIndent(f.jsonFunc(), "", "\t")
		fmt.Print(string(bytes))
	case "table":
		header, rows := f.tableFunc()
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetCenterSeparator("")
		table.SetColumnSeparator("")
		table.SetRowSeparator("")
		table.SetHeaderLine(false)
		table.SetBorder(false)
		table.SetTablePadding("\t") // pad with tabs
		table.SetNoWhiteSpace(true)
		table.SetHeader(header)
		table.AppendBulk(rows)
		table.Render()
	default:
		logger.Fatal().Str("output", outputFlag).Msg("Unsupported output. It must be one of [yaml, json, table]")
	}
}

func installOutputFlag(fs *pflag.FlagSet) {
	fs.StringP("output", "o", "table", "output format. one of [yaml, json, table]")
}

func mustValidateOutputFlag(fs *pflag.FlagSet) {
	outputFlag, err := fs.GetString("output")
	if err != nil {
		logger.Fatal().Err(err).Msg("Internal Error")
	}

	for _, o := range []string{"yaml", "json", "table"} {
		if outputFlag == o {
			return
		}
	}
	logger.Fatal().Str("output", outputFlag).Msg("Unsupported output. It must be one of [yaml, json, table]")
}
