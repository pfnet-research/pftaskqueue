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
	"encoding/csv"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var bindPFlagMap = map[string]BindPFlag{}

type BindPFlag struct {
	Default interface{}
	Flag    *pflag.Flag
}

func viperBindPFlag(key string, defaultValueStr string, flg *pflag.Flag) {
	bindPFlagMap[key] = BindPFlag{
		Default: defaultValueStr,
		Flag:    flg,
	}
}

func mergePFlagToViper() {
	for k, b := range bindPFlagMap {
		sval := b.Flag.Value.String()
		if b.Flag.Value.String() != b.Default {
			var err error
			var v interface{}
			switch b.Flag.Value.Type() {
			case "bool":
				v, err = strconv.ParseBool(sval)
			case "string":
				v = sval
			case "int":
				v, err = strconv.Atoi(sval)
			case "float64":
				v, err = strconv.ParseFloat(sval, 64)
			case "duration":
				v, err = time.ParseDuration(sval)
			case "stringArray":
				sval = sval[1 : len(sval)-1]
				// An empty string would cause a array with one (empty) string
				if sval == "" {
					v = []string{}
					break
				}
				stringReader := strings.NewReader(sval)
				csvReader := csv.NewReader(stringReader)
				v, err = csvReader.Read()
			default:
				panic(errors.Errorf("unsupported type to merge pflag to viper: %s", b.Flag.Value.Type()))
			}
			if err != nil {
				panic(errors.Wrapf(err, "cant convert flag value, type=%s value=%s", b.Flag.Value.Type(), b.Flag.Value.String()))
			}
			viper.Set(k, v)
		}
	}
}

//
//func strArrrayToString(vals []string) (string, error) {
//	b := &bytes.Buffer{}
//	w := csv.NewWriter(b)
//	err := w.Write(vals)
//	if err != nil {
//		return "", err
//	}
//	w.Flush()
//	return "[" + strings.TrimSuffix(b.String(), "\n") + "]", nil
//}
