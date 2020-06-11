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

package util

func StringP(str string) *string {
	return &str
}

func Truncate(str *string, maxLength int) *string {
	if str == nil {
		return str
	}
	if len(*str) <= maxLength {
		return str
	}
	if maxLength == 0 {
		return StringP("")
	}
	if maxLength <= 5 {
		return StringP((*str)[0:maxLength])
	}
	length := len(*str)
	half := maxLength / 2
	return StringP((*str)[0:half-1] + ".." + (*str)[length-(half-1):length])
}
