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

package taskqueue

import (
	"encoding/json"

	"github.com/pkg/errors"
)

var _ error = TaskToDeadletterError{}

type TaskToDeadletterError struct {
	Body string `jsong:"body"`
	Err  string `json:"error"`
}

func NewTaskToDeadletterError(body string, err error) TaskToDeadletterError {
	return TaskToDeadletterError{
		Body: body,
		Err:  err.Error(),
	}
}
func (e TaskToDeadletterError) DLItem() string {
	raw, err := json.Marshal(&e)
	if err != nil {
		return errors.Wrapf(err, "can't marshal TaskToDeadletterError: %#v", e).Error()
	}
	return string(raw)
}

func (e TaskToDeadletterError) Error() string {
	return e.Err
}
