/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package openwhisk

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func (ap *ActionProxy) cleanHandler(w http.ResponseWriter, r *http.Request) {

	// Remove action codebase
	err := ap.removeAction()
	if err != nil {
		msg := "cannot remove action: " + err.Error()
        sendError(w, http.StatusBadRequest, msg)
		log.Println(msg)
		return
    }

	// Unset user env
	ap.UnsetEnv()

	// Unset current executor
	ap.theExecutor = nil

	// Unset current directory index
	ap.currentDir = highestDir(ap.baseDir)

	ap.initialized = false
	sendOK(w)
}

// Remove action codebase
func (ap *ActionProxy) removeAction() error {
    d, err := os.Open(ap.baseDir)
    if err != nil {
        return err
    }
    defer d.Close()
    names, err := d.Readdirnames(-1)
    if err != nil {
        return err
    }
    for _, name := range names {
        err = os.RemoveAll(filepath.Join(ap.baseDir, name))
        if err != nil {
            return err
        }
    }
    return nil
}
