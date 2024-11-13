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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// ErrResponse is the response when there are errors
type ErrResponse struct {
	Error string `json:"error"`
}

func sendError(w http.ResponseWriter, code int, cause string) {
	errResponse := ErrResponse{Error: cause}
	b, err := json.Marshal(errResponse)
	if err != nil {
		b = []byte("error marshalling error response")
		Debug(err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(b)
	w.Write([]byte("\n"))
}

func (ap *ActionProxy) runHandler(w http.ResponseWriter, r *http.Request) {

	// parse the request
	body, err1 := ioutil.ReadAll(r.Body)

	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	defer r.Body.Close()
	if err1 != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error reading request body: %v", err1))
		return
	}
	Debug("runHandler done reading %d bytes", len(body))

	// remove newlines
	body = bytes.Replace(body, []byte("\n"), []byte(""), -1)
	var response []byte
	var err error

	//The original design of the proxy was intended for use with a single action.
	//To support multiple actions, we refresh the executor after completing
	//the inference action and then execute the next task.
	var req requestBody
	err = json.Unmarshal(body, &req)
	if err != nil {
		Debug("Meet error:")
		Debug(err.Error())
		return
	}
	actionName := req.ActionName
	if strings.Contains(actionName, "ptest04") && (ap.theresnet18Executor.started == false) {
		//NEWresnet18Executor1 := Newresnet18Executor(ap.outFile, ap.errFile, "_test/loadres18.sh", ap.env)
		//ap.theresnet18Executor = NEWresnet18Executor1
		Debug("has created res18executor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOriginresnet18Executor.StartAndWaitForOutput()

		//create new executor
		NEWOriginresnet18Executor := NewOriginresnet18Executor(ap.outFile, ap.errFile, "_test/func18.sh", ap.env)
		ap.theOriginresnet18Executor = NEWOriginresnet18Executor

	} else if strings.Contains(actionName, "ptest05") && (ap.theresnet50Executor.started == false) {
		//NEWresnet50Executor1 := Newresnet50Executor(ap.outFile, ap.errFile, "_test/loadres50.sh", ap.env)
		//ap.theresnet50Executor = NEWresnet50Executor1
		Debug("has created res50executor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOriginresnet50Executor.StartAndWaitForOutput()

		//create new executor
		NEWOriginresnet50Executor := NewOriginresnet50Executor(ap.outFile, ap.errFile, "_test/func50.sh", ap.env)
		ap.theOriginresnet50Executor = NEWOriginresnet50Executor

	} else if strings.Contains(actionName, "ptest06") && (ap.theresnet152Executor.started == false) {
		//NEWresnet152Executor1 := Newresnet152Executor(ap.outFile, ap.errFile, "_test/loadres152.sh", ap.env)
		//ap.theresnet152Executor = NEWresnet152Executor1
		Debug("has created res152executor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOriginresnet152Executor.StartAndWaitForOutput()

		//create new executor
		NEWOriginresnet152Executor := NewOriginresnet152Executor(ap.outFile, ap.errFile, "_test/func152.sh", ap.env)
		ap.theOriginresnet152Executor = NEWOriginresnet152Executor

	} else if strings.Contains(actionName, "ptest01") && (ap.thealexExecutor.started == false) {
		Debug("has created alexexecutor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOriginalexExecutor.StartAndWaitForOutput()
		//create new executor
		NEWOriginalexExecutor := NewOriginalexExecutor(ap.outFile, ap.errFile, "_test/funcalex.sh", ap.env)
		ap.theOriginalexExecutor = NEWOriginalexExecutor
	} else if strings.Contains(actionName, "ptest02") && (ap.thevggExecutor.started == false) {
		Debug("has created vggexecutor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOriginvggExecutor.StartAndWaitForOutput()
		//create new executor
		NEWOriginvggExecutor := NewOriginvggExecutor(ap.outFile, ap.errFile, "_test/funcvgg.sh", ap.env)
		ap.theOriginvggExecutor = NEWOriginvggExecutor
	} else if strings.Contains(actionName, "ptest03") && (ap.theinceptionExecutor.started == false) {
		Debug("has created inceptionexecutor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOrigininceptionExecutor.StartAndWaitForOutput()
		//create new executor
		NEWOrigininceptionExecutor := NewOrigininceptionExecutor(ap.outFile, ap.errFile, "_test/funcinception.sh", ap.env)
		ap.theOrigininceptionExecutor = NEWOrigininceptionExecutor
	} else if strings.Contains(actionName, "ptest07") && (ap.thegooglenetExecutor.started == false) {
		Debug("has created googlenetexecutor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOrigingooglenetExecutor.StartAndWaitForOutput()
		//create new executor
		NEWOrigingooglenetExecutor := NewOrigingooglenetExecutor(ap.outFile, ap.errFile, "_test/funcgooglenet.sh", ap.env)
		ap.theOrigingooglenetExecutor = NEWOrigingooglenetExecutor
	} else if strings.Contains(actionName, "ptest08") && (ap.thebertExecutor.started == false) {
		Debug("has created bertexecutor")
		ap.StopAllExecutorsExcept("none")
		response, err = ap.theOriginbertExecutor.StartAndWaitForOutput()
		//create new executor
		NEWOriginbertExecutor := NewOriginbertExecutor(ap.outFile, ap.errFile, "_test/funcbert.sh", ap.env)
		ap.theOriginbertExecutor = NEWOriginbertExecutor
	} else {
		// check if you have an action
		if ap.theExecutor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		// check if the process exited
		if ap.theExecutor.Exited() {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("command exited"))
			return
		}

		response, err = ap.theExecutor.Interact(body)
	}

	// execute the action

	// check for early termination
	if err != nil {
		Debug("WARNING! Command exited (runHandler). Error is: ")
		Debug(string(err.Error()))
		ap.theExecutor = nil
		sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited"))
		return
	}
	DebugLimit("received:", response, 120)

	// check if the answer is an object map
	var objmap map[string]*json.RawMessage
	resStr := strings.ReplaceAll(string(response), "'", "\"")
	response = []byte(resStr)
	err = json.Unmarshal(response, &objmap)
	if err != nil {
		sendError(w, http.StatusBadGateway, "The action did not return a dictionary.")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(response)))
	numBytesWritten, err := w.Write(response)

	// flush output
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// diagnostic when you have writing problems
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Error writing response: %v", err))
		return
	}
	if numBytesWritten != len(response) {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Only wrote %d of %d bytes to response", numBytesWritten, len(response)))
		return
	}
}
