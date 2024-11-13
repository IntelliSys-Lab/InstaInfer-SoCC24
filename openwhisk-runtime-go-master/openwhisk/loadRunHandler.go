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

type requestBody struct {
	ActionName string `json:"action_name"`
	// Include any other fields you need
}

type Data struct {
	Content string `json:"content"`
}

func (ap *ActionProxy) loadRunHandler(w http.ResponseWriter, r *http.Request) {

	// parse the request
	body, err := ioutil.ReadAll(r.Body)

	r.Body = ioutil.NopCloser(bytes.NewBuffer(body))

	defer r.Body.Close()
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error reading request body: %v", err))
		return
	}

	//get the action Name
	var req requestBody
	err = json.Unmarshal(body, &req)
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Error reading request body: %v", err))
		return
	}
	actionName := req.ActionName

	if strings.Contains(actionName, "ptest04") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.theresnet18Executor == nil || ap.theresnet18Executor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWresnet18Executor1 := Newresnet18Executor(ap.outFile, ap.errFile, "_test/loadres18.sh", ap.env)
			ap.theresnet18Executor = NEWresnet18Executor1
			ap.runHandler(w, r)
			return
		}
		if ap.theresnet18Executor.started == false {
			Debug("Haven't pre-loaded resnet18")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("resnet18")

		// remove newlines
		Debug("Served By LoadRunHandler18")
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.theresnet18Executor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! RES18 Command exited！！")
			Debug(err.Error())
			ap.theresnet18Executor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("resnet18 command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else if strings.Contains(actionName, "ptest05") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.theresnet50Executor == nil || ap.theresnet50Executor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWresnet50Executor1 := Newresnet50Executor(ap.outFile, ap.errFile, "_test/loadres50.sh", ap.env)
			ap.theresnet50Executor = NEWresnet50Executor1
			ap.runHandler(w, r)
			return
		}
		if ap.theresnet50Executor.started == false {
			Debug("Haven't pre-loaded resnet50")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("resnet50")

		Debug("Served By LoadRunHandler50")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.theresnet50Executor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING!!! Command exited")
			Debug(err.Error())
			//ap.theresnet50Executor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("resnet50 command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(string(response)))
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
	} else if strings.Contains(actionName, "ptest06") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.theresnet152Executor == nil || ap.theresnet152Executor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWresnet152Executor1 := Newresnet152Executor(ap.outFile, ap.errFile, "_test/loadres152.sh", ap.env)
			ap.theresnet152Executor = NEWresnet152Executor1
			ap.runHandler(w, r)
			return
		}
		if ap.theresnet152Executor.started == false {
			Debug("Haven't pre-loaded resnet152")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("resnet152")

		Debug("Served By LoadRunHandler152")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.theresnet152Executor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited?")
			Debug(err.Error())
			ap.theresnet152Executor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("resnet152 command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else if strings.Contains(actionName, "ptest01") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.thealexExecutor == nil || ap.thealexExecutor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWalexExecutor1 := NewalexExecutor(ap.outFile, ap.errFile, "_test/loadalex.sh", ap.env)
			ap.thealexExecutor = NEWalexExecutor1
			ap.runHandler(w, r)
			return
		}
		if ap.thealexExecutor.started == false {
			Debug("Haven't pre-loaded alex")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("alex")

		Debug("Served By LoadRunHandlerXX")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.thealexExecutor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited?")
			Debug(err.Error())
			ap.thealexExecutor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("alex command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else if strings.Contains(actionName, "ptest02") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.thevggExecutor == nil || ap.thevggExecutor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWvggExecutor1 := NewvggExecutor(ap.outFile, ap.errFile, "_test/loadvgg.sh", ap.env)
			ap.thevggExecutor = NEWvggExecutor1
			ap.runHandler(w, r)
			return
		}
		if ap.thevggExecutor.started == false {
			Debug("Haven't pre-loaded vgg")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("vgg")

		Debug("Served By LoadRunHandlerXX")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.thevggExecutor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited?")
			Debug(err.Error())
			ap.thevggExecutor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("vgg command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else if strings.Contains(actionName, "ptest03") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.theinceptionExecutor == nil || ap.theinceptionExecutor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWinceptionExecutor1 := NewinceptionExecutor(ap.outFile, ap.errFile, "_test/loadinception.sh", ap.env)
			ap.theinceptionExecutor = NEWinceptionExecutor1
			ap.runHandler(w, r)
			return
		}
		if ap.theinceptionExecutor.started == false {
			Debug("Haven't pre-loaded inception")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("inception")

		Debug("Served By LoadRunHandlerXX")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.theinceptionExecutor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited?")
			Debug(err.Error())
			ap.theinceptionExecutor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("inception command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else if strings.Contains(actionName, "ptest07") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.thegooglenetExecutor == nil || ap.thegooglenetExecutor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWgooglenetExecutor1 := NewgooglenetExecutor(ap.outFile, ap.errFile, "_test/loadgooglenet.sh", ap.env)
			ap.thegooglenetExecutor = NEWgooglenetExecutor1
			ap.runHandler(w, r)
			return
		}
		if ap.thegooglenetExecutor.started == false {
			Debug("Haven't pre-loaded googlenet")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("googlenet")

		Debug("Served By LoadRunHandlerXX")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.thegooglenetExecutor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited?")
			Debug(err.Error())
			ap.thegooglenetExecutor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("googlenet command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else if strings.Contains(actionName, "ptest08") {
		// actionName contains "ptest"
		Debug("LoadRunHandler done reading %d bytes", len(body))

		// check if you have an action
		if ap.thebertExecutor == nil || ap.thebertExecutor.cmd == nil {
			//sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet (new)"))
			NEWbertExecutor1 := NewbertExecutor(ap.outFile, ap.errFile, "_test/loadbert.sh", ap.env)
			ap.thebertExecutor = NEWbertExecutor1
			ap.runHandler(w, r)
			return
		}
		if ap.thebertExecutor.started == false {
			Debug("Haven't pre-loaded bert")
			ap.runHandler(w, r)
			return
		}

		ap.StopAllExecutorsExcept("bert")

		Debug("Served By LoadRunHandlerXX")

		// remove newlines
		body = bytes.Replace(body, []byte("\n"), []byte(""), -1)

		// execute the action
		response, err := ap.thebertExecutor.Interact(body)

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited?")
			Debug(err.Error())
			ap.thebertExecutor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("bert command exited"))
			sendError(w, http.StatusBadRequest, fmt.Sprintf(err.Error()))
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
	} else {
		ap.runHandler(w, r)
	}
}
