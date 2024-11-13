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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func (ap *ActionProxy) offloadHandler(w http.ResponseWriter, r *http.Request) {

	// parse the request
	body, err := ioutil.ReadAll(r.Body)
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
		// check if you have an action
		if ap.theresnet18Executor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.theresnet18Executor.started == false {
			return
		}
		if ap.theresnet18Executor.started == true {
			Debug("received a offload signal, now stopping resnet18")
			ap.theresnet18Executor.Stop()
		}

	} else if strings.Contains(actionName, "ptest05") {
		// check if you have an action
		if ap.theresnet50Executor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.theresnet50Executor.started == false {
			return
		}
		if ap.theresnet50Executor.started == true {
			Debug("received a offload signal, now stopping resnet50")
			ap.theresnet50Executor.Stop()
		}
	} else if strings.Contains(actionName, "ptest06") {
		// check if you have an action
		if ap.theresnet152Executor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.theresnet152Executor.started == false {
			return
		}
		if ap.theresnet152Executor.started == true {
			Debug("received a offload signal, now stopping resnet152")
			ap.theresnet152Executor.Stop()
		}
	} else if strings.Contains(actionName, "ptest01") {
		// check if you have an action
		if ap.thealexExecutor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}

		if ap.thealexExecutor.started == false {
			Debug("received a offload signal, alex has not started")
			return
		}
		if ap.thealexExecutor.started == true {
			Debug("received a offload signal, now stopping alex")
			ap.thealexExecutor.Stop()
		}
	} else if strings.Contains(actionName, "ptest02") {
		// check if you have an action
		if ap.thevggExecutor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.thevggExecutor.started == false {
			return
		}
		if ap.thevggExecutor.started == true {
			Debug("received a offload signal, now stopping vgg")
			ap.thevggExecutor.Stop()
		}
	} else if strings.Contains(actionName, "ptest03") {
		// check if you have an action
		if ap.theinceptionExecutor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.theinceptionExecutor.started == false {
			return
		}
		if ap.theinceptionExecutor.started == true {
			Debug("received a offload signal, now stopping inception")
			ap.theinceptionExecutor.Stop()
		}
	} else if strings.Contains(actionName, "ptest07") {
		// check if you have an action
		if ap.thegooglenetExecutor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.thegooglenetExecutor.started == false {
			return
		}
		if ap.thegooglenetExecutor.started == true {
			Debug("received a offload signal, now stopping googlenet")
			ap.thegooglenetExecutor.Stop()
		}
	} else if strings.Contains(actionName, "ptest08") {
		// check if you have an action
		if ap.thebertExecutor == nil {
			sendError(w, http.StatusInternalServerError, fmt.Sprintf("no action defined yet"))
			return
		}
		if ap.thebertExecutor.started == false {
			return
		}
		if ap.thebertExecutor.started == true {
			Debug("received a offload signal, now stopping bert")
			ap.thebertExecutor.Stop()
		}
	} else {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Not defined this model!"))
		return
	}
}
