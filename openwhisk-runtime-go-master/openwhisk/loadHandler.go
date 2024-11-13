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

func (ap *ActionProxy) loadHandler(w http.ResponseWriter, r *http.Request) {

	//在最开始，先得到r.Body.ActionName，然后分析：是否有对应的modelExecutor（通过IsStarted参数）。
	//if IsStarted == false：证明是冷启动，直接调用runHandler(w,r）
	//else：证明已经有pre-load的容器了，继续往下执行。

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
		Debug("LoadHandler starts pre-loading ResNet18.")
		// check if you have an action
		if ap.theresnet18Executor == nil || ap.theresnet18Executor.cmd == nil {
			Debug("Just create a new res18executor")
			NEWresnet18Executor1 := Newresnet18Executor(ap.outFile, ap.errFile, "_test/loadres18.sh", ap.env)
			ap.theresnet18Executor = NEWresnet18Executor1
		}

		if ap.theresnet18Executor.started {
			Debug("already loaded resnet18")
			return
		}

		//Pre-load libraries & model
		err := ap.theresnet18Executor.Start(false)

		Debug("Handler Finished pre-loading ResNet18.")
		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (loadHandler)")
			//ap.theresnet18Executor = nil
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited！！"))
			Debug(err.Error())
			return
		}
	} else if strings.Contains(actionName, "ptest05") {
		Debug("LoadHandler starts pre-loading ResNet50.")
		// check if you have an action
		if ap.theresnet50Executor == nil || ap.theresnet50Executor.cmd == nil {
			Debug("Just create a new res50executor")
			NEWresnet50Executor1 := Newresnet50Executor(ap.outFile, ap.errFile, "_test/loadres50.sh", ap.env)
			ap.theresnet50Executor = NEWresnet50Executor1
		}
		if ap.theresnet50Executor.started {
			Debug("already loaded resnet50")
			return
		}

		//NEWresnet50Executor1 := Newresnet50Executor(ap.outFile, ap.errFile, "_test/loadres50.sh", ap.env)
		//ap.theresnet50Executor = NEWresnet50Executor1

		//Pre-load libraries & model
		err := ap.theresnet50Executor.Start(false)
		Debug("Handler Finished pre-loading ResNet50.")

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (load)")
			//ap.theresnet50Executor = nil
			Debug(err.Error())
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited"))
			return
		}
	} else if strings.Contains(actionName, "ptest06") {
		Debug("LoadHandler starts pre-loading ResNet152.")
		// check if you have an action
		if ap.theresnet152Executor == nil || ap.theresnet152Executor.cmd == nil {
			Debug("Just create a new res152executor")
			NEWresnet152Executor1 := Newresnet152Executor(ap.outFile, ap.errFile, "_test/loadres152.sh", ap.env)
			ap.theresnet152Executor = NEWresnet152Executor1
		}
		if ap.theresnet152Executor.started {
			Debug("already loaded resnet152")
			return
		}

		//NEWresnet152Executor1 := Newresnet152Executor(ap.outFile, ap.errFile, "_test/loadres152.sh", ap.env)
		//ap.theresnet152Executor = NEWresnet152Executor1

		//Pre-load libraries & model
		err := ap.theresnet152Executor.Start(false)
		Debug("Handler Finished pre-loading ResNet152.")

		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited(load)")
			//ap.theresnet152Executor = nil
			Debug(err.Error())
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited"))
			return
		}
	} else if strings.Contains(actionName, "ptest01") {
		Debug("LoadHandler starts pre-loading alex.")
		// check if you have an action
		if ap.thealexExecutor == nil || ap.thealexExecutor.cmd == nil {
			Debug("Just create a new alexexecutor")
			NEWalexExecutor1 := NewalexExecutor(ap.outFile, ap.errFile, "_test/loadalex.sh", ap.env)
			ap.thealexExecutor = NEWalexExecutor1
		}
		if ap.thealexExecutor.started {
			Debug("already loaded alex")
			return
		}

		//Pre-load libraries & model
		err := ap.thealexExecutor.Start(false)

		Debug("Handler Finished pre-loading alex.")
		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (loadHandler)")
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited！！"))
			Debug(err.Error())
			return
		}
	} else if strings.Contains(actionName, "ptest02") {
		Debug("LoadHandler starts pre-loading vgg.")
		// check if you have an action
		if ap.thevggExecutor == nil || ap.thevggExecutor.cmd == nil {
			Debug("Just create a new vggexecutor")
			NEWvggExecutor1 := NewvggExecutor(ap.outFile, ap.errFile, "_test/loadvgg.sh", ap.env)
			ap.thevggExecutor = NEWvggExecutor1
		}
		if ap.thevggExecutor.started {
			Debug("already loaded vgg")
			return
		}

		//Pre-load libraries & model
		err := ap.thevggExecutor.Start(false)

		Debug("Handler Finished pre-loading vgg.")
		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (loadHandler)")
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited！！"))
			Debug(err.Error())
			return
		}
	} else if strings.Contains(actionName, "ptest03") {
		Debug("LoadHandler starts pre-loading inception.")
		// check if you have an action
		if ap.theinceptionExecutor == nil || ap.theinceptionExecutor.cmd == nil {
			Debug("Just create a new inceptionexecutor")
			NEWinceptionExecutor1 := NewinceptionExecutor(ap.outFile, ap.errFile, "_test/loadinception.sh", ap.env)
			ap.theinceptionExecutor = NEWinceptionExecutor1
		}
		if ap.theinceptionExecutor.started {
			Debug("already loaded inception")
			return
		}

		//Pre-load libraries & model
		err := ap.theinceptionExecutor.Start(false)

		Debug("Handler Finished pre-loading inception.")
		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (loadHandler)")
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited！！"))
			Debug(err.Error())
			return
		}
	} else if strings.Contains(actionName, "ptest07") {
		Debug("LoadHandler starts pre-loading googlenet.")
		// check if you have an action
		if ap.thegooglenetExecutor == nil || ap.thegooglenetExecutor.cmd == nil {
			Debug("Just create a new googlenetexecutor")
			NEWgooglenetExecutor1 := NewgooglenetExecutor(ap.outFile, ap.errFile, "_test/loadgooglenet.sh", ap.env)
			ap.thegooglenetExecutor = NEWgooglenetExecutor1
		}
		if ap.thegooglenetExecutor.started {
			Debug("already loaded googlenet")
			return
		}

		//Pre-load libraries & model
		err := ap.thegooglenetExecutor.Start(false)

		Debug("Handler Finished pre-loading googlenet.")
		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (loadHandler)")
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited！！"))
			Debug(err.Error())
			return
		}
	} else if strings.Contains(actionName, "ptest08") {
		Debug("LoadHandler starts pre-loading bert.")
		// check if you have an action
		if ap.thebertExecutor == nil || ap.thebertExecutor.cmd == nil {
			Debug("Just create a new bertexecutor")
			NEWbertExecutor1 := NewbertExecutor(ap.outFile, ap.errFile, "_test/loadbert.sh", ap.env)
			ap.thebertExecutor = NEWbertExecutor1
		}
		if ap.thebertExecutor.started {
			Debug("already loaded bert")
			return
		}

		//Pre-load libraries & model
		err := ap.thebertExecutor.Start(false)

		Debug("Handler Finished pre-loading bert.")
		// check for early termination
		if err != nil {
			Debug("WARNING! Command exited (loadHandler)")
			sendError(w, http.StatusBadRequest, fmt.Sprintf("command exited！！"))
			Debug(err.Error())
			return
		}
	} else {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Not defined this model!"))
		return
	}

}
