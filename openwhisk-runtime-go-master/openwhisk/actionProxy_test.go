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
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"
)

func Example_startTestServer() {
	ts, cur, log := startTestServer("")
	res, _, _ := doPost(ts.URL+"/init", "{}")
	fmt.Print(res)
	res, _, _ = doPost(ts.URL+"/init", "XXX")
	fmt.Print(res)
	res, _, _ = doPost(ts.URL+"/run", "{}")
	fmt.Print(res)
	res, _, _ = doPost(ts.URL+"/run", "XXX")
	fmt.Print(res)
	stopTestServer(ts, cur, log)
	// Output:
	// {"error":"Missing main/no code to execute."}
	// {"error":"Error unmarshaling request: invalid character 'X' looking for beginning of value"}
	// {"error":"no action defined yet"}
	// {"error":"no action defined yet"}
}

func TestStartLatestAction_emit1(t *testing.T) {
	os.RemoveAll("./action/t2")
	logf, _ := ioutil.TempFile("/tmp", "log")
	ap := NewActionProxy("./action/t2", "", logf, logf)
	// start the action that emits 1
	buf := []byte("#!/bin/sh\nwhile read a; do echo 1 >&3 ; done\n")
	ap.ExtractAction(&buf, "bin")
	ap.StartLatestAction()
	res, _ := ap.theExecutor.Interact([]byte("x"))
	assert.Equal(t, res, []byte("1\n"))
	ap.theExecutor.Stop()
}

func TestStartLatestAction_terminate(t *testing.T) {
	os.RemoveAll("./action/t3")
	logf, _ := ioutil.TempFile("/tmp", "log")
	ap := NewActionProxy("./action/t3", "", logf, logf)
	// now start an action that terminate immediately
	buf := []byte("#!/bin/sh\ntrue\n")
	ap.ExtractAction(&buf, "bin")
	ap.StartLatestAction()
	assert.Nil(t, ap.theExecutor)
}

func TestStartLatestAction_emit2(t *testing.T) {
	os.RemoveAll("./action/t4")
	logf, _ := ioutil.TempFile("/tmp", "log")
	ap := NewActionProxy("./action/t4", "", logf, logf)
	// start the action that emits 2
	buf := []byte("#!/bin/sh\nwhile read a; do echo 2 >&3 ; done\n")
	ap.ExtractAction(&buf, "bin")
	ap.StartLatestAction()

	body := map[string]interface{}{
		"action_name":    "/guest/test05",
		"action_version": "0.0.1",
		"activation_id":  "31eaceb63dda44e1aaceb63ddae4e1a2",
		"deadline":       "1693902427193",
		"namespace":      "guest",
		"transaction_id": "d430c71ff9c7c1762cfa19e5a1c289d3",
		"value":          map[string]string{"name": "OpenAI"},
	}

	// 使用 json.Marshal 将 map 转换为 JSON 格式的字节流
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return
	}
	// 输出生成的 JSON 字符串
	fmt.Println(string(bodyBytes))

	// 测试：提取actionName
	var req requestBody
	err = json.Unmarshal(bodyBytes, &req)
	if err != nil {
		return
	}
	actionName := req.ActionName
	fmt.Println(string("ActionName:"))
	fmt.Println(string(actionName))

	// load model
	err1 := ap.theresnet50Executor.Start(false)
	//res, _ := ap.theOriginresnet50Executor.StartAndWaitForOutput()

	fmt.Println(string("Noerr:"))
	//fmt.Println(err1.Error())
	// check for early termination
	if err1 != nil {
		Debug("WARNING! Command exited")
		fmt.Println(string("err:"))
		//ap.theresnet50Executor = nil
		//return
	}
	time.Sleep(1 * time.Second)
	res, _ := ap.theresnet50Executor.Interact([]byte(bodyBytes))

	fmt.Println(string("res:"))
	fmt.Println(string(res))
	//fmt.Println(string("err:"))
	//fmt.Println(string(err2.Error()))
	//fmt.Println(string(err1.Error()))

	var objmap map[string]*json.RawMessage
	fmt.Println("JSON:")
	resStr := strings.ReplaceAll(string(res), "'", "\"")
	err = json.Unmarshal([]byte(resStr), &objmap)
	if err != nil {
		fmt.Println(err)
	}
	res = []byte(resStr)
	fmt.Println([]byte(resStr))
	fmt.Println(string(res))
	dump(logf)
}

func Example_compile_bin() {
	os.RemoveAll("./action/c1")
	logf, _ := ioutil.TempFile("/tmp", "log")
	ap := NewActionProxy("./action/c1", "_test/compile.py", logf, logf)
	dat, _ := Zip("_test/pysample")
	inp := bytes.NewBuffer(dat)
	out := new(bytes.Buffer)
	ap.ExtractAndCompileIO(inp, out, "main", "")
	Unzip(out.Bytes(), "./action/c1/out")
	sys("_test/find.sh", "./action/c1/out")
	// Output:
	// ./action/c1/out
	// ./action/c1/out/exec
	// ./action/c1/out/lib
	// ./action/c1/out/lib/action
	// ./action/c1/out/lib/action/__init__.py
	// ./action/c1/out/lib/action/main.py
	// ./action/c1/out/lib/exec.py
}

func Example_compile_src() {
	os.RemoveAll("./action/c2")
	logf, _ := ioutil.TempFile("/tmp", "log")
	ap := NewActionProxy("./action/c2", "_test/compile.py", logf, logf)
	log.Println(ioutil.ReadAll(logf))
	dat, _ := Zip("_test/pysample/lib")
	inp := bytes.NewBuffer(dat)
	out := new(bytes.Buffer)
	ap.ExtractAndCompileIO(inp, out, "main", "")
	Unzip(out.Bytes(), "./action/c2/out")
	sys("_test/find.sh", "./action/c2/out")
	// Output:
	// ./action/c2/out
	// ./action/c2/out/action
	// ./action/c2/out/action/action
	// ./action/c2/out/action/action/__init__.py
	// ./action/c2/out/action/action/main.py
	// ./action/c2/out/action/exec.py
	// ./action/c2/out/exec
}

func Example_badcompile() {

	os.Setenv("OW_LOG_INIT_ERROR", "1")
	ts, cur, log := startTestServer("_test/badcompile.sh")
	res, _, _ := doPost(ts.URL+"/init", initBytes([]byte("hello"), "main"))
	fmt.Print(res)
	stopTestServer(ts, cur, log)
	os.Setenv("OW_LOG_INIT_ERROR", "")
	// Unordered output:
	// {"error":"The action failed to generate or locate a binary. See logs for details."}
	// error in stdout
	// error in stderr
	//
	// XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX
	// XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX

}

func Example_SetEnv() {
	ap := NewActionProxy("", "", nil, nil)
	fmt.Println(ap.env)
	var m map[string]interface{}
	json.Unmarshal([]byte(`{
		  "s": "string",
		  "n": 123,
		  "a": [1,2,3],
		  "o": {"a":1,"b":2}
		}`), &m)
	log.Println(m)
	ap.SetEnv(m)
	fmt.Println(ap.env["a"], ap.env["o"], ap.env["s"], ap.env["n"])
	// Output:
	// map[]
	// [1,2,3] {"a":1,"b":2} string 123

}

func Example_executionEnv_nocheck() {
	os.Setenv("OW_EXECUTION_ENV", "")
	ts, cur, log := startTestServer("")
	res, _, _ := doPost(ts.URL+"/init", initBinary("_test/helloack.zip", "main"))
	fmt.Print(res)
	stopTestServer(ts, cur, log)
	// Output:
	// {"ok":true}
}

func Example_executionEnv_check() {
	os.Setenv("OW_EXECUTION_ENV", "bad/env")
	ts, cur, log := startTestServer("")
	res, _, _ := doPost(ts.URL+"/init", initBinary("_test/helloack.zip", "main"))
	fmt.Print(res)
	os.Setenv("OW_EXECUTION_ENV", "exec/env")
	res, _, _ = doPost(ts.URL+"/init", initBinary("_test/helloack.zip", "main"))
	fmt.Print(res)
	stopTestServer(ts, cur, log)
	// reset value
	os.Setenv("OW_EXECUTION_ENV", "")
	// Output:
	// Expected exec.env should start with bad/env
	// Actual value: exec/env
	// {"error":"cannot start action: Execution environment version mismatch. See logs for details."}
	// {"ok":true}
}
