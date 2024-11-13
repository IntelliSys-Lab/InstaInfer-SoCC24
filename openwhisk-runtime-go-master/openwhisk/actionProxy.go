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
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// ActionProxy is the container of the data specific to a server
type ActionProxy struct {

	// is it initialized?
	initialized bool

	// current directory
	baseDir string

	// Compiler is the script to use to compile your code when action are source code
	compiler string

	// index current dir
	currentDir int

	// theChannel is the channel communicating with the action
	theExecutor *Executor

	// theChannel is the channel communicating with the action

	thealexExecutor      *alexExecutor
	thevggExecutor       *vggExecutor
	theinceptionExecutor *inceptionExecutor
	theresnet18Executor  *resnet18Executor
	theresnet50Executor  *resnet50Executor
	theresnet152Executor *resnet152Executor
	thegooglenetExecutor *googlenetExecutor
	thebertExecutor      *bertExecutor

	theOriginalexExecutor      *OriginalexExecutor
	theOriginvggExecutor       *OriginvggExecutor
	theOrigininceptionExecutor *OrigininceptionExecutor
	theOriginresnet18Executor  *Originresnet18Executor
	theOriginresnet50Executor  *Originresnet50Executor
	theOriginresnet152Executor *Originresnet152Executor
	theOrigingooglenetExecutor *OrigingooglenetExecutor
	theOriginbertExecutor      *OriginbertExecutor

	// out and err files
	outFile *os.File
	errFile *os.File

	// environment
	env map[string]string
}

// NewActionProxy creates a new action proxy that can handle http requests
func NewActionProxy(baseDir string, compiler string, outFile *os.File, errFile *os.File) *ActionProxy {
	os.Mkdir(baseDir, 0755)
	return &ActionProxy{
		false,
		baseDir,
		compiler,
		highestDir(baseDir),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		outFile,
		errFile,
		map[string]string{},
	}
}

//SetEnv sets the environment
func (ap *ActionProxy) SetEnv(env map[string]interface{}) {
	// Propagate proxy version
	ap.env["__OW_PROXY_VERSION"] = Version
	// propagate OW_EXECUTION_ENV as  __OW_EXECUTION_ENV
	ee := os.Getenv("OW_EXECUTION_ENV")
	if ee != "" {
		ap.env["__OW_EXECUTION_ENV"] = ee
	}
	// require an ack
	wa := os.Getenv("OW_WAIT_FOR_ACK")
	if wa != "" {
		ap.env["__OW_WAIT_FOR_ACK"] = wa
	}
	// propagate all the variables starting with "__OW_"
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "__OW_") {
			res := strings.Split(v, "=")
			ap.env[res[0]] = res[1]
		}
	}
	// get other variables from the init payload
	for k, v := range env {
		s, ok := v.(string)
		if ok {
			ap.env[k] = s
			continue
		}
		buf, err := json.Marshal(v)
		if err == nil {
			ap.env[k] = string(buf)
		}
	}
	Debug("init env: %s", ap.env)
}

// Unset user environment
func (ap *ActionProxy) UnsetEnv() {
	ap.env = map[string]string{}
	Debug("clean env: %s", ap.env)
}

// StartLatestAction tries to start
// the more recently uploaded
// action if valid, otherwise remove it
// and fallback to the previous, if any
//主要作用：启动一个全新的executor。如果有旧executor，把它删掉
func (ap *ActionProxy) StartLatestAction() error {

	// find the action if any
	highestDir := highestDir(ap.baseDir)
	if highestDir == 0 {
		Debug("no action found")
		ap.theExecutor = nil
		return fmt.Errorf("no valid actions available")
	}

	// check version
	execEnv := os.Getenv("OW_EXECUTION_ENV")
	if execEnv != "" {
		execEnvFile := fmt.Sprintf("%s/%d/bin/exec.env", ap.baseDir, highestDir)
		execEnvData, err := ioutil.ReadFile(execEnvFile)
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(execEnvData)) != execEnv {
			fmt.Printf("Expected exec.env should start with %s\nActual value: %s", execEnv, execEnvData)
			return fmt.Errorf("Execution environment version mismatch. See logs for details.")
		}
	}

	//Create executor for each inference function：
	NEWalexExecutor := NewalexExecutor(ap.outFile, ap.errFile, "_test/loadalex.sh", ap.env)
	NEWvggExecutor := NewvggExecutor(ap.outFile, ap.errFile, "_test/loadvgg.sh", ap.env)
	NEWinceptionExecutor := NewinceptionExecutor(ap.outFile, ap.errFile, "_test/loadinception.sh", ap.env)
	NEWresnet18Executor := Newresnet18Executor(ap.outFile, ap.errFile, "_test/loadres18.sh", ap.env)
	NEWresnet50Executor := Newresnet50Executor(ap.outFile, ap.errFile, "_test/loadres50.sh", ap.env)
	NEWresnet152Executor := Newresnet152Executor(ap.outFile, ap.errFile, "_test/loadres152.sh", ap.env)
	NEWgooglenetExecutor := NewgooglenetExecutor(ap.outFile, ap.errFile, "_test/loadgooglenet.sh", ap.env)
	NEWbertExecutor := NewbertExecutor(ap.outFile, ap.errFile, "_test/loadbert.sh", ap.env)

	if ap.thealexExecutor == nil {
		ap.thealexExecutor = NEWalexExecutor
	}
	if ap.thevggExecutor == nil {
		ap.thevggExecutor = NEWvggExecutor
	}
	if ap.theinceptionExecutor == nil {
		ap.theinceptionExecutor = NEWinceptionExecutor
	}
	if ap.theresnet18Executor == nil {
		ap.theresnet18Executor = NEWresnet18Executor
	}
	if ap.theresnet50Executor == nil {
		ap.theresnet50Executor = NEWresnet50Executor
	}
	if ap.theresnet152Executor == nil {
		ap.theresnet152Executor = NEWresnet152Executor
	}
	if ap.thegooglenetExecutor == nil {
		ap.thegooglenetExecutor = NEWgooglenetExecutor
	}
	if ap.thebertExecutor == nil {
		ap.thebertExecutor = NEWbertExecutor
	}

	NEWOriginalexExecutor := NewOriginalexExecutor(ap.outFile, ap.errFile, "_test/funcalex.sh", ap.env)
	ap.theOriginalexExecutor = NEWOriginalexExecutor

	NEWOriginvggExecutor := NewOriginvggExecutor(ap.outFile, ap.errFile, "_test/funcvgg.sh", ap.env)
	ap.theOriginvggExecutor = NEWOriginvggExecutor

	NEWOrigininceptionExecutor := NewOrigininceptionExecutor(ap.outFile, ap.errFile, "_test/funcinception.sh", ap.env)
	ap.theOrigininceptionExecutor = NEWOrigininceptionExecutor

	NEWOriginresnet18Executor := NewOriginresnet18Executor(ap.outFile, ap.errFile, "_test/func18.sh", ap.env)
	ap.theOriginresnet18Executor = NEWOriginresnet18Executor

	NEWOriginresnet50Executor := NewOriginresnet50Executor(ap.outFile, ap.errFile, "_test/func50.sh", ap.env)
	ap.theOriginresnet50Executor = NEWOriginresnet50Executor

	NEWOriginresnet152Executor := NewOriginresnet152Executor(ap.outFile, ap.errFile, "_test/func152.sh", ap.env)
	ap.theOriginresnet152Executor = NEWOriginresnet152Executor

	NEWOrigingooglenetExecutor := NewOrigingooglenetExecutor(ap.outFile, ap.errFile, "_test/funcgooglenet.sh", ap.env)
	ap.theOrigingooglenetExecutor = NEWOrigingooglenetExecutor

	NEWOriginbertExecutor := NewOriginbertExecutor(ap.outFile, ap.errFile, "_test/funcbert.sh", ap.env)
	ap.theOriginbertExecutor = NEWOriginbertExecutor

	// save the current executor  将ActionProxy结构体中的成员theExecutor的值赋给curExecutor
	curExecutor := ap.theExecutor

	// try to launch the action
	//通过格式化字符串函数生成一个路径，并赋值给executable  /action/1/bin/
	executable := fmt.Sprintf("%s/%d/bin/exec", ap.baseDir, highestDir)
	os.Chmod(executable, 0755) //改变executable文件的权限为0755
	//生成一个新Executor，并将其赋给newExecutor
	newExecutor := NewExecutor(ap.outFile, ap.errFile, executable, ap.env)
	Debug("starting %s", executable)

	// start executor 这是唯一使用到executor.Start()的地方
	//executor.Start()没有将cmd作为input，而是直接读取executor类中的cmd：
	//也就是executable := fmt.Sprintf("%s/%d/bin/exec", ap.baseDir, highestDir)

	err := newExecutor.Start(os.Getenv("OW_WAIT_FOR_ACK") != "")
	if err == nil {
		ap.theExecutor = newExecutor
		if curExecutor != nil {
			Debug("stopping old executor")
			curExecutor.Stop()
		}
		return nil
	}

	// cannot start, removing the action
	// and leaving the current executor running
	if !Debugging {
		exeDir := fmt.Sprintf("./action/%d/", highestDir)
		Debug("removing the failed action in %s", exeDir)
		os.RemoveAll(exeDir)
	}
	return err
}

//这里用来处理ContainerProxy.scala发来的signal
func (ap *ActionProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/init":
		Debug("Proxy Receive an init Signal")
		ap.initHandler(w, r)
	case "/load":
		Debug("Proxy Receive a load Signal")
		if !ap.HasAnyExecutorStarted() {
			ap.loadHandler(w, r)
		}
	case "/offload":
		Debug("Proxy Receive an offload Signal")
		ap.offloadHandler(w, r)
	case "/run":
		Debug("Proxy Receive a run Signal")
		ap.loadRunHandler(w, r)
	case "/clean":
		Debug("Proxy Receive a clean Signal")
		ap.cleanHandler(w, r)
	}
}

// Start creates a proxy to execute actions
func (ap *ActionProxy) Start(port int) {
	// listen and start
	//启动一个 HTTP 服务器，该服务器监听在指定的端口，并使用 ActionProxy 作为处理器
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), ap))
}

// ExtractAndCompileIO read in input and write in output to use the runtime as a compiler "on-the-fly"
func (ap *ActionProxy) ExtractAndCompileIO(r io.Reader, w io.Writer, main string, env string) {

	// read the std input
	in, err := ioutil.ReadAll(r) //从输入流 r 中读取所有数据，将数据赋给 in，如果读取过程中出现错误，该错误被赋值给 err
	if err != nil {
		log.Fatal(err)
	}

	envMap := make(map[string]interface{})
	if env != "" {
		json.Unmarshal([]byte(env), &envMap)
	}
	ap.SetEnv(envMap) //设置环境变量

	// extract and compile it
	//ExtractAndCompileIO 是更高级别的函数，它处理的是输入/输出流，而 ExtractAndCompile 处理的是字节切片。
	//这意味着 ExtractAndCompileIO 可以直接从输入流中读取数据并向输出流写入数据， 而 ExtractAndCompile 则需要提前得到字节切片。
	file, err := ap.ExtractAndCompile(&in, main) //提取和编译输入内容，编译后的文件路径被赋值给 file
	if err != nil {
		log.Fatal(err)
	}

	// zip the directory containing the file and write output
	zip, err := Zip(filepath.Dir(file))
	if err != nil {
		log.Fatal(err)
	}

	_, err = w.Write(zip)
	if err != nil {
		log.Fatal(err)
	}
}

func (ap *ActionProxy) StopAllExecutorsExcept(name string) {
	if name != "alex" && ap.thealexExecutor != nil && ap.thealexExecutor.started {
		ap.thealexExecutor.Stop()
		ap.thealexExecutor = nil
		Debug("ap stopped alex")
	}
	if name != "vgg" && ap.thevggExecutor != nil && ap.thevggExecutor.started {
		ap.thevggExecutor.Stop()
		ap.thevggExecutor = nil
		Debug("ap stopped vgg")
	}
	if name != "inception" && ap.theinceptionExecutor != nil && ap.theinceptionExecutor.started {
		ap.theinceptionExecutor.Stop()
		ap.theinceptionExecutor = nil
		Debug("ap stopped inception")
	}
	if name != "resnet18" && ap.theresnet18Executor != nil && ap.theresnet18Executor.started {
		ap.theresnet18Executor.Stop()
		ap.theresnet18Executor = nil
		Debug("ap stopped resnet18")
	}
	if name != "resnet50" && ap.theresnet50Executor != nil && ap.theresnet50Executor.started {
		ap.theresnet50Executor.Stop()
		ap.theresnet50Executor = nil
		Debug("ap stopped resnet50")
	}
	if name != "resnet152" && ap.theresnet152Executor != nil && ap.theresnet152Executor.started {
		ap.theresnet152Executor.Stop()
		ap.theresnet152Executor = nil
		Debug("ap stopped resnet152")
	}
	if name != "googlenet" && ap.thegooglenetExecutor != nil && ap.thegooglenetExecutor.started {
		ap.thegooglenetExecutor.Stop()
		ap.thegooglenetExecutor = nil
		Debug("ap stopped googlenet")
	}
	if name != "bert" && ap.thebertExecutor != nil && ap.thebertExecutor.started {
		ap.thebertExecutor.Stop()
		ap.thebertExecutor = nil
		Debug("ap stopped bert")
	}
}

//在load前，检查proxy中是否正在执行OriginExecutor（non-loaded function)
func (ap *ActionProxy) HasAnyExecutorStarted() bool {
	if ap.theOriginalexExecutor != nil && ap.theOriginalexExecutor.started {
		return true
	}
	if ap.theOriginvggExecutor != nil && ap.theOriginvggExecutor.started {
		return true
	}
	if ap.theOrigininceptionExecutor != nil && ap.theOrigininceptionExecutor.started {
		return true
	}
	if ap.theOriginresnet18Executor != nil && ap.theOriginresnet18Executor.started {
		return true
	}
	if ap.theOriginresnet50Executor != nil && ap.theOriginresnet50Executor.started {
		return true
	}
	if ap.theOriginresnet152Executor != nil && ap.theOriginresnet152Executor.started {
		return true
	}
	if ap.theOrigingooglenetExecutor != nil && ap.theOrigingooglenetExecutor.started {
		return true
	}
	if ap.theOriginbertExecutor != nil && ap.theOriginbertExecutor.started {
		return true
	}
	return false
}
