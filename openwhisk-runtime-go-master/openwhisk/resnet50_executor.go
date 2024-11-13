package openwhisk

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// OutputGuard constant string

// Executor is the container and the guardian  of a child process
// It starts a command, feeds input and output, read logs and control its termination
type resnet50Executor struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	output  *bufio.Reader
	exited  chan bool
	started bool
}

// NewExecutor creates a child subprocess using the provided command line,
// writing the logs in the given file.
// You can then start it getting a communication channel

func Newresnet50Executor(logout *os.File, logerr *os.File, command string, env map[string]string, args ...string) (proc *resnet50Executor) {
	//env:子进程的环境变量; cmd + arg: 真正的命令
	cmd := exec.Command(command, args...) //创建一个可以用来启动命令的 *Cmd
	// 创建一个新的进程组
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	//cmd.Stdout = logout
	//cmd.Stdout = cmd.StdoutPipe()
	//cmd.Stderr = logerr
	cmd.Env = []string{} //初始化 *Cmd 的 Env 字段，这个字段用来设置子进程的环境变量
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v) //遍历传入的环境变量 env，并将它们添加到 *Cmd 的 Env 字段
	}
	Debug("env: %v", cmd.Env) //如果 Debugging 是 true，则输出环境变量到调试日志
	if Debugging {
		cmd.Env = append(cmd.Env, "OW_DEBUG=/tmp/action.log")
	}

	input, err := cmd.StdinPipe() //调用 *Cmd 的 StdinPipe 方法，返回一个连接到命令标准输入的管道和一个错误
	if err != nil {
		return nil
	}

	output, err := cmd.StdoutPipe()
	if err != nil {
		return nil
	}

	cmd.Stderr = cmd.Stdout

	return &resnet50Executor{
		cmd,
		input,
		bufio.NewReader(output),
		make(chan bool),
		false,
	}
}

// Start starts the Executor's command and waits for it to be ready to accept input.
// If waitForAck is true, it waits indefinitely for an acknowledgement from the command.
// If waitForAck is false, it waits for a short time to check if the command has exited.
func (proc *resnet50Executor) Start(waitForAck bool) error {
	Debug("Start Loading ResNet50:")
	//reader, writer := io.Pipe()
	//proc.cmd.Stdout = io.MultiWriter(os.Stdout, writer)
	//proc.output = bufio.NewReader(reader)
	proc.started = true
	err := proc.cmd.Start()
	if err != nil {
		proc.cmd = nil // No need to keep the command around if it failed to start
		Debug(err.Error())
		return fmt.Errorf("failed to start command: %w", err)
	}
	Debug("resnet50 pid: %d", proc.cmd.Process.Pid) //如果 Debugging 是 true，则在调试日志中输出命令的进程 ID

	Debug("Executor Finished pre-loading ResNet50.")

	go func() {
		proc.cmd.Wait()
		proc.exited <- true
	}()

	if !waitForAck {
		select {
		case <-proc.exited:
			return fmt.Errorf("command exited!!")
		case <-time.After(100 * time.Millisecond):
			return nil
		}
	}

	// If we reach here, waitForAck is true, so we wait for an acknowledgement from the command
	ack := make(chan error)
	go func() {
		out, err := proc.output.ReadBytes('\n')
		if err != nil {
			ack <- err
			return
		}

		var ackData ActionAck
		err = json.Unmarshal(out, &ackData)
		if err != nil {
			ack <- err
			return
		}

		if !ackData.Ok {
			ack <- fmt.Errorf("The action did not initialize properly.")
			return
		}

		ack <- nil
	}()

	select {
	case err = <-ack:
		return err
	case <-proc.exited:
		return fmt.Errorf("command exited abruptly during initialization")
	}
}

func (proc *resnet50Executor) IsStarted() bool {
	return proc.started
}

func (proc *resnet50Executor) Interact1(in []byte) ([]byte, error) {
	_, err := proc.input.Write(in)
	if err != nil {
		return nil, fmt.Errorf("failed to write to stdin: %w", err)
	}

	_, err = proc.input.Write([]byte("\n"))
	if err != nil {
		return nil, fmt.Errorf("failed to write newline to stdin: %w", err)
	}

	var outputBuffer bytes.Buffer
	chout := make(chan []byte)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		/*
			在原始的代码中，你的 go func() goroutine 是通过 io.Copy() 来获取子进程的所有输出的。
			然而，io.Copy() 会一直读取直到遇到EOF或者发生错误。这就导致了问题，
			因为你提到子进程是永远不会结束的，所以 io.Copy() 会一直等待更多的输出，而不会返回结果。
		*/
		_, err := io.Copy(&outputBuffer, proc.output)
		if err != nil {
			// Handle error, maybe log it or send it somewhere else.
			Debug("Meet Error while Interacting!:")
			Debug(err.Error())
			fmt.Errorf("meet error when copy output: %w", err)
		}
		chout <- outputBuffer.Bytes()
	}()

	//这个是不行的：
	/* 如果Python子进程在输出结果后并不立即退出，那么 case <-proc.exited: 就会阻塞，
	即使 chout 通道中已经有了Python子进程的输出。
	在这种情况下，你的 Interact 方法将会一直等待，直到超时。
	*/

	select {
	case out := <-chout:
		if len(out) == 0 {
			return nil, errors.New("no answer from the action")
		}
		proc.started = false
		return out, nil
	case <-proc.exited:
		proc.started = false
		return nil, errors.New("command exited!!!!!!!!")
	}
}

func (proc *resnet50Executor) Interact(in []byte) ([]byte, error) {
	_, err := proc.input.Write(in)
	if err != nil {
		return nil, fmt.Errorf("failed to write to stdin: %w", err)
	}

	_, err = proc.input.Write([]byte("\n"))
	if err != nil {
		return nil, fmt.Errorf("failed to write newline to stdin: %w", err)
	}

	chout := make(chan []byte)

	go func() {
		reader := bufio.NewReader(proc.output)
		line, _, err := reader.ReadLine()
		if err != nil {
			Debug("Res50 Meet Error while Interacting!:")
			Debug(err.Error())
			fmt.Errorf("meet error when scanning output: %w", err)
			return
		}
		chout <- line
	}()

	timer := time.NewTimer(15 * time.Second)

	select {
	case out := <-chout:
		if !timer.Stop() {
			<-timer.C
		}
		if len(out) == 0 {
			return nil, errors.New("no answer from the Res50 action")
		}
		//proc.started = false
		return out, nil
	case <-timer.C:
		proc.started = false
		return nil, errors.New("Res50 operation timed out")
	}
}

//func (proc *resnet50Executor) Interact(in []byte) ([]byte, error) {
//	_, err := proc.input.Write(in)
//	if err != nil {
//		return nil, fmt.Errorf("failed to write to stdin: %w", err)
//	}
//
//	_, err = proc.input.Write([]byte("\n"))
//	if err != nil {
//		return nil, fmt.Errorf("failed to write newline to stdin: %w", err)
//	}
//
//	chout := make(chan []byte)
//
//	go func() {
//		scanner := bufio.NewScanner(proc.output)
//		for scanner.Scan() {
//			chout <- scanner.Bytes()
//			return
//		}
//		if err := scanner.Err(); err != nil {
//			Debug("Meet Error while Interacting!:")
//			Debug(err.Error())
//			fmt.Errorf("meet error when scanning output: %w", err)
//		}
//	}()
//
//	timer := time.NewTimer(3 * time.Second)
//	defer timer.Stop()
//
//	select {
//	case out := <-chout:
//		if len(out) == 0 {
//			return nil, errors.New("no answer from the action")
//		}
//		proc.started = false
//		return out, nil
//	case <-timer.C:
//		proc.started = false
//		return nil, errors.New("operation timed out")
//	}
//}

// Stop will kill the process
// and close the channels
func (proc *resnet50Executor) Stop() {
	Debug("stopping res50")

	proc.started = false
	if proc.cmd != nil {
		//// Get the process to kill
		//processToKill, err := os.FindProcess(proc.cmd.Process.Pid + 1)
		//if err != nil {
		//	Debug("Failed to find process: %v", err)
		//	return
		//}
		//
		//// Kill the process
		//if err := processToKill.Kill(); err != nil {
		//	Debug("Failed to kill process: %v", err)
		//	return
		//}
		//
		//// Release the process
		//if err := processToKill.Release(); err != nil {
		//	Debug("Failed to release process: %v", err)
		//}
		//proc.cmd.Process.Kill()

		pgid, err := syscall.Getpgid(proc.cmd.Process.Pid)
		if err == nil {
			syscall.Kill(-pgid, 9) // 注意pgid必须是负数 “9”表示SIGKILL信号
		} else {
			fmt.Printf("获取进程组失败: %v\n", err)
			os.Exit(1)
		}

		proc.started = false
		proc.cmd = nil
	}
	runtime.GC()
}
