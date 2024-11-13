package openwhisk

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"
)

// OutputGuard constant string

// Executor is the container and the guardian  of a child process
// It starts a command, feeds input and output, read logs and control its termination
type googlenetExecutor struct {
	cmd     *exec.Cmd
	input   io.WriteCloser
	output  *bufio.Reader
	exited  chan bool
	started bool
}

// NewExecutor creates a child subprocess using the provided command line,
// writing the logs in the given file.
// You can then start it getting a communication channel

func NewgooglenetExecutor(logout *os.File, logerr *os.File, command string, env map[string]string, args ...string) (proc *googlenetExecutor) {
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
		print("googlenetExecutor input meets an error:")
		print(err.Error())
		return nil
	}

	output, err := cmd.StdoutPipe()
	if err != nil {
		print("googlenetExecutor output meets an error:")
		print(err.Error())
		return nil
	}

	cmd.Stderr = cmd.Stdout

	return &googlenetExecutor{
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
func (proc *googlenetExecutor) Start(waitForAck bool) error {
	Debug("Start Loading googlenetNet (pre-load):")
	//reader, writer := io.Pipe()
	//proc.cmd.Stdout = io.MultiWriter(os.Stdout, writer)
	//proc.output = bufio.NewReader(reader)
	proc.started = true

	err := proc.cmd.Start()

	if err != nil {
		Debug(err.Error())
		proc.cmd = nil // No need to keep the command around if it failed to start
		return fmt.Errorf("failed to start command: %w", err)
	}
	Debug("googlenetNet pid: %d", proc.cmd.Process.Pid) //如果 Debugging 是 true，则在调试日志中输出命令的进程 ID
	Debug("Executor Finished pre-loading googlenetNet.")

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

func (proc *googlenetExecutor) IsStarted() bool {
	return proc.started
}

func (proc *googlenetExecutor) Interact(in []byte) ([]byte, error) {
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
			Debug("Res18 Meet Error while Interacting!:")
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
			return nil, errors.New("no answer from the Res18 action")
		}
		//proc.started = false
		return out, nil
	case <-timer.C:
		proc.started = false
		return nil, errors.New("Res18 operation timed out")
	}
}

// Stop will kill the process
// and close the channels
func (proc *googlenetExecutor) Stop() {
	Debug("stopping googlenet")

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
