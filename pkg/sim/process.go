package sim

import (
	"context"
	"io"
	"os/exec"
)

type Process interface {
	PID() int
	Wait() error
	Kill() error
}

type ProcessRunner interface {
	Start(ctx context.Context, command string, args []string, env []string, dir string, stdout io.Writer, stderr io.Writer) (Process, error)
}

type OSProcessRunner struct{}

func (OSProcessRunner) Start(ctx context.Context, command string, args []string, env []string, dir string, stdout io.Writer, stderr io.Writer) (Process, error) {
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &osProcess{cmd: cmd}, nil
}

type osProcess struct {
	cmd *exec.Cmd
}

func (p *osProcess) PID() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *osProcess) Wait() error {
	return p.cmd.Wait()
}

func (p *osProcess) Kill() error {
	if p.cmd.Process == nil {
		return nil
	}
	return p.cmd.Process.Kill()
}
