package sim

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

type fakeRunner struct {
	mu      sync.Mutex
	command string
	args    []string
	process *fakeProcess
}

func (r *fakeRunner) Start(ctx context.Context, command string, args []string, env []string, dir string, stdout io.Writer, stderr io.Writer) (Process, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.command = command
	r.args = append([]string(nil), args...)
	r.process = newFakeProcess(42)
	return r.process, nil
}

type fakeProcess struct {
	mu     sync.Mutex
	pid    int
	done   chan error
	closed bool
}

func newFakeProcess(pid int) *fakeProcess {
	return &fakeProcess{pid: pid, done: make(chan error)}
}

func (p *fakeProcess) PID() int {
	return p.pid
}

func (p *fakeProcess) Wait() error {
	return <-p.done
}

func (p *fakeProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		close(p.done)
		p.closed = true
	}
	return nil
}

func TestManagerStartNodeBuildsCommand(t *testing.T) {
	root := testBuildRoot(t)
	runner := &fakeRunner{}
	manager := NewManager(
		WithRootDir(root),
		WithBinaryPath(filepath.Join("bin", "dkvs-node.exe")),
		WithRunner(runner),
		WithNodes([]NodeConfig{
			{Name: "node1", RaftAddr: "localhost:5001", ControlAddr: "localhost:6001"},
			{Name: "node2", RaftAddr: "localhost:5002", ControlAddr: "localhost:6002"},
		}),
	)

	if err := manager.StartNode(context.Background(), "node1"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.StopNode("node1") })

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.command != filepath.Join(root, "bin", "dkvs-node.exe") {
		t.Errorf("unexpected command path %q", runner.command)
	}
	for _, want := range []string{"-id", "localhost:5001", "-peers", "localhost:5002", "-control", "localhost:6001"} {
		if !slices.Contains(runner.args, want) {
			t.Errorf("expected args to contain %q, got %v", want, runner.args)
		}
	}
}

func testBuildRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{filepath.Join(root, "cmd", "node"), filepath.Join(root, "pkg"), filepath.Join(root, "bin")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	binary := filepath.Join(root, "bin", "dkvs-node.exe")
	if err := os.WriteFile(binary, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}
	return root
}
