package sim

import (
	"Distributed_Key_Value_Store/pkg/raft"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	mu          sync.RWMutex
	rootDir     string
	binaryPath  string
	nodes       []NodeConfig
	processes   map[string]*managedProcess
	partitions  [][]string
	events      *EventBus
	runner      ProcessRunner
	httpClient  *http.Client
	buildTarget string
}

type managedProcess struct {
	process Process
	cancel  context.CancelFunc
	started time.Time
}

type Option func(*Manager)

func NewManager(options ...Option) *Manager {
	m := &Manager{
		rootDir:     ".",
		binaryPath:  defaultBinaryPath(),
		nodes:       DefaultNodes(),
		processes:   make(map[string]*managedProcess),
		events:      NewEventBus(500),
		runner:      OSProcessRunner{},
		httpClient:  &http.Client{Timeout: 500 * time.Millisecond},
		buildTarget: "./cmd/node",
	}
	for _, option := range options {
		option(m)
	}
	return m
}

func WithRootDir(rootDir string) Option {
	return func(m *Manager) { m.rootDir = rootDir }
}

func WithBinaryPath(path string) Option {
	return func(m *Manager) { m.binaryPath = path }
}

func WithNodes(nodes []NodeConfig) Option {
	return func(m *Manager) { m.nodes = append([]NodeConfig(nil), nodes...) }
}

func WithRunner(runner ProcessRunner) Option {
	return func(m *Manager) { m.runner = runner }
}

func WithHTTPClient(client *http.Client) Option {
	return func(m *Manager) { m.httpClient = client }
}

func DefaultNodes() []NodeConfig {
	return []NodeConfig{
		{Name: "node1", RaftAddr: "127.0.0.1:5001", ControlAddr: "127.0.0.1:6001"},
		{Name: "node2", RaftAddr: "127.0.0.1:5002", ControlAddr: "127.0.0.1:6002"},
		{Name: "node3", RaftAddr: "127.0.0.1:5003", ControlAddr: "127.0.0.1:6003"},
	}
}

func (m *Manager) Events() *EventBus {
	return m.events
}

func (m *Manager) EnsureNodeBinary(ctx context.Context) error {
	needsBuild, err := m.needsBuild()
	if err != nil {
		return err
	}
	if !needsBuild {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(m.binaryAbsPath()), 0755); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, "go", "build", "-o", m.binaryAbsPath(), m.buildTarget)
	cmd.Dir = m.rootDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("build node binary: %w: %s", err, strings.TrimSpace(string(output)))
	}
	m.events.Publish(Event{Type: "sim.build", Message: "node binary built", Data: map[string]string{"path": m.binaryAbsPath()}})
	return nil
}

func (m *Manager) StartCluster(ctx context.Context) error {
	if err := m.EnsureNodeBinary(ctx); err != nil {
		return err
	}
	for _, node := range m.nodesSnapshot() {
		if err := m.StartNode(ctx, node.Name); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) StopCluster() error {
	for _, node := range m.nodesSnapshot() {
		if err := m.StopNode(node.Name); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) StartNode(ctx context.Context, name string) error {
	node, ok := m.nodeByName(name)
	if !ok {
		return fmt.Errorf("unknown node %q", name)
	}
	if err := m.EnsureNodeBinary(ctx); err != nil {
		return err
	}
	args := []string{"-id", node.RaftAddr, "-peers", strings.Join(m.peerRaftAddrs(node.Name), ","), "-control", node.ControlAddr}

	m.mu.Lock()
	if _, running := m.processes[name]; running {
		m.mu.Unlock()
		return nil
	}
	nodeCtx, cancel := context.WithCancel(context.Background())
	stdout := newLineEventWriter(func(line string) {
		m.events.Publish(Event{Type: "node.log", Node: name, Message: line, Data: map[string]string{"stream": "stdout"}})
	})
	stderr := newLineEventWriter(func(line string) {
		m.events.Publish(Event{Type: "node.log", Node: name, Message: line, Data: map[string]string{"stream": "stderr"}})
	})
	process, err := m.runner.Start(nodeCtx, m.binaryAbsPath(), args, nil, m.rootDir, stdout, stderr)
	if err != nil {
		cancel()
		m.mu.Unlock()
		return err
	}
	m.processes[name] = &managedProcess{process: process, cancel: cancel, started: time.Now().UTC()}
	m.mu.Unlock()

	m.events.Publish(Event{Type: "node.started", Node: name, Message: "node process started", Data: map[string]any{"pid": process.PID(), "raftAddr": node.RaftAddr, "controlAddr": node.ControlAddr}})
	go m.waitForProcess(name, process)
	return nil
}

func (m *Manager) StopNode(name string) error {
	m.mu.Lock()
	managed, running := m.processes[name]
	if running {
		delete(m.processes, name)
	}
	m.mu.Unlock()
	if !running {
		return nil
	}
	managed.cancel()
	err := managed.process.Kill()
	m.events.Publish(Event{Type: "node.stopped", Node: name, Message: "node process stopped"})
	return err
}

func (m *Manager) Snapshot(ctx context.Context) ClusterSnapshot {
	nodes := m.nodesSnapshot()
	snapshots := make([]NodeSnapshot, 0, len(nodes))
	for _, node := range nodes {
		snapshot := NodeSnapshot{Name: node.Name, RaftAddr: node.RaftAddr, ControlAddr: node.ControlAddr}
		if managed, ok := m.managedProcess(node.Name); ok {
			snapshot.Running = true
			snapshot.PID = managed.process.PID()
			status, err := m.fetchStatus(ctx, node)
			if err != nil {
				snapshot.Error = err.Error()
			} else {
				snapshot.Status = status
			}
		}
		snapshots = append(snapshots, snapshot)
	}

	m.mu.RLock()
	partitions := cloneGroups(m.partitions)
	m.mu.RUnlock()
	return ClusterSnapshot{Nodes: snapshots, Partitions: partitions}
}

func (m *Manager) ApplyPartitions(ctx context.Context, groups [][]string) error {
	nodes := m.nodesSnapshot()
	groupByName := make(map[string]int)
	for groupIndex, group := range groups {
		for _, name := range group {
			groupByName[name] = groupIndex
		}
	}

	for _, node := range nodes {
		groupIndex, grouped := groupByName[node.Name]
		blocked := []string{}
		for _, peer := range nodes {
			if peer.Name == node.Name {
				continue
			}
			if !grouped || groupByName[peer.Name] != groupIndex {
				blocked = append(blocked, peer.RaftAddr)
			}
		}
		if err := m.putNetwork(ctx, node, blocked); err != nil {
			return err
		}
	}

	m.mu.Lock()
	m.partitions = cloneGroups(groups)
	m.mu.Unlock()
	m.events.Publish(Event{Type: "partition.applied", Message: "network partition applied", Data: map[string]any{"groups": cloneGroups(groups)}})
	return nil
}

func (m *Manager) ClearPartitions(ctx context.Context) error {
	for _, node := range m.nodesSnapshot() {
		if err := m.putNetwork(ctx, node, nil); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.partitions = nil
	m.mu.Unlock()
	m.events.Publish(Event{Type: "partition.cleared", Message: "network partition cleared"})
	return nil
}

func (m *Manager) SubmitSet(ctx context.Context, key, value string) (map[string]any, error) {
	leader, ok := m.findLeader(ctx)
	if !ok {
		return nil, ErrNoLeader
	}
	return m.postJSON(ctx, leader.ControlAddr, "/kv/set", KVSetRequest{Key: key, Value: value})
}

func (m *Manager) SubmitDelete(ctx context.Context, key string) (map[string]any, error) {
	leader, ok := m.findLeader(ctx)
	if !ok {
		return nil, ErrNoLeader
	}
	return m.postJSON(ctx, leader.ControlAddr, "/kv/delete", KVDeleteRequest{Key: key})
}

func (m *Manager) waitForProcess(name string, process Process) {
	err := process.Wait()
	m.mu.Lock()
	if managed, ok := m.processes[name]; ok && managed.process == process {
		delete(m.processes, name)
	}
	m.mu.Unlock()
	message := "node process exited"
	if err != nil {
		message = err.Error()
	}
	m.events.Publish(Event{Type: "node.exited", Node: name, Message: message})
}

func (m *Manager) findLeader(ctx context.Context) (NodeConfig, bool) {
	for _, node := range m.nodesSnapshot() {
		if _, running := m.managedProcess(node.Name); !running {
			continue
		}
		status, err := m.fetchStatus(ctx, node)
		if err == nil && status.Role == "leader" {
			return node, true
		}
	}
	return NodeConfig{}, false
}

func (m *Manager) fetchStatus(ctx context.Context, node NodeConfig) (*raft.NodeStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+node.ControlAddr+"/status", nil)
	if err != nil {
		return nil, err
	}
	res, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status returned %d", res.StatusCode)
	}
	var status raft.NodeStatus
	if err := json.NewDecoder(res.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (m *Manager) putNetwork(ctx context.Context, node NodeConfig, blocked []string) error {
	_, err := m.putJSON(ctx, node.ControlAddr, "/network", map[string]any{"blockedPeers": blocked})
	return err
}

func (m *Manager) postJSON(ctx context.Context, address, path string, body any) (map[string]any, error) {
	return m.doJSON(ctx, http.MethodPost, address, path, body)
}

func (m *Manager) putJSON(ctx context.Context, address, path string, body any) (map[string]any, error) {
	return m.doJSON(ctx, http.MethodPut, address, path, body)
}

func (m *Manager) doJSON(ctx context.Context, method, address, path string, body any) (map[string]any, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+address+path, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := m.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if res.StatusCode >= 400 {
		return payload, fmt.Errorf("request returned %d", res.StatusCode)
	}
	return payload, nil
}

func (m *Manager) nodesSnapshot() []NodeConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]NodeConfig(nil), m.nodes...)
}

func (m *Manager) managedProcess(name string) (*managedProcess, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	process, ok := m.processes[name]
	return process, ok
}

func (m *Manager) nodeByName(name string) (NodeConfig, bool) {
	for _, node := range m.nodesSnapshot() {
		if node.Name == name {
			return node, true
		}
	}
	return NodeConfig{}, false
}

func (m *Manager) peerRaftAddrs(name string) []string {
	nodes := m.nodesSnapshot()
	peers := make([]string, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.Name != name {
			peers = append(peers, node.RaftAddr)
		}
	}
	return peers
}

func (m *Manager) needsBuild() (bool, error) {
	binary := m.binaryAbsPath()
	info, err := os.Stat(binary)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	newer := false
	for _, root := range []string{"cmd/node", "pkg"} {
		if err := filepath.WalkDir(filepath.Join(m.rootDir, root), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			goInfo, err := os.Stat(path)
			if err != nil {
				return err
			}
			if goInfo.ModTime().After(info.ModTime()) {
				newer = true
			}
			return nil
		}); err != nil {
			return false, err
		}
	}
	return newer, nil
}

func (m *Manager) binaryAbsPath() string {
	if filepath.IsAbs(m.binaryPath) {
		return m.binaryPath
	}
	return filepath.Join(m.rootDir, m.binaryPath)
}

func defaultBinaryPath() string {
	name := "dkvs-node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join("bin", name)
}

func cloneGroups(groups [][]string) [][]string {
	if len(groups) == 0 {
		return nil
	}
	clone := make([][]string, 0, len(groups))
	for _, group := range groups {
		clone = append(clone, append([]string(nil), group...))
	}
	return clone
}

var ErrNoLeader = errors.New("no leader available")
