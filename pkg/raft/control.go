package raft

import (
	"Distributed_Key_Value_Store/pkg/wal"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
)

type NodeStatus struct {
	ID           string           `json:"id"`
	Role         string           `json:"role"`
	CurrentTerm  int32            `json:"currentTerm"`
	Leader       string           `json:"leader"`
	Peers        []string         `json:"peers"`
	CommitIndex  int32            `json:"commitIndex"`
	LastApplied  int32            `json:"lastApplied"`
	LastLogIndex int32            `json:"lastLogIndex"`
	NextIndex    map[string]int32 `json:"nextIndex"`
	MatchIndex   map[string]int32 `json:"matchIndex"`
	BlockedPeers []string         `json:"blockedPeers"`
}

type WriteResult struct {
	Index int32 `json:"index"`
	Term  int32 `json:"term"`
}

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type deleteRequest struct {
	Key string `json:"key"`
}

type networkRequest struct {
	BlockedPeers []string `json:"blockedPeers"`
}

func (n *Node) Status() NodeStatus {
	n.mutex.RLock()
	status := NodeStatus{
		ID:          n.id,
		Role:        n.role,
		CurrentTerm: n.currentTerm,
		Leader:      n.currentLeader,
		Peers:       append([]string(nil), n.peers...),
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		NextIndex:   copyInt32Map(n.nextIndex),
		MatchIndex:  copyInt32Map(n.matchIndex),
	}
	for peer, blocked := range n.blockedPeers {
		if blocked {
			status.BlockedPeers = append(status.BlockedPeers, peer)
		}
	}
	n.mutex.RUnlock()

	status.LastLogIndex = n.log.LastLogIndex()
	return status
}

func (n *Node) SetBlockedPeers(peers []string) {
	blocked := make(map[string]bool, len(peers))
	for _, peer := range peers {
		peer = strings.TrimSpace(peer)
		if peer != "" {
			blocked[peer] = true
		}
	}

	n.mutex.Lock()
	n.blockedPeers = blocked
	n.mutex.Unlock()
}

func (n *Node) SubmitSet(key, value string) (WriteResult, error) {
	if key == "" {
		return WriteResult{}, errors.New("key is required")
	}

	n.mutex.RLock()
	if n.role != "leader" {
		leader := n.currentLeader
		n.mutex.RUnlock()
		return WriteResult{}, &NotLeaderError{Leader: leader}
	}
	term := n.currentTerm
	n.mutex.RUnlock()

	if err := n.log.Append(wal.NewLogEntry("SET", []string{key, value}, int(term))); err != nil {
		return WriteResult{}, err
	}
	return WriteResult{Index: n.log.LastLogIndex(), Term: term}, nil
}

func (n *Node) SubmitDelete(key string) (WriteResult, error) {
	if key == "" {
		return WriteResult{}, errors.New("key is required")
	}

	n.mutex.RLock()
	if n.role != "leader" {
		leader := n.currentLeader
		n.mutex.RUnlock()
		return WriteResult{}, &NotLeaderError{Leader: leader}
	}
	term := n.currentTerm
	n.mutex.RUnlock()

	if err := n.log.Append(wal.NewLogEntry("DELETE", []string{key}, int(term))); err != nil {
		return WriteResult{}, err
	}
	return WriteResult{Index: n.log.LastLogIndex(), Term: term}, nil
}

func (n *Node) ControlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", n.handleStatus)
	mux.HandleFunc("PUT /network", n.handleNetwork)
	mux.HandleFunc("POST /kv/set", n.handleSet)
	mux.HandleFunc("POST /kv/delete", n.handleDelete)
	mux.HandleFunc("GET /kv/", n.handleGet)
	mux.HandleFunc("GET /kv", n.handleDumpKV)
	mux.HandleFunc("GET /log", n.handleDumpLog)
	return mux
}

func (n *Node) StartControlServer(address string) *http.Server {
	server := &http.Server{Addr: address, Handler: n.ControlHandler()}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[control] node=%s server error: %v", n.id, err)
		}
	}()
	return server
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, n.Status())
}

func (n *Node) handleNetwork(w http.ResponseWriter, r *http.Request) {
	var req networkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	n.SetBlockedPeers(req.BlockedPeers)
	writeJSON(w, http.StatusOK, map[string]any{"blockedPeers": req.BlockedPeers})
}

func (n *Node) handleSet(w http.ResponseWriter, r *http.Request) {
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := n.SubmitSet(req.Key, req.Value)
	if err != nil {
		writeRaftWriteError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req deleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := n.SubmitDelete(req.Key)
	if err != nil {
		writeRaftWriteError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	value, ok := n.store.Get(key)
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": value, "ok": ok})
}

func (n *Node) handleDumpKV(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, n.store.All())
}

type logEntryView struct {
	Index  int      `json:"index"`
	Term   int      `json:"term"`
	Method string   `json:"method"`
	Params []string `json:"params"`
}

func (n *Node) handleDumpLog(w http.ResponseWriter, r *http.Request) {
	entries, err := n.log.EntriesFromIndex(1)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	views := make([]logEntryView, 0, len(entries))
	for i, raw := range entries {
		entry, err := wal.ParseToLogEntry(raw)
		if err != nil {
			continue
		}
		views = append(views, logEntryView{
			Index:  i + 1,
			Term:   entry.Term,
			Method: entry.MethodName,
			Params: entry.Params,
		})
	}
	writeJSON(w, http.StatusOK, views)
}

func (n *Node) isPeerBlocked(peer string) bool {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.blockedPeers[peer]
}

func copyInt32Map(src map[string]int32) map[string]int32 {
	dst := make(map[string]int32, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func writeRaftWriteError(w http.ResponseWriter, err error) {
	var notLeader *NotLeaderError
	if errors.As(err, &notLeader) {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "leader": notLeader.Leader})
		return
	}
	writeError(w, http.StatusBadRequest, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}

type NotLeaderError struct {
	Leader string
}

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "node is not leader"
	}
	return "node is not leader; leader is " + e.Leader
}
