package sim

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"nhooyr.io/websocket"
)

func (m *Manager) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/cluster", m.handleCluster)
	mux.HandleFunc("POST /api/cluster/start", m.handleClusterStart)
	mux.HandleFunc("POST /api/cluster/stop", m.handleClusterStop)
	mux.HandleFunc("POST /api/nodes/{id}/start", m.handleNodeStart)
	mux.HandleFunc("POST /api/nodes/{id}/stop", m.handleNodeStop)
	mux.HandleFunc("GET /api/nodes/{id}/kv", m.handleNodeKV)
	mux.HandleFunc("GET /api/nodes/{id}/log", m.handleNodeLog)
	mux.HandleFunc("POST /api/partitions", m.handlePartitions)
	mux.HandleFunc("DELETE /api/partitions", m.handleClearPartitions)
	mux.HandleFunc("POST /api/kv/set", m.handleSet)
	mux.HandleFunc("POST /api/kv/delete", m.handleDelete)
	mux.HandleFunc("GET /api/events", m.handleEvents)
	mux.HandleFunc("GET /api/ws", m.handleWebSocket)
	return withLocalCORS(mux)
}

func withLocalCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *Manager) handleCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, m.Snapshot(r.Context()))
}

func (m *Manager) handleClusterStart(w http.ResponseWriter, r *http.Request) {
	if err := m.StartCluster(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m.Snapshot(r.Context()))
}

func (m *Manager) handleClusterStop(w http.ResponseWriter, r *http.Request) {
	if err := m.StopCluster(); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m.Snapshot(r.Context()))
}

func (m *Manager) handleNodeStart(w http.ResponseWriter, r *http.Request) {
	if err := m.StartNode(r.Context(), r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m.Snapshot(r.Context()))
}

func (m *Manager) handleNodeStop(w http.ResponseWriter, r *http.Request) {
	if err := m.StopNode(r.PathValue("id")); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m.Snapshot(r.Context()))
}

func (m *Manager) handlePartitions(w http.ResponseWriter, r *http.Request) {
	var req PartitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := m.ApplyPartitions(r.Context(), req.Groups); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m.Snapshot(r.Context()))
}

func (m *Manager) handleClearPartitions(w http.ResponseWriter, r *http.Request) {
	if err := m.ClearPartitions(r.Context()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, m.Snapshot(r.Context()))
}

func (m *Manager) handleSet(w http.ResponseWriter, r *http.Request) {
	var req KVSetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := m.SubmitSet(r.Context(), req.Key, req.Value)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, ErrNoLeader) {
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (m *Manager) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req KVDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := m.SubmitDelete(r.Context(), req.Key)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, ErrNoLeader) {
			status = http.StatusConflict
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (m *Manager) handleNodeKV(w http.ResponseWriter, r *http.Request) {
	m.proxyNodePath(w, r, "/kv")
}

func (m *Manager) handleNodeLog(w http.ResponseWriter, r *http.Request) {
	m.proxyNodePath(w, r, "/log")
}

func (m *Manager) proxyNodePath(w http.ResponseWriter, r *http.Request, path string) {
	id := r.PathValue("id")
	node, ok := m.nodeByName(id)
	if !ok {
		writeError(w, http.StatusNotFound, "node not found: "+id)
		return
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, "http://"+node.ControlAddr+path, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	res, err := m.httpClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer res.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.StatusCode)
	_, _ = io.Copy(w, res.Body)
}

func (m *Manager) handleEvents(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, m.events.Snapshot())
}

func (m *Manager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "closing")

	ctx := r.Context()
	ctx = conn.CloseRead(ctx)
	if err := writeWebSocket(ctx, conn, Envelope{Type: "cluster.snapshot", Data: m.Snapshot(ctx)}); err != nil {
		return
	}

	events, unsubscribe := m.events.Subscribe()
	defer unsubscribe()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := writeWebSocket(ctx, conn, Envelope{Type: "event", Data: event}); err != nil {
				return
			}
		case <-ticker.C:
			if err := writeWebSocket(ctx, conn, Envelope{Type: "cluster.snapshot", Data: m.Snapshot(ctx)}); err != nil {
				return
			}
		}
	}
}

func writeWebSocket(ctx context.Context, conn *websocket.Conn, value any) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return wsjsonWrite(ctx, conn, value)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
