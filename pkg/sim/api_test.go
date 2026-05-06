package sim

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

func TestClusterAPIShape(t *testing.T) {
	manager := NewManager(WithNodes([]NodeConfig{{Name: "node1", RaftAddr: "localhost:5001", ControlAddr: "localhost:6001"}}))
	req := httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	rec := httptest.NewRecorder()

	manager.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var snapshot ClusterSnapshot
	if err := json.NewDecoder(rec.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].Name != "node1" {
		t.Fatalf("unexpected cluster snapshot: %+v", snapshot)
	}
}

func TestWebSocketReceivesClusterSnapshot(t *testing.T) {
	manager := NewManager(WithNodes([]NodeConfig{{Name: "node1", RaftAddr: "localhost:5001", ControlAddr: "localhost:6001"}}))
	server := httptest.NewServer(manager.Handler())
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/api/ws"
	conn, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	var envelope Envelope
	if err := wsjson.Read(context.Background(), conn, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Type != "cluster.snapshot" {
		t.Fatalf("expected cluster.snapshot envelope, got %q", envelope.Type)
	}
}
