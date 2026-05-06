package raft

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNodeStatusCopiesMaps(t *testing.T) {
	n := newTestNode(t)
	n.nextIndex = map[string]int32{"p1": 2}
	n.matchIndex = map[string]int32{"p1": 1}

	status := n.Status()
	status.NextIndex["p1"] = 99
	status.MatchIndex["p1"] = 99

	next := n.Status()
	if got := next.NextIndex["p1"]; got != 2 {
		t.Errorf("expected copied nextIndex to leave node state at 2, got %d", got)
	}
	if got := next.MatchIndex["p1"]; got != 1 {
		t.Errorf("expected copied matchIndex to leave node state at 1, got %d", got)
	}
}

func TestNetworkPolicyBlocksPeers(t *testing.T) {
	n := newTestNode(t)

	n.SetBlockedPeers([]string{"p1", " p2 ", ""})

	if !n.isPeerBlocked("p1") {
		t.Error("expected p1 to be blocked")
	}
	if !n.isPeerBlocked("p2") {
		t.Error("expected p2 to be blocked")
	}
	if n.isPeerBlocked("p3") {
		t.Error("expected p3 to be allowed")
	}
}

func TestControlWriteRejectsNonLeader(t *testing.T) {
	n := newTestNode(t)
	req := httptest.NewRequest(http.MethodPost, "/kv/set", bytes.NewBufferString(`{"key":"k","value":"v"}`))
	rec := httptest.NewRecorder()

	n.ControlHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := n.log.LastLogIndex(); got != 0 {
		t.Errorf("expected non-leader write not to append, got log index %d", got)
	}
}

func TestControlWriteAppendsOnLeader(t *testing.T) {
	n := newTestNode(t)
	n.role = "leader"
	n.currentTerm = 2
	req := httptest.NewRequest(http.MethodPost, "/kv/set", bytes.NewBufferString(`{"key":"k","value":"v"}`))
	rec := httptest.NewRecorder()

	n.ControlHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := n.log.LastLogIndex(); got != 1 {
		t.Errorf("expected leader write to append at index 1, got %d", got)
	}
}
