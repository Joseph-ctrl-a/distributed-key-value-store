package raft

import (
	"Distributed_Key_Value_Store/pkg/transport"
	"context"
	"time"
)

type heartBeatResult struct {
	count int
	err   error
	term  int32
}

// sendHeartBeats will send empty AppendEntriesRequests (HeartBeats) to all clients in the cluster
func (n *Node) sendHeartBeats(channel chan *transport.AppendEntriesResponse, c context.Context, req *transport.AppendEntriesRequest) {
	// Make a deepCopy of clients so you don't need to read from the node when sending heartbeats
	clients := n.DeepCopyClients()
	for _, client := range clients {
		go func(client transport.RaftClient) {
			res, err := client.AppendEntries(c, req)

			// Send the response value into the channel
			if err != nil {
				channel <- nil
			} else {
				channel <- res
			}
		}(client)
	}
}

// handleHeartBeatTimer handles when to start / stop sending heartbeats & what to do with them
func (n *Node) handleHeartBeatTimer() error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mutex.RLock()
		channel := make(chan *transport.AppendEntriesResponse, len(n.peers))
		if n.role != "leader" {
			n.mutex.RUnlock()
			break
		}
		n.mutex.RUnlock()
		<-ticker.C
		n.mutex.Lock()
		prevLogTerm, err := n.log.LastLogTerm()
		if err != nil {
			n.mutex.RUnlock()
			return err
		}

		req := &transport.AppendEntriesRequest{
			Term:         n.currentTerm,
			LeaderId:     n.id,
			Entries:      []string{},
			PrevLogIndex: n.log.LastLogIndex(),
			LeaderCommit: n.commitIndex,
			PrevLogTerm:  prevLogTerm,
		}

		n.mutex.RUnlock()

		ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
		n.sendHeartBeats(channel, ctx, req)
		responses := n.collectHeartBeatResponses(channel)
		tally := n.tallyHeartBeats(responses)

		if tally.err != nil {

			cancel()
			return tally.err
		}
		cancel()
	}
	return nil
}

func (n *Node) tallyHeartBeats(responses []*transport.AppendEntriesResponse) (r heartBeatResult) {
	for _, res := range responses {
		if res == nil {
			continue
		}
		if res.Term > n.currentTerm {
			n.mutex.Lock()
			r.err = n.stepDown(res.Term)
			r.term = res.Term
			n.mutex.Unlock()
			return
		}
	}
	return r
}

// Collect all responses from a heartbeat
func (n *Node) collectHeartBeatResponses(channel chan *transport.AppendEntriesResponse) []*transport.AppendEntriesResponse {

	n.mutex.RLock()
	responses := make([]*transport.AppendEntriesResponse, 0, len(n.clients))
	n.mutex.RUnlock()
	timeout := time.After(75 * time.Millisecond)

	for {
		select {
		case res := <-channel:
			responses = append(responses, res)
		case <-timeout:
			return responses
		}
	}
}

// DeepCopyClients makes a deep copy of n.clients

func (n *Node) DeepCopyClients() []transport.RaftClient {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	clients := make([]transport.RaftClient, 0, len(n.clients))

	for _, client := range n.clients {
		clients = append(clients, client)
	}
	return clients
}
