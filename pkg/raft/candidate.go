package raft

import "time"

func (n *Node) startElection() {
	defer n.startElectionTimer()

}

func (n *Node) startElectionTimer() {
	go func() {
		defer n.resetElectionTimer()

		<-n.electionTimer.C

		n.startElection()
	}()

}

func (n *Node) resetElectionTimer() {
	n.electionTimer.Reset(time.Millisecond * 150)
}
