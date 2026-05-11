package raft

import "testing"

func TestQuorumSize(t *testing.T) {
	n := &Node{cfg: Config{Peers: map[string]string{"n2": "x", "n3": "x", "n4": "x", "n5": "x"}}}
	if n.quorumLocked() != 3 {
		t.Fatalf("expected 3-node quorum in 5-node cluster, got %d", n.quorumLocked())
	}
}
