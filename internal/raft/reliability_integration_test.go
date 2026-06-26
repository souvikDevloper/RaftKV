package raft

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
)

func TestQuorumLossRejectsWrites(t *testing.T) {
	nodes, _, stopped := startTestCluster(t, 50)
	defer stopTestCluster(nodes, stopped)
	leader := waitForLeader(t, nodes, stopped, 5*time.Second)

	for index := range nodes {
		if index != leader {
			nodes[index].Stop()
			stopped[index] = true
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	response, err := nodes[leader].Put(ctx, &rpc.PutRequest{Key: "must-not-commit", Value: "without-quorum"})
	cancel()
	if err != nil {
		t.Fatalf("Put returned transport error instead of Raft result: %v", err)
	}
	if response.Ok || !strings.Contains(response.Error, ErrQuorum.Error()) {
		t.Fatalf("write succeeded without quorum: %+v", response)
	}
}

func TestStoppedFollowerCatchesUpThroughSnapshot(t *testing.T) {
	nodes, configs, stopped := startTestCluster(t, 3)
	defer stopTestCluster(nodes, stopped)
	leader := waitForLeader(t, nodes, stopped, 5*time.Second)
	follower := (leader + 1) % len(nodes)
	nodes[follower].Stop()
	stopped[follower] = true

	for index := 0; index < 12; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		response, err := nodes[leader].Put(ctx, &rpc.PutRequest{Key: fmt.Sprintf("key-%02d", index), Value: fmt.Sprintf("value-%02d", index)})
		cancel()
		if err != nil || !response.Ok {
			t.Fatalf("write %d failed: response=%+v err=%v", index, response, err)
		}
	}

	nodes[leader].mu.Lock()
	leaderSnapshot := nodes[leader].snapshotIndex
	nodes[leader].mu.Unlock()
	if leaderSnapshot == 0 {
		t.Fatal("leader did not compact its committed log")
	}

	restarted, err := New(configs[follower])
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	nodes[follower], stopped[follower] = restarted, false

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		restarted.mu.Lock()
		caughtUp := restarted.snapshotIndex >= leaderSnapshot && restarted.kv["key-11"] == "value-11" && restarted.lastApplied == restarted.commitIndex
		restarted.mu.Unlock()
		if caughtUp {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	status, _ := restarted.Status(context.Background(), &rpc.StatusRequest{})
	t.Fatalf("restarted follower did not catch up through snapshot: %+v", status)
}

func startTestCluster(t *testing.T, snapshotEvery int64) ([]*Node, []Config, []bool) {
	t.Helper()
	addresses := []string{reserveAddress(t), reserveAddress(t), reserveAddress(t)}
	nodes := make([]*Node, len(addresses))
	configs := make([]Config, len(addresses))
	stopped := make([]bool, len(addresses))
	for index, address := range addresses {
		peers := map[string]string{}
		for peerIndex, peerAddress := range addresses {
			if peerIndex != index {
				peers[fmt.Sprintf("n%d", peerIndex+1)] = peerAddress
			}
		}
		configs[index] = Config{
			ID: fmt.Sprintf("n%d", index+1), Listen: address, Peers: peers, DataDir: t.TempDir(),
			ElectionMin: 200 * time.Millisecond, ElectionMax: 400 * time.Millisecond,
			HeartbeatInterval: 35 * time.Millisecond, GroupCommitWindow: 100 * time.Microsecond,
			SnapshotEvery: snapshotEvery,
		}
		node, err := New(configs[index])
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		nodes[index] = node
	}
	return nodes, configs, stopped
}

func stopTestCluster(nodes []*Node, stopped []bool) {
	for index, node := range nodes {
		if node != nil && !stopped[index] {
			node.Stop()
		}
	}
}
