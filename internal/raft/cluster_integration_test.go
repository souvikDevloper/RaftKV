package raft

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
)

func TestClusterElectsReplicatesAndFailsOver(t *testing.T) {
	addresses := []string{reserveAddress(t), reserveAddress(t), reserveAddress(t)}
	nodes := make([]*Node, len(addresses))
	stopped := make([]bool, len(addresses))
	for index, address := range addresses {
		peers := map[string]string{}
		for peerIndex, peerAddress := range addresses {
			if peerIndex != index {
				peers[fmt.Sprintf("n%d", peerIndex+1)] = peerAddress
			}
		}
		node, err := New(Config{ID: fmt.Sprintf("n%d", index+1), Listen: address, Peers: peers,
			DataDir: t.TempDir(), ElectionMin: 120 * time.Millisecond, ElectionMax: 240 * time.Millisecond,
			HeartbeatInterval: 30 * time.Millisecond, GroupCommitWindow: 100 * time.Microsecond, SnapshotEvery: 5})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		nodes[index] = node
	}
	defer func() {
		for index, node := range nodes {
			if !stopped[index] {
				node.Stop()
			}
		}
	}()

	leaderIndex := waitForLeader(t, nodes, stopped, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	put, err := nodes[leaderIndex].Put(ctx, &rpc.PutRequest{Key: "survives", Value: "leader-crash"})
	cancel()
	if err != nil || !put.Ok {
		t.Fatalf("put failed: response=%+v err=%v", put, err)
	}

	nodes[leaderIndex].Stop()
	stopped[leaderIndex] = true
	newLeader := waitForLeader(t, nodes, stopped, 5*time.Second)
	if newLeader == leaderIndex {
		t.Fatal("stopped node cannot remain leader")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	get, err := nodes[newLeader].Get(ctx, &rpc.GetRequest{Key: "survives"})
	cancel()
	if err != nil || !get.Ok || get.Value != "leader-crash" {
		t.Fatalf("committed value lost after failover: response=%+v err=%v", get, err)
	}
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitForLeader(t *testing.T, nodes []*Node, stopped []bool, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader := -1
		for index, node := range nodes {
			if stopped[index] {
				continue
			}
			status, _ := node.Status(context.Background(), &rpc.StatusRequest{})
			if status.Role == string(Leader) {
				if leader != -1 {
					leader = -2
					break
				}
				leader = index
			}
		}
		if leader >= 0 {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			response, _ := nodes[leader].Query(ctx, &rpc.QueryRequest{Op: "get", Key: "readiness-probe"})
			cancel()
			if response.Error == "" {
				return leader
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	for index, node := range nodes {
		if stopped[index] {
			continue
		}
		status, _ := node.Status(context.Background(), &rpc.StatusRequest{})
		t.Logf("node %d final status: %+v", index, status)
	}
	t.Fatal("cluster did not converge to one ready leader")
	return -1
}
