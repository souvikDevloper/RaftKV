package raft

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
	"github.com/souvikDevloper/RaftKV/internal/store"
	"google.golang.org/grpc"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Config struct {
	ID            string
	Listen        string
	Peers         map[string]string
	DataDir       string
	SnapshotEvery int64
}

type Node struct {
	mu            sync.Mutex
	cfg           Config
	storage       *store.BoltStore
	server        *grpc.Server
	role          Role
	currentTerm   int64
	votedFor      string
	leader        string
	logEntries    []rpc.LogEntry
	commitIndex   int64
	lastApplied   int64
	snapshotIndex int64
	snapshotTerm  int64
	kv            map[string]string
	nextIndex     map[string]int64
	matchIndex    map[string]int64
	electionReset time.Time
	stopped       chan struct{}
}

func New(cfg Config) (*Node, error) {
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = 25
	}
	bs, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	ps, err := bs.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, storage: bs, role: Follower, currentTerm: ps.CurrentTerm, votedFor: ps.VotedFor,
		logEntries: ps.Log, snapshotIndex: ps.SnapshotIndex, snapshotTerm: ps.SnapshotTerm, kv: ps.Snapshot,
		nextIndex: map[string]int64{}, matchIndex: map[string]int64{}, electionReset: time.Now(), stopped: make(chan struct{})}
	n.commitIndex = n.snapshotIndex
	n.lastApplied = n.snapshotIndex
	for _, e := range n.logEntries {
		if e.Index <= n.commitIndex {
			n.apply(e)
		}
	}
	return n, nil
}

func (n *Node) Start() error {
	lis, err := net.Listen("tcp", n.cfg.Listen)
	if err != nil {
		return err
	}
	n.server = grpc.NewServer(rpc.ServerOptions()...)
	rpc.RegisterPeerServer(n.server, n)
	rpc.RegisterKVServer(n.server, n)
	go func() {
		if err := n.server.Serve(lis); err != nil {
			log.Printf("grpc server stopped: %v", err)
		}
	}()
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applyLoop()
	return nil
}
func (n *Node) Stop() {
	close(n.stopped)
	if n.server != nil {
		n.server.Stop()
	}
	_ = n.storage.Close()
}

func (n *Node) RequestVote(ctx context.Context, req *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return &rpc.RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}, nil
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, "")
	}
	upToDate := req.LastLogTerm > n.lastLogTermLocked() || (req.LastLogTerm == n.lastLogTermLocked() && req.LastLogIndex >= n.lastLogIndexLocked())
	grant := (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate
	if grant {
		n.votedFor = req.CandidateID
		n.electionReset = time.Now()
		_ = n.storage.SaveMeta(n.currentTerm, n.votedFor)
	}
	return &rpc.RequestVoteResponse{Term: n.currentTerm, VoteGranted: grant}, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return &rpc.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndexLocked()}, nil
	}
	if req.Term > n.currentTerm || n.role != Follower {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leader = req.LeaderID
	n.electionReset = time.Now()
	if req.PrevLogIndex > n.snapshotIndex {
		term, ok := n.termAtLocked(req.PrevLogIndex)
		if !ok || term != req.PrevLogTerm {
			return &rpc.AppendEntriesResponse{Term: n.currentTerm, Success: false, MatchIndex: n.lastLogIndexLocked()}, nil
		}
	}
	for _, e := range req.Entries {
		if e.Index <= n.snapshotIndex {
			continue
		}
		pos := int(e.Index - n.snapshotIndex - 1)
		if pos < len(n.logEntries) {
			if n.logEntries[pos].Term != e.Term {
				n.logEntries = n.logEntries[:pos]
				n.logEntries = append(n.logEntries, e)
			}
		} else {
			n.logEntries = append(n.logEntries, e)
		}
	}
	_ = n.storage.SaveLog(n.logEntries)
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, n.lastLogIndexLocked())
	}
	return &rpc.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: n.lastLogIndexLocked()}, nil
}

func (n *Node) InstallSnapshot(ctx context.Context, req *rpc.InstallSnapshotRequest) (*rpc.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.currentTerm {
		return &rpc.InstallSnapshotResponse{Term: n.currentTerm, Success: false}, nil
	}
	if req.Term > n.currentTerm {
		n.becomeFollowerLocked(req.Term, req.LeaderID)
	}
	n.leader = req.LeaderID
	n.electionReset = time.Now()
	n.snapshotIndex = req.LastIncludedIndex
	n.snapshotTerm = req.LastIncludedTerm
	n.kv = cloneMap(req.State)
	kept := []rpc.LogEntry{}
	for _, e := range n.logEntries {
		if e.Index > n.snapshotIndex {
			kept = append(kept, e)
		}
	}
	n.logEntries = kept
	n.commitIndex = max(n.commitIndex, n.snapshotIndex)
	n.lastApplied = max(n.lastApplied, n.snapshotIndex)
	_ = n.storage.SaveSnapshot(n.snapshotIndex, n.snapshotTerm, n.kv, n.logEntries)
	return &rpc.InstallSnapshotResponse{Term: n.currentTerm, Success: true}, nil
}

func (n *Node) Put(ctx context.Context, req *rpc.PutRequest) (*rpc.PutResponse, error) {
	n.mu.Lock()
	if n.role != Leader {
		leader := n.leader
		n.mu.Unlock()
		return &rpc.PutResponse{Ok: false, Leader: leader, Error: "not leader"}, nil
	}
	entry := rpc.LogEntry{Index: n.lastLogIndexLocked() + 1, Term: n.currentTerm, Op: "put", Key: req.Key, Value: req.Value}
	n.logEntries = append(n.logEntries, entry)
	_ = n.storage.SaveLog(n.logEntries)
	term := n.currentTerm
	n.mu.Unlock()
	acks := n.replicateEntry(ctx, entry)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.currentTerm != term || n.role != Leader {
		return &rpc.PutResponse{Ok: false, Leader: n.leader, Error: "lost leadership"}, nil
	}
	if acks >= n.quorumLocked() {
		n.commitIndex = max(n.commitIndex, entry.Index)
		return &rpc.PutResponse{Ok: true, Leader: n.cfg.ID, Index: entry.Index, Term: entry.Term}, nil
	}
	return &rpc.PutResponse{Ok: false, Leader: n.cfg.ID, Error: "quorum not reached"}, nil
}

func (n *Node) Get(ctx context.Context, req *rpc.GetRequest) (*rpc.GetResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader {
		return &rpc.GetResponse{Ok: false, Leader: n.leader, Error: "not leader"}, nil
	}
	v, ok := n.kv[req.Key]
	return &rpc.GetResponse{Ok: ok, Value: v, Leader: n.cfg.ID, Index: n.commitIndex, Term: n.currentTerm}, nil
}

func (n *Node) Status(ctx context.Context, req *rpc.StatusRequest) (*rpc.StatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &rpc.StatusResponse{ID: n.cfg.ID, Role: string(n.role), Term: n.currentTerm, Leader: n.leader, CommitIndex: n.commitIndex, LastApplied: n.lastApplied, LastLogIndex: n.lastLogIndexLocked(), SnapshotIndex: n.snapshotIndex, KV: cloneMap(n.kv)}, nil
}

func (n *Node) replicateEntry(ctx context.Context, entry rpc.LogEntry) int {
	acks := 1
	ch := make(chan bool, len(n.cfg.Peers))
	for id, addr := range n.cfg.Peers {
		id, addr := id, addr
		go func() {
			cctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
			defer cancel()
			ch <- n.replicateTo(cctx, id, addr)
		}()
	}
	deadline := time.After(900 * time.Millisecond)
	for i := 0; i < len(n.cfg.Peers); i++ {
		select {
		case ok := <-ch:
			if ok {
				acks++
			}
		case <-deadline:
			return acks
		}
	}
	return acks
}

func (n *Node) replicateTo(ctx context.Context, id, addr string) bool {
	for attempt := 0; attempt < 3; attempt++ {
		n.mu.Lock()
		next := n.nextIndex[id]
		if next == 0 {
			next = n.lastLogIndexLocked() + 1
		}
		if next <= n.snapshotIndex {
			req := &rpc.InstallSnapshotRequest{Term: n.currentTerm, LeaderID: n.cfg.ID, LastIncludedIndex: n.snapshotIndex, LastIncludedTerm: n.snapshotTerm, State: cloneMap(n.kv)}
			n.mu.Unlock()
			ok := n.sendSnapshot(ctx, addr, req)
			if ok {
				n.mu.Lock()
				n.nextIndex[id] = req.LastIncludedIndex + 1
				n.matchIndex[id] = req.LastIncludedIndex
				n.mu.Unlock()
			}
			continue
		}
		prev := next - 1
		prevTerm, _ := n.termAtLocked(prev)
		entries := []rpc.LogEntry{}
		for _, e := range n.logEntries {
			if e.Index >= next {
				entries = append(entries, e)
			}
		}
		req := &rpc.AppendEntriesRequest{Term: n.currentTerm, LeaderID: n.cfg.ID, PrevLogIndex: prev, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: n.commitIndex}
		n.mu.Unlock()
		resp, err := n.sendAppend(ctx, addr, req)
		if err != nil {
			return false
		}
		n.mu.Lock()
		if resp.Term > n.currentTerm {
			n.becomeFollowerLocked(resp.Term, "")
			n.mu.Unlock()
			return false
		}
		if resp.Success {
			n.matchIndex[id] = resp.MatchIndex
			n.nextIndex[id] = resp.MatchIndex + 1
			n.mu.Unlock()
			return true
		}
		n.nextIndex[id] = max(1, next-1)
		n.mu.Unlock()
	}
	return false
}

func (n *Node) sendAppend(ctx context.Context, addr string, req *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	cctx, cancel := context.WithTimeout(ctx, 450*time.Millisecond)
	defer cancel()
	cc, err := rpc.Dial(cctx, addr)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	return rpc.NewPeerClient(cc).AppendEntries(cctx, req)
}
func (n *Node) sendSnapshot(ctx context.Context, addr string, req *rpc.InstallSnapshotRequest) bool {
	cctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	cc, err := rpc.Dial(cctx, addr)
	if err != nil {
		return false
	}
	defer cc.Close()
	resp, err := rpc.NewPeerClient(cc).InstallSnapshot(cctx, req)
	return err == nil && resp.Success
}

func (n *Node) electionLoop() {
	for {
		select {
		case <-n.stopped:
			return
		default:
		}
		time.Sleep(25 * time.Millisecond)
		n.mu.Lock()
		elapsed := time.Since(n.electionReset)
		timeout := time.Duration(250+rand.Intn(250)) * time.Millisecond
		if n.role != Leader && elapsed > timeout {
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}
func (n *Node) startElectionLocked() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.leader = ""
	n.electionReset = time.Now()
	_ = n.storage.SaveMeta(n.currentTerm, n.votedFor)
	term := n.currentTerm
	lastIndex := n.lastLogIndexLocked()
	lastTerm := n.lastLogTermLocked()
	votes := 1
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range n.cfg.Peers {
		addr := addr
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
			defer cancel()
			cc, err := rpc.Dial(ctx, addr)
			if err != nil {
				return
			}
			defer cc.Close()
			resp, err := rpc.NewPeerClient(cc).RequestVote(ctx, &rpc.RequestVoteRequest{Term: term, CandidateID: n.cfg.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm})
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollowerLocked(resp.Term, "")
			}
			n.mu.Unlock()
			if resp.VoteGranted {
				votes++
			}
		}()
	}
	go func() {
		wg.Wait()
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.role == Candidate && n.currentTerm == term && votes >= n.quorumLocked() {
			n.becomeLeaderLocked()
		}
	}()
}
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leader = n.cfg.ID
	last := n.lastLogIndexLocked()
	for id := range n.cfg.Peers {
		n.nextIndex[id] = last + 1
		n.matchIndex[id] = 0
	}
	log.Printf("%s became leader term=%d", n.cfg.ID, n.currentTerm)
}
func (n *Node) becomeFollowerLocked(term int64, leader string) {
	n.role = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.leader = leader
	n.electionReset = time.Now()
	_ = n.storage.SaveMeta(n.currentTerm, n.votedFor)
}
func (n *Node) heartbeatLoop() {
	for {
		select {
		case <-n.stopped:
			return
		default:
		}
		time.Sleep(90 * time.Millisecond)
		n.mu.Lock()
		isLeader := n.role == Leader
		n.mu.Unlock()
		if isLeader {
			for id, addr := range n.cfg.Peers {
				go n.replicateTo(context.Background(), id, addr)
			}
		}
	}
}
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopped:
			return
		default:
		}
		time.Sleep(15 * time.Millisecond)
		n.mu.Lock()
		for n.lastApplied < n.commitIndex {
			idx := n.lastApplied + 1
			e, ok := n.entryAtLocked(idx)
			if !ok {
				n.lastApplied = idx
				continue
			}
			n.apply(e)
			n.lastApplied = idx
		}
		if n.commitIndex-n.snapshotIndex >= n.cfg.SnapshotEvery {
			n.makeSnapshotLocked()
		}
		n.mu.Unlock()
	}
}
func (n *Node) apply(e rpc.LogEntry) {
	if e.Op == "put" {
		n.kv[e.Key] = e.Value
	}
}
func (n *Node) makeSnapshotLocked() {
	idx := n.commitIndex
	term, _ := n.termAtLocked(idx)
	kept := []rpc.LogEntry{}
	for _, e := range n.logEntries {
		if e.Index > idx {
			kept = append(kept, e)
		}
	}
	n.snapshotIndex = idx
	n.snapshotTerm = term
	n.logEntries = kept
	_ = n.storage.SaveSnapshot(n.snapshotIndex, n.snapshotTerm, n.kv, n.logEntries)
}
func (n *Node) lastLogIndexLocked() int64 {
	if len(n.logEntries) == 0 {
		return n.snapshotIndex
	}
	return n.logEntries[len(n.logEntries)-1].Index
}
func (n *Node) lastLogTermLocked() int64 { t, _ := n.termAtLocked(n.lastLogIndexLocked()); return t }
func (n *Node) termAtLocked(idx int64) (int64, bool) {
	if idx == 0 {
		return 0, true
	}
	if idx == n.snapshotIndex {
		return n.snapshotTerm, true
	}
	if idx < n.snapshotIndex {
		return n.snapshotTerm, true
	}
	for _, e := range n.logEntries {
		if e.Index == idx {
			return e.Term, true
		}
	}
	return 0, false
}
func (n *Node) entryAtLocked(idx int64) (rpc.LogEntry, bool) {
	for _, e := range n.logEntries {
		if e.Index == idx {
			return e, true
		}
	}
	return rpc.LogEntry{}, false
}
func (n *Node) quorumLocked() int { return (len(n.cfg.Peers)+1)/2 + 1 }
func cloneMap(m map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func SortPeers(peers map[string]string) []string {
	xs := []string{}
	for id := range peers {
		xs = append(xs, id)
	}
	sort.Strings(xs)
	return xs
}

var ErrNoLeader = errors.New("no leader found")
