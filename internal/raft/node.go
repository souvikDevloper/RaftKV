package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/metrics"
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

var (
	ErrNoLeader       = errors.New("no leader found")
	ErrNotLeader      = errors.New("not leader")
	ErrQuorum         = errors.New("quorum not reached")
	ErrLeaderNotReady = errors.New("leader has not committed an entry in its current term")
)

type Config struct {
	ID                string
	Listen            string
	Peers             map[string]string
	DataDir           string
	SnapshotEvery     int64
	GroupCommitMax    int
	GroupCommitWindow time.Duration
	ElectionMin       time.Duration
	ElectionMax       time.Duration
	HeartbeatInterval time.Duration
	ReadLease         time.Duration
	Metrics           *metrics.Registry
}

type proposal struct {
	term   int64
	op     string
	key    string
	value  string
	result chan proposalResult
}
type proposalResult struct {
	index int64
	term  int64
	err   error
}

type Node struct {
	mu                sync.RWMutex
	readMu            sync.Mutex
	connMu            sync.Mutex
	stopOnce          sync.Once
	cfg               Config
	storage           *store.BoltStore
	server            *grpc.Server
	listener          net.Listener
	role              Role
	currentTerm       int64
	votedFor          string
	leader            string
	logEntries        []rpc.LogEntry
	commitIndex       int64
	lastApplied       int64
	snapshotIndex     int64
	snapshotTerm      int64
	kv                map[string]string
	hashValues        map[string][]string
	nextIndex         map[string]int64
	matchIndex        map[string]int64
	leaderReadyTerm   int64
	lastQuorumContact time.Time
	electionDeadline  time.Time
	rng               *rand.Rand
	peerConns         map[string]*grpc.ClientConn
	peerClients       map[string]*rpc.PeerClient
	proposals         chan *proposal
	stopped           chan struct{}
}

func New(cfg Config) (*Node, error) {
	applyDefaults(&cfg)
	storage, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	persisted, err := storage.Load()
	if err != nil {
		_ = storage.Close()
		return nil, err
	}
	node := &Node{
		cfg: cfg, storage: storage, role: Follower,
		currentTerm: persisted.CurrentTerm, votedFor: persisted.VotedFor,
		logEntries:    append([]rpc.LogEntry(nil), persisted.Log...),
		snapshotIndex: persisted.SnapshotIndex, snapshotTerm: persisted.SnapshotTerm,
		kv: cloneMap(persisted.Snapshot), hashValues: map[string][]string{}, nextIndex: map[string]int64{}, matchIndex: map[string]int64{},
		rng:       rand.New(rand.NewSource(randomSeed(cfg.ID))),
		peerConns: map[string]*grpc.ClientConn{}, peerClients: map[string]*rpc.PeerClient{},
		proposals: make(chan *proposal, 4096), stopped: make(chan struct{}),
	}
	last := node.lastLogIndexLocked()
	node.commitIndex = min(max(persisted.CommitIndex, node.snapshotIndex), last)
	node.lastApplied = node.snapshotIndex
	node.applyCommittedLocked()
	node.resetElectionDeadlineLocked()
	return node, nil
}

func applyDefaults(cfg *Config) {
	if cfg.SnapshotEvery <= 0 {
		cfg.SnapshotEvery = 10_000
	}
	if cfg.GroupCommitMax <= 0 {
		cfg.GroupCommitMax = 256
	}
	if cfg.GroupCommitWindow <= 0 {
		cfg.GroupCommitWindow = time.Millisecond
	}
	if cfg.ElectionMin <= 0 {
		// Local durable writes can briefly stall a process while the filesystem
		// flushes. A production-like timeout prevents those stalls from causing
		// needless elections under concurrent benchmark load.
		cfg.ElectionMin = 1500 * time.Millisecond
	}
	if cfg.ElectionMax <= cfg.ElectionMin {
		cfg.ElectionMax = cfg.ElectionMin + 1500*time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 150 * time.Millisecond
	}
	if cfg.ReadLease <= 0 {
		cfg.ReadLease = minDuration(500*time.Millisecond, cfg.ElectionMin/3)
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
}

func (n *Node) Start() error {
	listener, err := net.Listen("tcp", n.cfg.Listen)
	if err != nil {
		return err
	}
	n.listener = listener
	n.server = grpc.NewServer(rpc.ServerOptions()...)
	rpc.RegisterPeerServer(n.server, n)
	rpc.RegisterKVServer(n.server, n)
	go func() {
		if err := n.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("node %s gRPC server stopped: %v", n.cfg.ID, err)
		}
	}()
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.proposalLoop()
	return nil
}

func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stopped)
		if n.server != nil {
			n.server.Stop()
		}
		n.connMu.Lock()
		for _, connection := range n.peerConns {
			_ = connection.Close()
		}
		n.connMu.Unlock()
		_ = n.storage.Close()
	})
}

func (n *Node) Metrics() *metrics.Registry { return n.cfg.Metrics }

func (n *Node) RequestVote(_ context.Context, request *rpc.RequestVoteRequest) (*rpc.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.Term < n.currentTerm {
		return &rpc.RequestVoteResponse{Term: n.currentTerm}, nil
	}
	if request.Term > n.currentTerm {
		if err := n.becomeFollowerLocked(request.Term, ""); err != nil {
			return nil, err
		}
	}
	upToDate := request.LastLogTerm > n.lastLogTermLocked() ||
		(request.LastLogTerm == n.lastLogTermLocked() && request.LastLogIndex >= n.lastLogIndexLocked())
	grant := (n.votedFor == "" || n.votedFor == request.CandidateID) && upToDate
	if grant {
		n.votedFor = request.CandidateID
		n.resetElectionDeadlineLocked()
		if err := n.storage.SaveMeta(n.currentTerm, n.votedFor); err != nil {
			return nil, err
		}
	}
	return &rpc.RequestVoteResponse{Term: n.currentTerm, VoteGranted: grant}, nil
}

func (n *Node) AppendEntries(_ context.Context, request *rpc.AppendEntriesRequest) (*rpc.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.Term < n.currentTerm {
		return &rpc.AppendEntriesResponse{Term: n.currentTerm, MatchIndex: n.lastLogIndexLocked(), ConflictIndex: n.lastLogIndexLocked() + 1}, nil
	}
	if request.Term > n.currentTerm || n.role != Follower {
		if err := n.becomeFollowerLocked(request.Term, request.LeaderID); err != nil {
			return nil, err
		}
	}
	n.leader = request.LeaderID
	n.resetElectionDeadlineLocked()
	if request.PrevLogIndex < n.snapshotIndex {
		return &rpc.AppendEntriesResponse{Term: n.currentTerm, ConflictIndex: n.snapshotIndex + 1}, nil
	}
	if term, ok := n.termAtLocked(request.PrevLogIndex); !ok || term != request.PrevLogTerm {
		conflict := min(request.PrevLogIndex, n.lastLogIndexLocked()+1)
		return &rpc.AppendEntriesResponse{Term: n.currentTerm, MatchIndex: n.lastLogIndexLocked(), ConflictIndex: max(1, conflict)}, nil
	}
	oldLastIndex := n.lastLogIndexLocked()
	changedAt := int64(0)
	var suffix []rpc.LogEntry
	for offset, entry := range request.Entries {
		if entry.Index <= n.snapshotIndex {
			continue
		}
		existingTerm, exists := n.termAtLocked(entry.Index)
		if !exists || existingTerm != entry.Term {
			changedAt = entry.Index
			suffix = append([]rpc.LogEntry(nil), request.Entries[offset:]...)
			break
		}
	}
	if changedAt > 0 {
		position := int(changedAt - n.snapshotIndex - 1)
		if position < 0 {
			position = 0
		}
		if position < len(n.logEntries) {
			n.logEntries = n.logEntries[:position]
		}
		n.logEntries = append(n.logEntries, suffix...)
		var err error
		if changedAt == oldLastIndex+1 {
			err = n.storage.AppendLog(suffix)
		} else {
			err = n.storage.ReplaceSuffix(changedAt, suffix)
		}
		if err != nil {
			return nil, err
		}
	}
	if request.LeaderCommit > n.commitIndex {
		n.commitIndex = min(request.LeaderCommit, n.lastLogIndexLocked())
		n.applyCommittedLocked()
		if err := n.maybeSnapshotLocked(); err != nil {
			return nil, err
		}
	}
	return &rpc.AppendEntriesResponse{Term: n.currentTerm, Success: true, MatchIndex: n.lastLogIndexLocked()}, nil
}

func (n *Node) InstallSnapshot(_ context.Context, request *rpc.InstallSnapshotRequest) (*rpc.InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.Term < n.currentTerm {
		return &rpc.InstallSnapshotResponse{Term: n.currentTerm}, nil
	}
	if request.Term > n.currentTerm {
		if err := n.becomeFollowerLocked(request.Term, request.LeaderID); err != nil {
			return nil, err
		}
	}
	n.leader = request.LeaderID
	n.resetElectionDeadlineLocked()
	if request.LastIncludedIndex <= n.snapshotIndex {
		return &rpc.InstallSnapshotResponse{Term: n.currentTerm, Success: true}, nil
	}
	n.snapshotIndex, n.snapshotTerm = request.LastIncludedIndex, request.LastIncludedTerm
	n.kv = cloneMap(request.State)
	n.hashValues = map[string][]string{}
	kept := n.logEntries[:0]
	for _, entry := range n.logEntries {
		if entry.Index > n.snapshotIndex {
			kept = append(kept, entry)
		}
	}
	n.logEntries = kept
	n.commitIndex = max(n.commitIndex, n.snapshotIndex)
	n.lastApplied = max(n.lastApplied, n.snapshotIndex)
	if err := n.storage.SaveSnapshot(n.snapshotIndex, n.snapshotTerm, n.commitIndex, n.kv, n.logEntries); err != nil {
		return nil, err
	}
	return &rpc.InstallSnapshotResponse{Term: n.currentTerm, Success: true}, nil
}

func (n *Node) Put(ctx context.Context, request *rpc.PutRequest) (*rpc.PutResponse, error) {
	started := time.Now()
	defer func() { n.cfg.Metrics.Observe("client_write", time.Since(started)) }()
	result, leader, err := n.submit(ctx, "put", request.Key, request.Value)
	if err != nil {
		return &rpc.PutResponse{Leader: leader, Error: err.Error()}, nil
	}
	return &rpc.PutResponse{Ok: true, Leader: n.cfg.ID, Index: result.index, Term: result.term}, nil
}

func (n *Node) Get(ctx context.Context, request *rpc.GetRequest) (*rpc.GetResponse, error) {
	started := time.Now()
	defer func() { n.cfg.Metrics.Observe("client_read", time.Since(started)) }()
	response, err := n.Query(ctx, &rpc.QueryRequest{Op: "get", Key: request.Key})
	if err != nil {
		return nil, err
	}
	value := ""
	if len(response.Values) > 0 {
		value = response.Values[0]
	}
	return &rpc.GetResponse{Ok: response.Ok, Value: value, Leader: response.Leader, Error: response.Error, Index: response.Index, Term: response.Term}, nil
}

func (n *Node) Execute(ctx context.Context, request *rpc.ExecuteRequest) (*rpc.ExecuteResponse, error) {
	started := time.Now()
	defer func() { n.cfg.Metrics.Observe("client_write", time.Since(started)) }()
	result, leader, err := n.submit(ctx, strings.ToLower(request.Op), request.Key, request.Value)
	if err != nil {
		return &rpc.ExecuteResponse{Leader: leader, Error: err.Error()}, nil
	}
	return &rpc.ExecuteResponse{Ok: true, Leader: n.cfg.ID, Index: result.index, Term: result.term, Affected: 1}, nil
}

func (n *Node) Query(ctx context.Context, request *rpc.QueryRequest) (*rpc.QueryResponse, error) {
	started := time.Now()
	defer func() { n.cfg.Metrics.Observe("client_read", time.Since(started)) }()
	term, index, leader, err := n.confirmLeadership(ctx)
	if err != nil {
		return &rpc.QueryResponse{Leader: leader, Error: err.Error()}, nil
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	response := &rpc.QueryResponse{Ok: true, Leader: n.cfg.ID, Term: term, Index: index}
	switch strings.ToLower(request.Op) {
	case "get":
		value, ok := n.kv[request.Key]
		response.Ok = ok
		if ok {
			response.Values = []string{value}
		}
	case "hgetall":
		values := flattenHash(decodeStringMap(n.kv[request.Key]))
		response.Values = append(response.Values, values...)
		response.Ok = len(values) > 0
	case "hmget":
		fields := decodeStringMap(n.kv[request.Key])
		for _, field := range request.Args {
			response.Values = append(response.Values, fields[field])
		}
	case "zrangebyscore":
		response.Values = n.zRangeLocked(request.Key, request.Args)
	default:
		response.Ok = false
		response.Error = "unsupported query: " + request.Op
	}
	return response, nil
}

func (n *Node) Status(_ context.Context, _ *rpc.StatusRequest) (*rpc.StatusResponse, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	n.updateMetricsLocked()
	return &rpc.StatusResponse{ID: n.cfg.ID, Role: string(n.role), Term: n.currentTerm, Leader: n.leader,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied, LastLogIndex: n.lastLogIndexLocked(),
		SnapshotIndex: n.snapshotIndex, MemberCount: len(n.cfg.Peers) + 1, Quorum: n.quorumLocked(),
		KeyCount: len(visibleKV(n.kv)), Ready: n.lastApplied == n.commitIndex}, nil
}

func (n *Node) updateMetricsLocked() {
	role := map[Role]float64{Follower: 0, Candidate: 1, Leader: 2}[n.role]
	n.cfg.Metrics.SetGauge("raft_role", role)
	n.cfg.Metrics.SetGauge("current_term", float64(n.currentTerm))
	n.cfg.Metrics.SetGauge("commit_index", float64(n.commitIndex))
	n.cfg.Metrics.SetGauge("last_applied", float64(n.lastApplied))
	n.cfg.Metrics.SetGauge("apply_lag", float64(max(0, n.commitIndex-n.lastApplied)))
	n.cfg.Metrics.SetGauge("snapshot_index", float64(n.snapshotIndex))
	n.cfg.Metrics.SetGauge("member_count", float64(len(n.cfg.Peers)+1))
	n.cfg.Metrics.SetGauge("quorum_size", float64(n.quorumLocked()))
}

func (n *Node) submit(ctx context.Context, op, key, value string) (proposalResult, string, error) {
	n.mu.RLock()
	term, role, leader := n.currentTerm, n.role, n.leader
	n.mu.RUnlock()
	if role != Leader {
		return proposalResult{}, leader, ErrNotLeader
	}
	request := &proposal{term: term, op: op, key: key, value: value, result: make(chan proposalResult, 1)}
	select {
	case n.proposals <- request:
	case <-ctx.Done():
		return proposalResult{}, leader, ctx.Err()
	case <-n.stopped:
		return proposalResult{}, leader, errors.New("node stopped")
	}
	select {
	case result := <-request.result:
		return result, n.cfg.ID, result.err
	case <-ctx.Done():
		return proposalResult{}, n.cfg.ID, ctx.Err()
	case <-n.stopped:
		return proposalResult{}, leader, errors.New("node stopped")
	}
}

func (n *Node) proposalLoop() {
	for {
		select {
		case <-n.stopped:
			return
		case first := <-n.proposals:
			batch := []*proposal{first}
			timer := time.NewTimer(n.cfg.GroupCommitWindow)
		collect:
			for len(batch) < n.cfg.GroupCommitMax {
				select {
				case next := <-n.proposals:
					batch = append(batch, next)
				case <-timer.C:
					break collect
				case <-n.stopped:
					timer.Stop()
					n.failBatch(batch, errors.New("node stopped"))
					return
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			n.processBatch(batch)
		}
	}
}

func (n *Node) processBatch(batch []*proposal) {
	started := time.Now()
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		n.failBatch(batch, ErrNotLeader)
		return
	}
	term := n.currentTerm
	for _, item := range batch {
		if item.term != term {
			n.mu.Unlock()
			n.failBatch(batch, ErrNotLeader)
			return
		}
	}
	entries := make([]rpc.LogEntry, 0, len(batch))
	for _, item := range batch {
		entry := rpc.LogEntry{Index: n.lastLogIndexLocked() + 1, Term: term, Op: item.op, Key: item.key, Value: item.value}
		n.logEntries = append(n.logEntries, entry)
		entries = append(entries, entry)
	}
	if err := n.storage.AppendLog(entries); err != nil {
		n.mu.Unlock()
		n.failBatch(batch, fmt.Errorf("persist WAL: %w", err))
		return
	}
	target := entries[len(entries)-1].Index
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	acks := n.replicateQuorum(ctx, target)
	cancel()
	if acks < n.quorum() {
		n.failBatch(batch, ErrQuorum)
		return
	}
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		n.failBatch(batch, ErrNotLeader)
		return
	}
	n.commitIndex = max(n.commitIndex, target)
	n.applyCommittedLocked()
	n.leaderReadyTerm = term
	n.lastQuorumContact = time.Now()
	if err := n.maybeSnapshotLocked(); err != nil {
		n.mu.Unlock()
		n.failBatch(batch, err)
		return
	}
	n.mu.Unlock()
	n.cfg.Metrics.Observe("raft_commit", time.Since(started))
	for index, item := range batch {
		item.result <- proposalResult{index: entries[index].Index, term: term}
	}
}

func (n *Node) failBatch(batch []*proposal, err error) {
	for _, item := range batch {
		item.result <- proposalResult{err: err}
	}
}

func (n *Node) replicateQuorum(ctx context.Context, target int64) int {
	if n.quorum() == 1 {
		return 1
	}
	results := make(chan bool, len(n.cfg.Peers))
	for id, address := range n.cfg.Peers {
		go func(id, address string) { results <- n.replicateTo(ctx, id, address, target) }(id, address)
	}
	acks := 1
	for range n.cfg.Peers {
		select {
		case ok := <-results:
			if ok {
				acks++
			}
			if acks >= n.quorum() {
				return acks
			}
		case <-ctx.Done():
			return acks
		}
	}
	return acks
}

// replicateAll deliberately waits for every peer (or the context deadline).
// Returning as soon as a quorum responds is correct for committing a write,
// but it is wrong for periodic heartbeats: cancelling the slower requests can
// starve the same followers until they time out and disrupt a healthy leader.
func (n *Node) replicateAll(ctx context.Context, target int64) int {
	if n.quorum() == 1 {
		return 1
	}
	results := make(chan bool, len(n.cfg.Peers))
	for id, address := range n.cfg.Peers {
		go func(id, address string) { results <- n.replicateTo(ctx, id, address, target) }(id, address)
	}
	acks := 1
	for range n.cfg.Peers {
		select {
		case ok := <-results:
			if ok {
				acks++
			}
		case <-ctx.Done():
			return acks
		}
	}
	return acks
}

func (n *Node) replicateTo(ctx context.Context, id, address string, target int64) bool {
	for attempt := 0; attempt < 64; attempt++ {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return false
		}
		next := n.nextIndex[id]
		if next == 0 {
			next = n.lastLogIndexLocked() + 1
		}
		if next <= n.snapshotIndex {
			request := &rpc.InstallSnapshotRequest{Term: n.currentTerm, LeaderID: n.cfg.ID, LastIncludedIndex: n.snapshotIndex, LastIncludedTerm: n.snapshotTerm, State: cloneMap(n.kv)}
			n.mu.Unlock()
			client, err := n.peerClient(ctx, id, address)
			if err != nil {
				return false
			}
			callCtx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
			response, err := client.InstallSnapshot(callCtx, request)
			cancel()
			if err != nil || !response.Success {
				return false
			}
			n.mu.Lock()
			n.nextIndex[id] = request.LastIncludedIndex + 1
			n.matchIndex[id] = request.LastIncludedIndex
			n.mu.Unlock()
			continue
		}
		previous := next - 1
		previousTerm, _ := n.termAtLocked(previous)
		start := int(next - n.snapshotIndex - 1)
		if start < 0 {
			start = 0
		}
		if start > len(n.logEntries) {
			start = len(n.logEntries)
		}
		entries := append([]rpc.LogEntry(nil), n.logEntries[start:]...)
		term, commit := n.currentTerm, n.commitIndex
		n.mu.Unlock()
		client, err := n.peerClient(ctx, id, address)
		if err != nil {
			return false
		}
		callCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		response, err := client.AppendEntries(callCtx, &rpc.AppendEntriesRequest{Term: term, LeaderID: n.cfg.ID, PrevLogIndex: previous, PrevLogTerm: previousTerm, Entries: entries, LeaderCommit: commit})
		cancel()
		if err != nil {
			return false
		}
		n.mu.Lock()
		if response.Term > n.currentTerm {
			_ = n.becomeFollowerLocked(response.Term, "")
			n.mu.Unlock()
			return false
		}
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return false
		}
		if response.Success {
			n.matchIndex[id] = response.MatchIndex
			n.nextIndex[id] = response.MatchIndex + 1
			matched := response.MatchIndex
			n.mu.Unlock()
			return target == 0 || matched >= target
		}
		fallback := next - 1
		if response.ConflictIndex > 0 {
			fallback = min(fallback, response.ConflictIndex)
		}
		n.nextIndex[id] = max(1, fallback)
		n.mu.Unlock()
	}
	return false
}

func (n *Node) peerClient(ctx context.Context, id, address string) (*rpc.PeerClient, error) {
	n.connMu.Lock()
	if client := n.peerClients[id]; client != nil {
		n.connMu.Unlock()
		return client, nil
	}
	n.connMu.Unlock()
	dialCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	connection, err := rpc.Dial(dialCtx, address)
	if err != nil {
		return nil, err
	}
	n.connMu.Lock()
	defer n.connMu.Unlock()
	if client := n.peerClients[id]; client != nil {
		_ = connection.Close()
		return client, nil
	}
	n.peerConns[id] = connection
	n.peerClients[id] = rpc.NewPeerClient(connection)
	return n.peerClients[id], nil
}

func (n *Node) electionLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopped:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader && time.Now().After(n.electionDeadline) {
				n.startElectionLocked()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElectionLocked() {
	n.role, n.currentTerm, n.votedFor, n.leader = Candidate, n.currentTerm+1, n.cfg.ID, ""
	n.resetElectionDeadlineLocked()
	if err := n.storage.SaveMeta(n.currentTerm, n.votedFor); err != nil {
		log.Printf("node %s cannot persist election term: %v", n.cfg.ID, err)
		return
	}
	term, lastIndex, lastTerm := n.currentTerm, n.lastLogIndexLocked(), n.lastLogTermLocked()
	go n.runElection(term, lastIndex, lastTerm)
}

func (n *Node) runElection(term, lastIndex, lastTerm int64) {
	type voteResult struct {
		term    int64
		granted bool
	}
	results := make(chan voteResult, len(n.cfg.Peers))
	for id, address := range n.cfg.Peers {
		go func(id, address string) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			client, err := n.peerClient(ctx, id, address)
			if err != nil {
				results <- voteResult{}
				return
			}
			response, err := client.RequestVote(ctx, &rpc.RequestVoteRequest{Term: term, CandidateID: n.cfg.ID, LastLogIndex: lastIndex, LastLogTerm: lastTerm})
			if err != nil {
				results <- voteResult{}
				return
			}
			results <- voteResult{term: response.Term, granted: response.VoteGranted}
		}(id, address)
	}
	votes := 1
	if votes >= n.quorum() {
		n.winElection(term)
		return
	}
	for range n.cfg.Peers {
		result := <-results
		n.mu.Lock()
		if result.term > n.currentTerm {
			_ = n.becomeFollowerLocked(result.term, "")
		}
		stillCandidate := n.role == Candidate && n.currentTerm == term
		n.mu.Unlock()
		if !stillCandidate {
			return
		}
		if result.granted {
			votes++
		}
		if votes >= n.quorum() {
			n.winElection(term)
			return
		}
	}
}

func (n *Node) winElection(term int64) {
	n.mu.Lock()
	if n.role != Candidate || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	n.role, n.leader = Leader, n.cfg.ID
	n.leaderReadyTerm = 0
	last := n.lastLogIndexLocked()
	for id := range n.cfg.Peers {
		n.nextIndex[id] = last + 1
		n.matchIndex[id] = 0
	}
	n.mu.Unlock()
	log.Printf("%s became leader term=%d", n.cfg.ID, term)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _, _ = n.submit(ctx, "noop", "", "")
	}()
}

func (n *Node) becomeFollowerLocked(term int64, leader string) error {
	if term < n.currentTerm {
		return nil
	}
	n.role, n.currentTerm, n.votedFor, n.leader, n.leaderReadyTerm = Follower, term, "", leader, 0
	n.resetElectionDeadlineLocked()
	return n.storage.SaveMeta(n.currentTerm, n.votedFor)
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopped:
			return
		case <-ticker.C:
			n.mu.RLock()
			leader, target := n.role == Leader, n.commitIndex
			n.mu.RUnlock()
			if !leader {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
			acks := n.replicateAll(ctx, target)
			cancel()
			if acks >= n.quorum() {
				n.mu.Lock()
				if n.role == Leader {
					n.lastQuorumContact = time.Now()
				}
				n.mu.Unlock()
			}
		}
	}
}

func (n *Node) confirmLeadership(ctx context.Context) (term, index int64, leader string, err error) {
	n.mu.RLock()
	term, index, leader = n.currentTerm, n.commitIndex, n.leader
	if n.role != Leader {
		n.mu.RUnlock()
		return term, index, leader, ErrNotLeader
	}
	if n.leaderReadyTerm != term {
		n.mu.RUnlock()
		return term, index, leader, ErrLeaderNotReady
	}
	if time.Since(n.lastQuorumContact) < n.cfg.ReadLease {
		n.mu.RUnlock()
		return term, index, n.cfg.ID, nil
	}
	n.mu.RUnlock()
	n.readMu.Lock()
	defer n.readMu.Unlock()
	n.mu.RLock()
	term, index, leader = n.currentTerm, n.commitIndex, n.leader
	if n.role != Leader || n.leaderReadyTerm != term {
		n.mu.RUnlock()
		return term, index, leader, ErrNotLeader
	}
	if time.Since(n.lastQuorumContact) < n.cfg.ReadLease {
		n.mu.RUnlock()
		return term, index, n.cfg.ID, nil
	}
	n.mu.RUnlock()
	checkCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	if n.replicateQuorum(checkCtx, index) < n.quorum() {
		return term, index, leader, ErrQuorum
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.currentTerm != term {
		return term, index, n.leader, ErrNotLeader
	}
	n.lastQuorumContact = time.Now()
	return term, n.commitIndex, n.cfg.ID, nil
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		index := n.lastApplied + 1
		entry, ok := n.entryAtLocked(index)
		if ok {
			n.applyLocked(entry)
		}
		n.lastApplied = index
	}
}

func (n *Node) applyLocked(entry rpc.LogEntry) {
	switch entry.Op {
	case "put":
		n.kv[entry.Key] = entry.Value
		delete(n.hashValues, entry.Key)
	case "delete":
		delete(n.kv, entry.Key)
		delete(n.hashValues, entry.Key)
	case "hmset":
		current := decodeStringMap(n.kv[entry.Key])
		patch := decodeStringMap(entry.Value)
		for key, value := range patch {
			current[key] = value
		}
		data, _ := json.Marshal(current)
		n.kv[entry.Key] = string(data)
		n.hashValues[entry.Key] = flattenHash(current)
	case "zadd":
		var value struct {
			Score  float64 `json:"score"`
			Member string  `json:"member"`
		}
		if json.Unmarshal([]byte(entry.Value), &value) == nil {
			n.kv[zsetKey(entry.Key, value.Member)] = strconv.FormatFloat(value.Score, 'g', -1, 64)
		}
	case "zrem":
		delete(n.kv, zsetKey(entry.Key, entry.Value))
	case "noop":
	}
}

func flattenHash(fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(fields)*2)
	for _, key := range keys {
		values = append(values, key, fields[key])
	}
	return values
}

func (n *Node) maybeSnapshotLocked() error {
	if n.commitIndex-n.snapshotIndex < n.cfg.SnapshotEvery {
		return nil
	}
	index := n.commitIndex
	term, _ := n.termAtLocked(index)
	kept := make([]rpc.LogEntry, 0)
	for _, entry := range n.logEntries {
		if entry.Index > index {
			kept = append(kept, entry)
		}
	}
	n.snapshotIndex, n.snapshotTerm, n.logEntries = index, term, kept
	return n.storage.SaveSnapshot(index, term, n.commitIndex, n.kv, n.logEntries)
}

func (n *Node) zRangeLocked(set string, args []string) []string {
	minScore, maxScore := -1.0e308, 1.0e308
	if len(args) > 0 && args[0] != "-inf" {
		minScore, _ = strconv.ParseFloat(args[0], 64)
	}
	if len(args) > 1 && args[1] != "+inf" {
		maxScore, _ = strconv.ParseFloat(args[1], 64)
	}
	offset, count := 0, int(^uint(0)>>1)
	if len(args) > 3 {
		offset, _ = strconv.Atoi(args[2])
		count, _ = strconv.Atoi(args[3])
	}
	type item struct {
		member string
		score  float64
	}
	items := make([]item, 0)
	prefix := zsetKey(set, "")
	for key, raw := range n.kv {
		if strings.HasPrefix(key, prefix) {
			score, _ := strconv.ParseFloat(raw, 64)
			if score >= minScore && score <= maxScore {
				items = append(items, item{strings.TrimPrefix(key, prefix), score})
			}
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score == items[j].score {
			return items[i].member < items[j].member
		}
		return items[i].score < items[j].score
	})
	if offset >= len(items) {
		return nil
	}
	end := minInt(len(items), offset+count)
	result := make([]string, 0, end-offset)
	for _, item := range items[offset:end] {
		result = append(result, item.member)
	}
	return result
}

func (n *Node) resetElectionDeadlineLocked() {
	span := n.cfg.ElectionMax - n.cfg.ElectionMin
	ids := append(SortPeers(n.cfg.Peers), n.cfg.ID)
	sort.Strings(ids)
	rank := 0
	for index, id := range ids {
		if id == n.cfg.ID {
			rank = index
			break
		}
	}
	// Randomness remains inside a stable per-node slot. The staggering prevents
	// two surviving nodes from repeatedly splitting the vote after a leader
	// failure while rotating the earliest slot across terms.
	width := span / time.Duration(len(ids))
	if width <= 0 {
		width = time.Millisecond
	}
	slot := (rank + int(n.currentTerm)) % len(ids)
	jitter := time.Duration(n.rng.Int63n(int64(width)))
	n.electionDeadline = time.Now().Add(n.cfg.ElectionMin + time.Duration(slot)*width + jitter)
}
func (n *Node) quorum() int       { return (len(n.cfg.Peers)+1)/2 + 1 }
func (n *Node) quorumLocked() int { return n.quorum() }
func (n *Node) lastLogIndexLocked() int64 {
	if len(n.logEntries) == 0 {
		return n.snapshotIndex
	}
	return n.logEntries[len(n.logEntries)-1].Index
}
func (n *Node) lastLogTermLocked() int64 {
	term, _ := n.termAtLocked(n.lastLogIndexLocked())
	return term
}
func (n *Node) termAtLocked(index int64) (int64, bool) {
	if index == 0 {
		return 0, true
	}
	if index == n.snapshotIndex {
		return n.snapshotTerm, true
	}
	if index < n.snapshotIndex {
		return 0, false
	}
	position := index - n.snapshotIndex - 1
	if position >= 0 && position < int64(len(n.logEntries)) && n.logEntries[position].Index == index {
		return n.logEntries[position].Term, true
	}
	return 0, false
}
func (n *Node) entryAtLocked(index int64) (rpc.LogEntry, bool) {
	position := index - n.snapshotIndex - 1
	if position >= 0 && position < int64(len(n.logEntries)) && n.logEntries[position].Index == index {
		return n.logEntries[position], true
	}
	return rpc.LogEntry{}, false
}

func decodeStringMap(raw string) map[string]string {
	result := map[string]string{}
	_ = json.Unmarshal([]byte(raw), &result)
	return result
}
func visibleKV(source map[string]string) map[string]string {
	result := map[string]string{}
	for key, value := range source {
		if !strings.HasPrefix(key, "\x00zset:") {
			result[key] = value
		}
	}
	return result
}
func cloneMap(source map[string]string) map[string]string {
	result := map[string]string{}
	for key, value := range source {
		result[key] = value
	}
	return result
}
func zsetKey(set, member string) string { return "\x00zset:" + set + ":" + member }
func randomSeed(id string) int64 {
	seed := time.Now().UnixNano()
	for _, character := range id {
		seed = seed*131 + int64(character)
	}
	return seed
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
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
func SortPeers(peers map[string]string) []string {
	ids := make([]string, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
