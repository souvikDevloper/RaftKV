package rpc

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// jsonCodec lets this project use real gRPC transport without requiring protoc
// generation. The RPCs are still gRPC calls; the payload codec is JSON so the
// code stays easy to inspect in interviews.
type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func init() { encoding.RegisterCodec(jsonCodec{}) }

func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ForceServerCodec(jsonCodec{})}
}

func Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, target,
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
	)
}

type LogEntry struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RequestVoteRequest struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int64  `json:"last_log_index"`
	LastLogTerm  int64  `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

type AppendEntriesRequest struct {
	Term         int64      `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int64      `json:"prev_log_index"`
	PrevLogTerm  int64      `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int64      `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term       int64 `json:"term"`
	Success    bool  `json:"success"`
	MatchIndex int64 `json:"match_index"`
}

type InstallSnapshotRequest struct {
	Term              int64             `json:"term"`
	LeaderID          string            `json:"leader_id"`
	LastIncludedIndex int64             `json:"last_included_index"`
	LastIncludedTerm  int64             `json:"last_included_term"`
	State             map[string]string `json:"state"`
}

type InstallSnapshotResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

type PutRequest struct{ Key, Value string }
type PutResponse struct {
	Ok     bool
	Leader string
	Error  string
	Index  int64
	Term   int64
}
type GetRequest struct{ Key string }
type GetResponse struct {
	Ok     bool
	Value  string
	Leader string
	Error  string
	Index  int64
	Term   int64
}
type StatusRequest struct{}
type StatusResponse struct {
	ID            string            `json:"id"`
	Role          string            `json:"role"`
	Term          int64             `json:"term"`
	Leader        string            `json:"leader"`
	CommitIndex   int64             `json:"commit_index"`
	LastApplied   int64             `json:"last_applied"`
	LastLogIndex  int64             `json:"last_log_index"`
	SnapshotIndex int64             `json:"snapshot_index"`
	KV            map[string]string `json:"kv"`
}

type PeerServer interface {
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(context.Context, *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

type KVServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Status(context.Context, *StatusRequest) (*StatusResponse, error)
}

type PeerClient struct{ cc *grpc.ClientConn }
type KVClient struct{ cc *grpc.ClientConn }

func NewPeerClient(cc *grpc.ClientConn) *PeerClient { return &PeerClient{cc: cc} }
func NewKVClient(cc *grpc.ClientConn) *KVClient     { return &KVClient{cc: cc} }
func (c *PeerClient) RequestVote(ctx context.Context, in *RequestVoteRequest) (*RequestVoteResponse, error) {
	out := new(RequestVoteResponse)
	err := c.cc.Invoke(ctx, "/raftkv.Peer/RequestVote", in, out)
	return out, err
}
func (c *PeerClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, "/raftkv.Peer/AppendEntries", in, out)
	return out, err
}
func (c *PeerClient) InstallSnapshot(ctx context.Context, in *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	out := new(InstallSnapshotResponse)
	err := c.cc.Invoke(ctx, "/raftkv.Peer/InstallSnapshot", in, out)
	return out, err
}
func (c *KVClient) Put(ctx context.Context, in *PutRequest) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/raftkv.KV/Put", in, out)
	return out, err
}
func (c *KVClient) Get(ctx context.Context, in *GetRequest) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/raftkv.KV/Get", in, out)
	return out, err
}
func (c *KVClient) Status(ctx context.Context, in *StatusRequest) (*StatusResponse, error) {
	out := new(StatusResponse)
	err := c.cc.Invoke(ctx, "/raftkv.KV/Status", in, out)
	return out, err
}

func RegisterPeerServer(s *grpc.Server, srv PeerServer) {
	s.RegisterService(&grpc.ServiceDesc{ServiceName: "raftkv.Peer", HandlerType: (*PeerServer)(nil), Methods: []grpc.MethodDesc{
		{MethodName: "RequestVote", Handler: unaryPeerHandler(srv, "RequestVote")},
		{MethodName: "AppendEntries", Handler: unaryPeerHandler(srv, "AppendEntries")},
		{MethodName: "InstallSnapshot", Handler: unaryPeerHandler(srv, "InstallSnapshot")},
	}, Streams: []grpc.StreamDesc{}}, srv)
}
func RegisterKVServer(s *grpc.Server, srv KVServer) {
	s.RegisterService(&grpc.ServiceDesc{ServiceName: "raftkv.KV", HandlerType: (*KVServer)(nil), Methods: []grpc.MethodDesc{
		{MethodName: "Put", Handler: unaryKVHandler(srv, "Put")},
		{MethodName: "Get", Handler: unaryKVHandler(srv, "Get")},
		{MethodName: "Status", Handler: unaryKVHandler(srv, "Status")},
	}, Streams: []grpc.StreamDesc{}}, srv)
}

func unaryPeerHandler(srv PeerServer, name string) grpc.MethodHandler {
	return func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		switch name {
		case "RequestVote":
			in := new(RequestVoteRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.RequestVote(ctx, in)
		case "AppendEntries":
			in := new(AppendEntriesRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.AppendEntries(ctx, in)
		case "InstallSnapshot":
			in := new(InstallSnapshotRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.InstallSnapshot(ctx, in)
		default:
			return nil, fmt.Errorf("unknown peer method %s", name)
		}
	}
}
func unaryKVHandler(srv KVServer, name string) grpc.MethodHandler {
	return func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		switch name {
		case "Put":
			in := new(PutRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.Put(ctx, in)
		case "Get":
			in := new(GetRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.Get(ctx, in)
		case "Status":
			in := new(StatusRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.Status(ctx, in)
		default:
			return nil, fmt.Errorf("unknown kv method %s", name)
		}
	}
}
