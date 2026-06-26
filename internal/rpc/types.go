package rpc

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

type jsonCodec struct{}

func (jsonCodec) Name() string                           { return "json" }
func (jsonCodec) Marshal(value any) ([]byte, error)      { return json.Marshal(value) }
func (jsonCodec) Unmarshal(data []byte, value any) error { return json.Unmarshal(data, value) }

func init() { encoding.RegisterCodec(jsonCodec{}) }

func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ForceServerCodec(jsonCodec{})}
}

func Dial(ctx context.Context, target string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})),
		grpc.WithBlock(),
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
	Term          int64 `json:"term"`
	Success       bool  `json:"success"`
	MatchIndex    int64 `json:"match_index"`
	ConflictIndex int64 `json:"conflict_index"`
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
type ExecuteRequest struct {
	Op    string   `json:"op"`
	Key   string   `json:"key"`
	Value string   `json:"value"`
	Args  []string `json:"args"`
}
type ExecuteResponse struct {
	Ok       bool   `json:"ok"`
	Leader   string `json:"leader"`
	Error    string `json:"error"`
	Index    int64  `json:"index"`
	Term     int64  `json:"term"`
	Affected int64  `json:"affected"`
}
type QueryRequest struct {
	Op   string   `json:"op"`
	Key  string   `json:"key"`
	Args []string `json:"args"`
}
type QueryResponse struct {
	Ok     bool     `json:"ok"`
	Leader string   `json:"leader"`
	Error  string   `json:"error"`
	Values []string `json:"values"`
	Index  int64    `json:"index"`
	Term   int64    `json:"term"`
}
type StatusRequest struct{}
type StatusResponse struct {
	ID            string `json:"id"`
	Role          string `json:"role"`
	Term          int64  `json:"term"`
	Leader        string `json:"leader"`
	CommitIndex   int64  `json:"commit_index"`
	LastApplied   int64  `json:"last_applied"`
	LastLogIndex  int64  `json:"last_log_index"`
	SnapshotIndex int64  `json:"snapshot_index"`
	MemberCount   int    `json:"member_count"`
	Quorum        int    `json:"quorum"`
	KeyCount      int    `json:"key_count"`
	Ready         bool   `json:"ready"`
}

type PeerServer interface {
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(context.Context, *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}
type KVServer interface {
	Put(context.Context, *PutRequest) (*PutResponse, error)
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Execute(context.Context, *ExecuteRequest) (*ExecuteResponse, error)
	Query(context.Context, *QueryRequest) (*QueryResponse, error)
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
func (c *KVClient) Execute(ctx context.Context, in *ExecuteRequest) (*ExecuteResponse, error) {
	out := new(ExecuteResponse)
	err := c.cc.Invoke(ctx, "/raftkv.KV/Execute", in, out)
	return out, err
}
func (c *KVClient) Query(ctx context.Context, in *QueryRequest) (*QueryResponse, error) {
	out := new(QueryResponse)
	err := c.cc.Invoke(ctx, "/raftkv.KV/Query", in, out)
	return out, err
}
func (c *KVClient) Status(ctx context.Context, in *StatusRequest) (*StatusResponse, error) {
	out := new(StatusResponse)
	err := c.cc.Invoke(ctx, "/raftkv.KV/Status", in, out)
	return out, err
}

func RegisterPeerServer(server *grpc.Server, service PeerServer) {
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "raftkv.Peer", HandlerType: (*PeerServer)(nil), Methods: []grpc.MethodDesc{
		{MethodName: "RequestVote", Handler: peerHandler(service, "RequestVote")},
		{MethodName: "AppendEntries", Handler: peerHandler(service, "AppendEntries")},
		{MethodName: "InstallSnapshot", Handler: peerHandler(service, "InstallSnapshot")},
	}}, service)
}
func RegisterKVServer(server *grpc.Server, service KVServer) {
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "raftkv.KV", HandlerType: (*KVServer)(nil), Methods: []grpc.MethodDesc{
		{MethodName: "Put", Handler: kvHandler(service, "Put")},
		{MethodName: "Get", Handler: kvHandler(service, "Get")},
		{MethodName: "Execute", Handler: kvHandler(service, "Execute")},
		{MethodName: "Query", Handler: kvHandler(service, "Query")},
		{MethodName: "Status", Handler: kvHandler(service, "Status")},
	}}, service)
}

func peerHandler(service PeerServer, method string) grpc.MethodHandler {
	return func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		var request any
		var invoke grpc.UnaryHandler
		switch method {
		case "RequestVote":
			value := new(RequestVoteRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) {
				return service.RequestVote(ctx, req.(*RequestVoteRequest))
			}
		case "AppendEntries":
			value := new(AppendEntriesRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) {
				return service.AppendEntries(ctx, req.(*AppendEntriesRequest))
			}
		case "InstallSnapshot":
			value := new(InstallSnapshotRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) {
				return service.InstallSnapshot(ctx, req.(*InstallSnapshotRequest))
			}
		default:
			return nil, fmt.Errorf("unknown peer method %q", method)
		}
		if err := decode(request); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return invoke(ctx, request)
		}
		return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: server, FullMethod: "/raftkv.Peer/" + method}, invoke)
	}
}

func kvHandler(service KVServer, method string) grpc.MethodHandler {
	return func(server any, ctx context.Context, decode func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		var request any
		var invoke grpc.UnaryHandler
		switch method {
		case "Put":
			value := new(PutRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) { return service.Put(ctx, req.(*PutRequest)) }
		case "Get":
			value := new(GetRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) { return service.Get(ctx, req.(*GetRequest)) }
		case "Execute":
			value := new(ExecuteRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) { return service.Execute(ctx, req.(*ExecuteRequest)) }
		case "Query":
			value := new(QueryRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) { return service.Query(ctx, req.(*QueryRequest)) }
		case "Status":
			value := new(StatusRequest)
			request = value
			invoke = func(ctx context.Context, req any) (any, error) { return service.Status(ctx, req.(*StatusRequest)) }
		default:
			return nil, fmt.Errorf("unknown KV method %q", method)
		}
		if err := decode(request); err != nil {
			return nil, err
		}
		if interceptor == nil {
			return invoke(ctx, request)
		}
		return interceptor(ctx, request, &grpc.UnaryServerInfo{Server: server, FullMethod: "/raftkv.KV/" + method}, invoke)
	}
}
