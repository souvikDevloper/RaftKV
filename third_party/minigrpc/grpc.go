package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
)

type ServerOption struct{}
type CallOption struct{}
type StreamDesc struct{}
type UnaryServerInterceptor interface{}
type MethodHandler func(srv any, ctx context.Context, dec func(any) error, interceptor UnaryServerInterceptor) (any, error)
type MethodDesc struct {
	MethodName string
	Handler    MethodHandler
}
type ServiceDesc struct {
	ServiceName string
	HandlerType any
	Methods     []MethodDesc
	Streams     []StreamDesc
}

type Server struct {
	mu      sync.RWMutex
	methods map[string]registered
}
type registered struct {
	srv     any
	handler MethodHandler
}

type ClientConn struct{ target string }

type CodecLike interface {
	Name() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

func ForceServerCodec(CodecLike) ServerOption  { return ServerOption{} }
func ForceCodec(CodecLike) CallOption          { return CallOption{} }
func WithInsecure() any                        { return nil }
func WithDefaultCallOptions(...CallOption) any { return nil }

func NewServer(...ServerOption) *Server { return &Server{methods: map[string]registered{}} }
func (s *Server) RegisterService(desc *ServiceDesc, srv any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range desc.Methods {
		s.methods["/"+desc.ServiceName+"/"+m.MethodName] = registered{srv: srv, handler: m.Handler}
	}
}
func (s *Server) Serve(lis net.Listener) error { return http.Serve(lis, s) }
func (s *Server) Stop()                        {}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	reg, ok := s.methods[r.URL.Path]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, "unknown method", 404)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	dec := func(v any) error { return json.Unmarshal(body, v) }
	resp, err := reg.handler(reg.srv, r.Context(), dec, nil)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
func DialContext(ctx context.Context, target string, _ ...any) (*ClientConn, error) {
	return &ClientConn{target: target}, nil
}
func (c *ClientConn) Close() error { return nil }
func (c *ClientConn) Invoke(ctx context.Context, method string, in any, out any, _ ...CallOption) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := c.target
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}
	url = strings.TrimRight(url, "/") + method
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		data, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("rpc %s failed: %s", method, strings.TrimSpace(string(data)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
