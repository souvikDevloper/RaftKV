// Package resp implements the small Redis protocol surface used by the
// official YCSB Redis binding. RaftKV itself remains a gRPC service; this proxy
// is a benchmark adapter so YCSB can drive the exact same A-F workloads used
// for Redis and other databases.
package resp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
	"google.golang.org/grpc"
)

type Router struct {
	mu        sync.Mutex
	nodes     []string
	preferred string
	conns     map[string]*grpc.ClientConn
	clients   map[string]*rpc.KVClient
}

type Backend interface {
	Execute(context.Context, *rpc.ExecuteRequest) (*rpc.ExecuteResponse, error)
	Query(context.Context, *rpc.QueryRequest) (*rpc.QueryResponse, error)
}

func NewRouter(nodes []string) *Router {
	return &Router{nodes: append([]string(nil), nodes...), conns: map[string]*grpc.ClientConn{}, clients: map[string]*rpc.KVClient{}}
}

func (r *Router) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, connection := range r.conns {
		_ = connection.Close()
	}
}

func (r *Router) Execute(ctx context.Context, request *rpc.ExecuteRequest) (*rpc.ExecuteResponse, error) {
	var lastErr error
	for _, address := range r.order() {
		client, err := r.client(ctx, address)
		if err != nil {
			lastErr = err
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		response, err := client.Execute(callCtx, request)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if response.Ok {
			r.setPreferred(address)
			return response, nil
		}
		lastErr = errors.New(response.Error)
	}
	if lastErr == nil {
		lastErr = errors.New("no RaftKV nodes configured")
	}
	return nil, lastErr
}

func (r *Router) Query(ctx context.Context, request *rpc.QueryRequest) (*rpc.QueryResponse, error) {
	var lastErr error
	for _, address := range r.order() {
		client, err := r.client(ctx, address)
		if err != nil {
			lastErr = err
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		response, err := client.Query(callCtx, request)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if response.Error == "" {
			r.setPreferred(address)
			return response, nil
		}
		lastErr = errors.New(response.Error)
	}
	if lastErr == nil {
		lastErr = errors.New("no RaftKV nodes configured")
	}
	return nil, lastErr
}

func (r *Router) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]string, 0, len(r.nodes))
	if r.preferred != "" {
		result = append(result, r.preferred)
	}
	for _, address := range r.nodes {
		if address != r.preferred {
			result = append(result, address)
		}
	}
	return result
}
func (r *Router) setPreferred(address string) { r.mu.Lock(); r.preferred = address; r.mu.Unlock() }
func (r *Router) client(ctx context.Context, address string) (*rpc.KVClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if client := r.clients[address]; client != nil {
		return client, nil
	}
	dialCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	connection, err := rpc.Dial(dialCtx, address)
	if err != nil {
		return nil, err
	}
	r.conns[address] = connection
	r.clients[address] = rpc.NewKVClient(connection)
	return r.clients[address], nil
}

type Server struct {
	address  string
	router   Backend
	listener net.Listener
}

func NewServer(address string, router Backend) *Server {
	return &Server{address: address, router: router}
}

func (s *Server) Serve(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.listener = listener
	go func() { <-ctx.Done(); _ = listener.Close() }()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handle(ctx, connection)
	}
}

func (s *Server) handle(ctx context.Context, connection net.Conn) {
	defer connection.Close()
	reader, writer := bufio.NewReaderSize(connection, 64*1024), bufio.NewWriterSize(connection, 64*1024)
	for {
		command, err := readCommand(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				writeError(writer, err.Error())
				_ = writer.Flush()
			}
			return
		}
		if len(command) == 0 {
			continue
		}
		quit := s.dispatch(ctx, writer, command)
		if err := writer.Flush(); err != nil || quit {
			return
		}
	}
}

func (s *Server) dispatch(ctx context.Context, writer *bufio.Writer, command []string) bool {
	name := strings.ToUpper(command[0])
	switch name {
	case "PING":
		writeSimple(writer, "PONG")
	case "QUIT":
		writeSimple(writer, "OK")
		return true
	case "AUTH", "SELECT":
		writeSimple(writer, "OK")
	case "CLIENT":
		if len(command) > 1 && strings.EqualFold(command[1], "GETNAME") {
			writeNil(writer)
		} else {
			writeSimple(writer, "OK")
		}
	case "COMMAND":
		writeArray(writer, nil)
	case "HSET", "HMSET":
		if len(command) < 4 || len(command)%2 != 0 {
			writeError(writer, "wrong number of arguments")
			break
		}
		fields := map[string]string{}
		for i := 2; i < len(command); i += 2 {
			fields[command[i]] = command[i+1]
		}
		data, _ := json.Marshal(fields)
		if _, err := s.router.Execute(ctx, &rpc.ExecuteRequest{Op: "hmset", Key: command[1], Value: string(data)}); err != nil {
			writeError(writer, err.Error())
		} else if name == "HSET" {
			writeInteger(writer, int64(len(fields)))
		} else {
			writeSimple(writer, "OK")
		}
	case "HGETALL":
		if len(command) != 2 {
			writeError(writer, "wrong number of arguments")
			break
		}
		response, err := s.router.Query(ctx, &rpc.QueryRequest{Op: "hgetall", Key: command[1]})
		if err != nil {
			writeError(writer, err.Error())
		} else {
			writeArray(writer, response.Values)
		}
	case "HMGET":
		if len(command) < 3 {
			writeError(writer, "wrong number of arguments")
			break
		}
		response, err := s.router.Query(ctx, &rpc.QueryRequest{Op: "hmget", Key: command[1], Args: command[2:]})
		if err != nil {
			writeError(writer, err.Error())
		} else {
			writeArray(writer, response.Values)
		}
	case "DEL":
		if len(command) != 2 {
			writeError(writer, "wrong number of arguments")
			break
		}
		if _, err := s.router.Execute(ctx, &rpc.ExecuteRequest{Op: "delete", Key: command[1]}); err != nil {
			writeError(writer, err.Error())
		} else {
			writeInteger(writer, 1)
		}
	case "ZADD":
		if len(command) != 4 {
			writeError(writer, "wrong number of arguments")
			break
		}
		score, err := strconv.ParseFloat(command[2], 64)
		if err != nil {
			writeError(writer, "invalid score")
			break
		}
		data, _ := json.Marshal(map[string]any{"score": score, "member": command[3]})
		if _, err := s.router.Execute(ctx, &rpc.ExecuteRequest{Op: "zadd", Key: command[1], Value: string(data)}); err != nil {
			writeError(writer, err.Error())
		} else {
			writeInteger(writer, 1)
		}
	case "ZREM":
		if len(command) != 3 {
			writeError(writer, "wrong number of arguments")
			break
		}
		if _, err := s.router.Execute(ctx, &rpc.ExecuteRequest{Op: "zrem", Key: command[1], Value: command[2]}); err != nil {
			writeError(writer, err.Error())
		} else {
			writeInteger(writer, 1)
		}
	case "ZRANGEBYSCORE":
		if len(command) < 4 {
			writeError(writer, "wrong number of arguments")
			break
		}
		args := []string{command[2], command[3], "0", strconv.Itoa(int(^uint(0) >> 1))}
		if len(command) >= 7 && strings.EqualFold(command[4], "LIMIT") {
			args[2], args[3] = command[5], command[6]
		}
		response, err := s.router.Query(ctx, &rpc.QueryRequest{Op: "zrangebyscore", Key: command[1], Args: args})
		if err != nil {
			writeError(writer, err.Error())
		} else {
			writeArray(writer, response.Values)
		}
	default:
		writeError(writer, fmt.Sprintf("unsupported command %s", name))
	}
	return false
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if !strings.HasPrefix(line, "*") {
		return strings.Fields(line), nil
	}
	count, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil {
		return nil, err
	}
	result := make([]string, count)
	for index := 0; index < count; index++ {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimSuffix(strings.TrimSuffix(header, "\n"), "\r")
		if !strings.HasPrefix(header, "$") {
			return nil, errors.New("expected bulk string")
		}
		length, err := strconv.Atoi(strings.TrimPrefix(header, "$"))
		if err != nil || length < 0 {
			return nil, errors.New("invalid bulk length")
		}
		buffer := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buffer); err != nil {
			return nil, err
		}
		result[index] = string(buffer[:length])
	}
	return result, nil
}

func writeSimple(writer *bufio.Writer, value string) { _, _ = fmt.Fprintf(writer, "+%s\r\n", value) }
func writeError(writer *bufio.Writer, value string) {
	value = strings.ReplaceAll(value, "\r\n", " ")
	_, _ = fmt.Fprintf(writer, "-ERR %s\r\n", value)
}
func writeInteger(writer *bufio.Writer, value int64) { _, _ = fmt.Fprintf(writer, ":%d\r\n", value) }
func writeNil(writer *bufio.Writer)                  { _, _ = writer.WriteString("$-1\r\n") }
func writeArray(writer *bufio.Writer, values []string) {
	_, _ = fmt.Fprintf(writer, "*%d\r\n", len(values))
	for _, value := range values {
		_, _ = fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(value), value)
	}
}
