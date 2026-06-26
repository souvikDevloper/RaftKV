package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/metrics"
	"github.com/souvikDevloper/RaftKV/internal/raft"
	"github.com/souvikDevloper/RaftKV/internal/resp"
	"github.com/souvikDevloper/RaftKV/internal/rpc"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "server":
		runServer(os.Args[2:])
	case "put":
		runPut(os.Args[2:])
	case "get":
		runGet(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "redis-proxy":
		runRedisProxy(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println(`RaftKV commands:
  raftkv server --id n1 --listen 127.0.0.1:7001 --peers n2=127.0.0.1:7002,n3=127.0.0.1:7003 --data data/n1
  raftkv put --nodes 127.0.0.1:7001,127.0.0.1:7002 --key x --value 42
  raftkv get --nodes 127.0.0.1:7001,127.0.0.1:7002 --key x
  raftkv status --nodes 127.0.0.1:7001,127.0.0.1:7002
  raftkv redis-proxy --listen 127.0.0.1:6380 --nodes 127.0.0.1:7001,127.0.0.1:7002`)
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	id := fs.String("id", "n1", "node id")
	listen := fs.String("listen", "127.0.0.1:7001", "gRPC listen address")
	peersRaw := fs.String("peers", "", "comma-separated id=addr peers")
	data := fs.String("data", "data/n1", "data directory")
	snapEvery := fs.Int64("snapshot-every", 10000, "entries between snapshots")
	metricsListen := fs.String("metrics-listen", "127.0.0.1:9101", "Prometheus and HdrHistogram HTTP address")
	groupMax := fs.Int("group-commit-max", 256, "maximum proposals per durable WAL transaction")
	groupWindow := fs.Duration("group-commit-window", time.Millisecond, "maximum proposal batching delay")
	respListen := fs.String("resp-listen", "", "optional direct RESP endpoint for YCSB")
	fs.Parse(args)
	peers := parsePeers(*peersRaw)
	registry := metrics.New()
	n, err := raft.New(raft.Config{ID: *id, Listen: *listen, Peers: peers, DataDir: *data, SnapshotEvery: *snapEvery,
		GroupCommitMax: *groupMax, GroupCommitWindow: *groupWindow, Metrics: registry})
	if err != nil {
		log.Fatal(err)
	}
	if err := n.Start(); err != nil {
		log.Fatal(err)
	}
	observability := http.NewServeMux()
	observability.Handle("/", registry.Handler())
	observability.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	observability.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		status, _ := n.Status(context.Background(), &rpc.StatusRequest{})
		w.Header().Set("Content-Type", "application/json")
		if !status.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	metricsServer := &http.Server{Addr: *metricsListen, Handler: observability, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *respListen != "" {
		go func() {
			log.Printf("direct YCSB RESP endpoint listening on %s", *respListen)
			if err := resp.NewServer(*respListen, n).Serve(ctx); err != nil {
				log.Printf("RESP endpoint stopped: %v", err)
				stop()
			}
		}()
	}
	log.Printf("node %s listening on %s peers=%v", *id, *listen, peers)
	<-ctx.Done()
	n.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = metricsServer.Shutdown(ctx)
}

func parsePeers(raw string) map[string]string {
	out := map[string]string{}
	if raw == "" {
		return out
	}
	for _, part := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			out[kv[0]] = kv[1]
		}
	}
	return out
}
func nodeList(raw string) []string {
	xs := []string{}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			xs = append(xs, p)
		}
	}
	return xs
}

func runPut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	nodes := fs.String("nodes", "127.0.0.1:7001", "comma-separated node addresses")
	key := fs.String("key", "x", "key")
	val := fs.String("value", "42", "value")
	jsonOut := fs.Bool("json", false, "json output")
	fs.Parse(args)
	resp, err := putToAny(nodeList(*nodes), *key, *val)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(resp)
	} else {
		fmt.Printf("ok=%v leader=%s index=%d term=%d error=%s\n", resp.Ok, resp.Leader, resp.Index, resp.Term, resp.Error)
	}
}
func runGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	nodes := fs.String("nodes", "127.0.0.1:7001", "comma-separated node addresses")
	key := fs.String("key", "x", "key")
	jsonOut := fs.Bool("json", false, "json output")
	fs.Parse(args)
	resp, err := getFromAny(nodeList(*nodes), *key)
	if err != nil {
		log.Fatal(err)
	}
	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(resp)
	} else {
		fmt.Printf("ok=%v value=%s leader=%s index=%d term=%d error=%s\n", resp.Ok, resp.Value, resp.Leader, resp.Index, resp.Term, resp.Error)
	}
}
func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	nodes := fs.String("nodes", "127.0.0.1:7001", "comma-separated node addresses")
	fs.Parse(args)
	for _, addr := range nodeList(*nodes) {
		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Millisecond)
		cc, err := rpc.Dial(ctx, addr)
		if err != nil {
			cancel()
			fmt.Printf("%s down\n", addr)
			continue
		}
		resp, err := rpc.NewKVClient(cc).Status(ctx, &rpc.StatusRequest{})
		cc.Close()
		cancel()
		if err != nil {
			fmt.Printf("%s error: %v\n", addr, err)
			continue
		}
		b, _ := json.Marshal(resp)
		fmt.Println(string(b))
	}
}
func runRedisProxy(args []string) {
	fs := flag.NewFlagSet("redis-proxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:6380", "RESP listen address")
	nodes := fs.String("nodes", "127.0.0.1:7001", "comma-separated RaftKV gRPC addresses")
	fs.Parse(args)
	router := resp.NewRouter(nodeList(*nodes))
	defer router.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log.Printf("YCSB Redis compatibility proxy listening on %s", *listen)
	if err := resp.NewServer(*listen, router).Serve(ctx); err != nil {
		log.Fatal(err)
	}
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	<-ch
}
func putToAny(nodes []string, key, val string) (*rpc.PutResponse, error) {
	seen := map[string]bool{}
	for tries := 0; tries < 3; tries++ {
		for _, addr := range nodes {
			if seen[addr] && tries == 0 {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			cc, err := rpc.Dial(ctx, addr)
			if err != nil {
				cancel()
				continue
			}
			resp, err := rpc.NewKVClient(cc).Put(ctx, &rpc.PutRequest{Key: key, Value: val})
			cc.Close()
			cancel()
			seen[addr] = true
			if err == nil && resp.Ok {
				return resp, nil
			}
			if err == nil && resp.Leader != "" {
				leaderAddr := leaderToAddr(nodes, resp.Leader)
				if leaderAddr != "" {
					nodes = append([]string{leaderAddr}, nodes...)
				}
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return nil, raft.ErrNoLeader
}
func getFromAny(nodes []string, key string) (*rpc.GetResponse, error) {
	for tries := 0; tries < 3; tries++ {
		for _, addr := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			cc, err := rpc.Dial(ctx, addr)
			if err != nil {
				cancel()
				continue
			}
			resp, err := rpc.NewKVClient(cc).Get(ctx, &rpc.GetRequest{Key: key})
			cc.Close()
			cancel()
			if err == nil && (resp.Ok || resp.Error == "") {
				return resp, nil
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return nil, raft.ErrNoLeader
}
func leaderToAddr(nodes []string, leader string) string { // scripts use n1->7001 mapping
	if strings.HasPrefix(leader, "n") && len(leader) > 1 {
		idx := leader[1:]
		for _, a := range nodes {
			if strings.HasSuffix(a, "700"+idx) {
				return a
			}
		}
	}
	return ""
}
