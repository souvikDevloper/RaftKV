package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
)

type historyRecord struct {
	ID       int64  `json:"id"`
	ClientID int    `json:"client_id"`
	Op       string `json:"op"`
	Value    string `json:"value,omitempty"`
	Output   string `json:"output,omitempty"`
	Call     int64  `json:"call_ns"`
	Return   int64  `json:"return_ns"`
	OK       bool   `json:"ok"`
}

func main() {
	nodesFlag := flag.String("nodes", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005", "comma-separated gRPC nodes")
	historyFlag := flag.String("history", "run/history.jsonl", "history output")
	workers := flag.Int("workers", 4, "concurrent clients")
	ops := flag.Int("ops-per-worker", 12, "operations per client")
	flag.Parse()
	nodes := strings.Split(*nodesFlag, ",")
	var sequence atomic.Int64
	var mu sync.Mutex
	records := make([]historyRecord, 0, *workers**ops)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for clientID := 0; clientID < *workers; clientID++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			random := rand.New(rand.NewSource(int64(clientID + 1)))
			<-start
			for index := 0; index < *ops; index++ {
				record := historyRecord{ID: sequence.Add(1), ClientID: clientID, Call: time.Now().UnixNano()}
				if random.Intn(100) < 55 {
					record.Op = "put"
					record.Value = fmt.Sprintf("%d-%d", clientID, index)
					record.OK = put(nodes, "chaos-register", record.Value)
				} else {
					record.Op = "get"
					record.Output, record.OK = get(nodes, "chaos-register")
				}
				record.Return = time.Now().UnixNano()
				mu.Lock()
				records = append(records, record)
				mu.Unlock()
			}
		}(clientID)
	}
	close(start)
	wg.Wait()
	sort.Slice(records, func(i, j int) bool { return records[i].Call < records[j].Call })
	file, err := os.Create(*historyFlag)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, record := range records {
		if err := json.NewEncoder(writer).Encode(record); err != nil {
			panic(err)
		}
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
	fmt.Printf("recorded %d concurrent operations in %s\n", len(records), *historyFlag)
}

func put(nodes []string, key, value string) bool {
	for attempt := 0; attempt < 3; attempt++ {
		for _, address := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			connection, err := rpc.Dial(ctx, address)
			if err != nil {
				cancel()
				continue
			}
			response, err := rpc.NewKVClient(connection).Put(ctx, &rpc.PutRequest{Key: key, Value: value})
			_ = connection.Close()
			cancel()
			if err == nil && response.Ok {
				return true
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}
func get(nodes []string, key string) (string, bool) {
	for attempt := 0; attempt < 3; attempt++ {
		for _, address := range nodes {
			ctx, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
			connection, err := rpc.Dial(ctx, address)
			if err != nil {
				cancel()
				continue
			}
			response, err := rpc.NewKVClient(connection).Get(ctx, &rpc.GetRequest{Key: key})
			_ = connection.Close()
			cancel()
			if err == nil && response.Error == "" {
				return response.Value, response.Ok
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	return "", false
}
