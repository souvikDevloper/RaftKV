package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const maxLatencyMicros = int64((10 * time.Minute) / time.Microsecond)
const histogramShards = 8

type Summary struct {
	Count int64 `json:"count"`
	P50US int64 `json:"p50_us"`
	P95US int64 `json:"p95_us"`
	P99US int64 `json:"p99_us"`
	MaxUS int64 `json:"max_us"`
}

type Registry struct {
	mu         sync.RWMutex
	startedAt  time.Time
	histograms map[string]*shardedHistogram
	gauges     map[string]float64
}

type histogramShard struct {
	mu        sync.Mutex
	histogram *hdr.Histogram
}

type shardedHistogram struct {
	next   atomic.Uint64
	shards [histogramShards]histogramShard
}

func newShardedHistogram() *shardedHistogram {
	h := &shardedHistogram{}
	for index := range h.shards {
		h.shards[index].histogram = hdr.New(1, maxLatencyMicros, 3)
	}
	return h
}

func (h *shardedHistogram) observe(micros int64) {
	shard := &h.shards[h.next.Add(1)%histogramShards]
	shard.mu.Lock()
	_ = shard.histogram.RecordValue(micros)
	shard.mu.Unlock()
}

func (h *shardedHistogram) merged() *hdr.Histogram {
	merged := hdr.New(1, maxLatencyMicros, 3)
	for index := range h.shards {
		shard := &h.shards[index]
		shard.mu.Lock()
		_ = merged.Merge(shard.histogram)
		shard.mu.Unlock()
	}
	return merged
}

func (h *shardedHistogram) reset() {
	for index := range h.shards {
		shard := &h.shards[index]
		shard.mu.Lock()
		shard.histogram.Reset()
		shard.mu.Unlock()
	}
}

func New() *Registry {
	return &Registry{
		startedAt: time.Now().UTC(),
		histograms: map[string]*shardedHistogram{
			"client_read":  newShardedHistogram(),
			"client_write": newShardedHistogram(),
			"raft_commit":  newShardedHistogram(),
		},
		gauges: map[string]float64{},
	}
}

func (r *Registry) SetGauge(name string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = value
}

func (r *Registry) Observe(name string, elapsed time.Duration) {
	micros := elapsed.Microseconds()
	if micros < 1 {
		micros = 1
	}
	if micros > maxLatencyMicros {
		micros = maxLatencyMicros
	}
	r.mu.RLock()
	histogram := r.histograms[name]
	r.mu.RUnlock()
	if histogram == nil {
		r.mu.Lock()
		histogram = r.histograms[name]
		if histogram == nil {
			histogram = newShardedHistogram()
			r.histograms[name] = histogram
		}
		r.mu.Unlock()
	}
	histogram.observe(micros)
}

func (r *Registry) Snapshot() map[string]Summary {
	r.mu.RLock()
	histograms := make(map[string]*shardedHistogram, len(r.histograms))
	for name, histogram := range r.histograms {
		histograms[name] = histogram
	}
	r.mu.RUnlock()
	result := make(map[string]Summary, len(histograms))
	for name, shards := range histograms {
		histogram := shards.merged()
		result[name] = Summary{
			Count: histogram.TotalCount(),
			P50US: histogram.ValueAtQuantile(50),
			P95US: histogram.ValueAtQuantile(95),
			P99US: histogram.ValueAtQuantile(99),
			MaxUS: histogram.Max(),
		}
	}
	return result
}

func (r *Registry) GaugeSnapshot() map[string]float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]float64, len(r.gauges))
	for name, value := range r.gauges {
		result[name] = value
	}
	return result
}

func (r *Registry) Reset() {
	r.mu.Lock()
	r.startedAt = time.Now().UTC()
	histograms := make([]*shardedHistogram, 0, len(r.histograms))
	for _, histogram := range r.histograms {
		histograms = append(histograms, histogram)
	}
	r.mu.Unlock()
	for _, histogram := range histograms {
		histogram.reset()
	}
}

func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/histograms", func(w http.ResponseWriter, req *http.Request) {
		if req.Method == http.MethodPost {
			r.Reset()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if req.Method != http.MethodGet {
			http.Error(w, "GET or POST required", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		r.mu.RLock()
		startedAt := r.startedAt
		r.mu.RUnlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"started_at": startedAt,
			"unit":       "microseconds",
			"histograms": r.Snapshot(),
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		snapshot := r.Snapshot()
		names := make([]string, 0, len(snapshot))
		for name := range snapshot {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			summary := snapshot[name]
			metric := strings.NewReplacer("-", "_", ".", "_").Replace(name)
			_, _ = fmt.Fprintf(w, "raftkv_%s_count %d\n", metric, summary.Count)
			_, _ = fmt.Fprintf(w, "raftkv_%s_p50_microseconds %d\n", metric, summary.P50US)
			_, _ = fmt.Fprintf(w, "raftkv_%s_p95_microseconds %d\n", metric, summary.P95US)
			_, _ = fmt.Fprintf(w, "raftkv_%s_p99_microseconds %d\n", metric, summary.P99US)
			_, _ = fmt.Fprintf(w, "raftkv_%s_max_microseconds %d\n", metric, summary.MaxUS)
		}
		gauges := r.GaugeSnapshot()
		gaugeNames := make([]string, 0, len(gauges))
		for name := range gauges {
			gaugeNames = append(gaugeNames, name)
		}
		sort.Strings(gaugeNames)
		for _, name := range gaugeNames {
			metric := strings.NewReplacer("-", "_", ".", "_").Replace(name)
			_, _ = fmt.Fprintf(w, "raftkv_%s %g\n", metric, gauges[name])
		}
	})
	return mux
}
