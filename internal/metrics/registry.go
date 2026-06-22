package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

const maxLatencyMicros = int64((10 * time.Minute) / time.Microsecond)

type Summary struct {
	Count int64 `json:"count"`
	P50US int64 `json:"p50_us"`
	P95US int64 `json:"p95_us"`
	P99US int64 `json:"p99_us"`
	MaxUS int64 `json:"max_us"`
}

type Registry struct {
	mu         sync.Mutex
	startedAt  time.Time
	histograms map[string]*hdr.Histogram
}

func New() *Registry {
	return &Registry{startedAt: time.Now().UTC(), histograms: map[string]*hdr.Histogram{}}
}

func (r *Registry) Observe(name string, elapsed time.Duration) {
	micros := elapsed.Microseconds()
	if micros < 1 {
		micros = 1
	}
	if micros > maxLatencyMicros {
		micros = maxLatencyMicros
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	histogram := r.histograms[name]
	if histogram == nil {
		histogram = hdr.New(1, maxLatencyMicros, 3)
		r.histograms[name] = histogram
	}
	_ = histogram.RecordValue(micros)
}

func (r *Registry) Snapshot() map[string]Summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]Summary, len(r.histograms))
	for name, histogram := range r.histograms {
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

func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.startedAt = time.Now().UTC()
	for _, histogram := range r.histograms {
		histogram.Reset()
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
		_ = json.NewEncoder(w).Encode(map[string]any{
			"started_at": r.startedAt,
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
	})
	return mux
}
