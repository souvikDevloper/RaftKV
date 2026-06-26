package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHistogramUsesCompleteDistribution(t *testing.T) {
	r := New()
	for _, latency := range []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 100 * time.Millisecond} {
		r.Observe("read", latency)
	}
	summary := r.Snapshot()["read"]
	if summary.Count != 4 || summary.P99US < 90_000 {
		t.Fatalf("unexpected full-distribution summary: %+v", summary)
	}
}

func TestConcurrentObservationsMergeIntoOneDistribution(t *testing.T) {
	r := New()
	const workers, perWorker = 64, 500
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(offset int) {
			defer wait.Done()
			for index := 0; index < perWorker; index++ {
				r.Observe("read", time.Duration(offset+index+1)*time.Microsecond)
			}
		}(worker)
	}
	wait.Wait()
	if got := r.Snapshot()["read"].Count; got != workers*perWorker {
		t.Fatalf("merged histogram count=%d want=%d", got, workers*perWorker)
	}
}

func BenchmarkRegistryObserveParallel(b *testing.B) {
	r := New()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Observe("read", 250*time.Microsecond)
		}
	})
}

func TestRegistryExportsRaftGauges(t *testing.T) {
	r := New()
	r.SetGauge("commit_index", 42)
	recorder := httptest.NewRecorder()
	r.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(recorder.Body.String(), "raftkv_commit_index 42") {
		t.Fatalf("missing gauge in metrics: %s", recorder.Body.String())
	}
}
