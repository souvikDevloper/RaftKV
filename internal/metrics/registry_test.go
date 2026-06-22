package metrics

import (
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
