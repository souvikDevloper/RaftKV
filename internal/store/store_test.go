package store

import (
	"github.com/souvikDevloper/RaftKV/internal/rpc"
	"testing"
)

func TestBoltStorePersistsLogAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	bs, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.SaveMeta(3, "n2"); err != nil {
		t.Fatal(err)
	}
	log := []rpc.LogEntry{{Index: 1, Term: 1, Op: "put", Key: "x", Value: "1"}}
	if err := bs.AppendLog(log); err != nil {
		t.Fatal(err)
	}
	if err := bs.SaveCommit(1); err != nil {
		t.Fatal(err)
	}
	if err := bs.SaveSnapshot(1, 1, 1, map[string]string{"x": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	bs.Close()
	bs, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	ps, err := bs.Load()
	if err != nil {
		t.Fatal(err)
	}
	if ps.CurrentTerm != 3 || ps.VotedFor != "n2" {
		t.Fatalf("bad meta: %+v", ps)
	}
	if ps.SnapshotIndex != 1 || ps.CommitIndex != 1 || ps.Snapshot["x"] != "1" {
		t.Fatalf("bad snapshot: %+v", ps)
	}
}
