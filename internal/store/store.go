package store

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
	bolt "go.etcd.io/bbolt"
)

var metaBucket = []byte("meta")
var logBucket = []byte("log")
var snapBucket = []byte("snapshot")

type PersistentState struct {
	CurrentTerm   int64
	VotedFor      string
	SnapshotIndex int64
	SnapshotTerm  int64
	Log           []rpc.LogEntry
	Snapshot      map[string]string
}

type BoltStore struct{ db *bolt.DB }

func Open(dir string) (*BoltStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "raft.db"), 0600, nil)
	if err != nil {
		return nil, err
	}
	bs := &BoltStore{db: db}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(metaBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(logBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(snapBucket); err != nil {
			return err
		}
		return nil
	})
	return bs, err
}
func (s *BoltStore) Close() error { return s.db.Close() }

func (s *BoltStore) Load() (*PersistentState, error) {
	ps := &PersistentState{VotedFor: "", Snapshot: map[string]string{}, Log: []rpc.LogEntry{}}
	err := s.db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		_ = json.Unmarshal(mb.Get([]byte("term")), &ps.CurrentTerm)
		_ = json.Unmarshal(mb.Get([]byte("voted_for")), &ps.VotedFor)
		_ = json.Unmarshal(mb.Get([]byte("snapshot_index")), &ps.SnapshotIndex)
		_ = json.Unmarshal(mb.Get([]byte("snapshot_term")), &ps.SnapshotTerm)
		lb := tx.Bucket(logBucket)
		return lb.ForEach(func(k, v []byte) error {
			var e rpc.LogEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			ps.Log = append(ps.Log, e)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	_ = s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(snapBucket).Get([]byte("state"))
		if data != nil {
			_ = json.Unmarshal(data, &ps.Snapshot)
		}
		return nil
	})
	return ps, nil
}

func (s *BoltStore) SaveMeta(term int64, votedFor string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		b, _ := json.Marshal(term)
		if err := mb.Put([]byte("term"), b); err != nil {
			return err
		}
		b, _ = json.Marshal(votedFor)
		return mb.Put([]byte("voted_for"), b)
	})
}

func (s *BoltStore) SaveLog(log []rpc.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		tx.DeleteBucket(logBucket)
		lb, err := tx.CreateBucket(logBucket)
		if err != nil {
			return err
		}
		for _, e := range log {
			b, _ := json.Marshal(e)
			key := []byte(formatIndex(e.Index))
			if err := lb.Put(key, b); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStore) SaveSnapshot(index, term int64, state map[string]string, compactedLog []rpc.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		b, _ := json.Marshal(index)
		if err := mb.Put([]byte("snapshot_index"), b); err != nil {
			return err
		}
		b, _ = json.Marshal(term)
		if err := mb.Put([]byte("snapshot_term"), b); err != nil {
			return err
		}
		sb := tx.Bucket(snapBucket)
		b, _ = json.Marshal(state)
		if err := sb.Put([]byte("state"), b); err != nil {
			return err
		}
		tx.DeleteBucket(logBucket)
		lb, err := tx.CreateBucket(logBucket)
		if err != nil {
			return err
		}
		for _, e := range compactedLog {
			b, _ := json.Marshal(e)
			if err := lb.Put([]byte(formatIndex(e.Index)), b); err != nil {
				return err
			}
		}
		return nil
	})
}

func formatIndex(i int64) string { return fmtIndex(i) }
func fmtIndex(i int64) string {
	return string([]byte{byte(i >> 56), byte(i >> 48), byte(i >> 40), byte(i >> 32), byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
}
