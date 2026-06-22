package store

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/souvikDevloper/RaftKV/internal/rpc"
	bolt "go.etcd.io/bbolt"
)

var (
	metaBucket = []byte("meta")
	logBucket  = []byte("log")
	snapBucket = []byte("snapshot")
)

type PersistentState struct {
	CurrentTerm   int64
	VotedFor      string
	CommitIndex   int64
	SnapshotIndex int64
	SnapshotTerm  int64
	Log           []rpc.LogEntry
	Snapshot      map[string]string
}

type BoltStore struct{ db *bolt.DB }

func Open(dir string) (*BoltStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "raft.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	bs := &BoltStore{db: db}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{metaBucket, logBucket, snapBucket} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return bs, nil
}

func (s *BoltStore) Close() error { return s.db.Close() }

func (s *BoltStore) Load() (*PersistentState, error) {
	ps := &PersistentState{Snapshot: map[string]string{}, Log: []rpc.LogEntry{}}
	err := s.db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		decode(mb.Get([]byte("term")), &ps.CurrentTerm)
		decode(mb.Get([]byte("voted_for")), &ps.VotedFor)
		decode(mb.Get([]byte("commit_index")), &ps.CommitIndex)
		decode(mb.Get([]byte("snapshot_index")), &ps.SnapshotIndex)
		decode(mb.Get([]byte("snapshot_term")), &ps.SnapshotTerm)
		if data := tx.Bucket(snapBucket).Get([]byte("state")); data != nil {
			if err := json.Unmarshal(data, &ps.Snapshot); err != nil {
				return err
			}
		}
		return tx.Bucket(logBucket).ForEach(func(_, value []byte) error {
			var entry rpc.LogEntry
			if err := json.Unmarshal(value, &entry); err != nil {
				return err
			}
			ps.Log = append(ps.Log, entry)
			return nil
		})
	})
	return ps, err
}

func (s *BoltStore) SaveMeta(term int64, votedFor string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		if err := putJSON(mb, "term", term); err != nil {
			return err
		}
		return putJSON(mb, "voted_for", votedFor)
	})
}

// AppendLog is the durable WAL group-commit path. A proposal batch is persisted
// in one fsync-backed Bolt transaction rather than rewriting the complete log.
func (s *BoltStore) AppendLog(entries []rpc.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		lb := tx.Bucket(logBucket)
		for _, entry := range entries {
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := lb.Put(indexKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReplaceSuffix removes conflicting WAL records at and after from, then writes
// the leader's suffix atomically.
func (s *BoltStore) ReplaceSuffix(from int64, entries []rpc.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		lb := tx.Bucket(logBucket)
		cursor := lb.Cursor()
		for key, _ := cursor.Seek(indexKey(from)); key != nil; key, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		for _, entry := range entries {
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := lb.Put(indexKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStore) SaveCommit(index int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return putJSON(tx.Bucket(metaBucket), "commit_index", index)
	})
}

func (s *BoltStore) SaveSnapshot(index, term, commit int64, state map[string]string, compactedLog []rpc.LogEntry) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		mb := tx.Bucket(metaBucket)
		for key, value := range map[string]int64{
			"snapshot_index": index,
			"snapshot_term":  term,
			"commit_index":   commit,
		} {
			if err := putJSON(mb, key, value); err != nil {
				return err
			}
		}
		data, err := json.Marshal(state)
		if err != nil {
			return err
		}
		if err := tx.Bucket(snapBucket).Put([]byte("state"), data); err != nil {
			return err
		}
		if err := tx.DeleteBucket(logBucket); err != nil {
			return err
		}
		lb, err := tx.CreateBucket(logBucket)
		if err != nil {
			return err
		}
		for _, entry := range compactedLog {
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := lb.Put(indexKey(entry.Index), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func indexKey(index int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(index))
	return key
}

func putJSON(bucket *bolt.Bucket, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(key), data)
}

func decode(data []byte, target any) {
	if data != nil {
		_ = json.Unmarshal(data, target)
	}
}
