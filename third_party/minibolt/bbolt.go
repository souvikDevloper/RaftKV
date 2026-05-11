package bbolt

import (
	"encoding/json"
	"errors"
	"os"
	"sync"
)

type Options struct{}
type DB struct {
	path    string
	mu      sync.RWMutex
	buckets map[string]map[string][]byte
}
type Tx struct {
	db       *DB
	writable bool
}
type Bucket struct {
	tx   *Tx
	name string
}

func Open(path string, mode os.FileMode, opts *Options) (*DB, error) {
	db := &DB{path: path, buckets: map[string]map[string][]byte{}}
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &db.buckets)
	}
	return db, nil
}
func (db *DB) Close() error { return nil }
func (db *DB) View(fn func(*Tx) error) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return fn(&Tx{db: db, writable: false})
}
func (db *DB) Update(fn func(*Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := fn(&Tx{db: db, writable: true}); err != nil {
		return err
	}
	return db.persist()
}
func (db *DB) persist() error {
	data, err := json.MarshalIndent(db.buckets, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(db.path, data, 0600)
}
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	if !tx.writable {
		return nil, errors.New("read-only tx")
	}
	s := string(name)
	if tx.db.buckets[s] == nil {
		tx.db.buckets[s] = map[string][]byte{}
	}
	return &Bucket{tx: tx, name: s}, nil
}
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if !tx.writable {
		return nil, errors.New("read-only tx")
	}
	s := string(name)
	tx.db.buckets[s] = map[string][]byte{}
	return &Bucket{tx: tx, name: s}, nil
}
func (tx *Tx) DeleteBucket(name []byte) error {
	if !tx.writable {
		return errors.New("read-only tx")
	}
	delete(tx.db.buckets, string(name))
	return nil
}
func (tx *Tx) Bucket(name []byte) *Bucket {
	s := string(name)
	if tx.db.buckets[s] == nil {
		return nil
	}
	return &Bucket{tx: tx, name: s}
}
func (b *Bucket) Put(k, v []byte) error {
	if !b.tx.writable {
		return errors.New("read-only tx")
	}
	vv := make([]byte, len(v))
	copy(vv, v)
	b.tx.db.buckets[b.name][string(k)] = vv
	return nil
}
func (b *Bucket) Get(k []byte) []byte {
	v := b.tx.db.buckets[b.name][string(k)]
	if v == nil {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	for k, v := range b.tx.db.buckets[b.name] {
		if err := fn([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}
