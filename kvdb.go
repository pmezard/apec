package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/cznic/kv"
)

const (
	maxKVSize = 65787
)

type KVDB struct {
	lock    sync.RWMutex
	db      *kv.DB
	maxSize int
}

func OpenKVDB(path string, maxSize int) (*KVDB, error) {
	opts := &kv.Options{}
	db, err := kv.Create(path, opts)
	if err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
		db, err = kv.Open(path, opts)
		if err != nil {
			return nil, err
		}
	}
	if maxSize <= 1 {
		maxSize = maxKVSize
	}
	return &KVDB{
		db:      db,
		maxSize: maxSize,
	}, nil
}

func (db *KVDB) Close() error {
	return db.db.Close()
}

func (db *KVDB) View(action func() error) error {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return action()
}

func (db *KVDB) Update(action func() error) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	done := false
	err := db.db.BeginTransaction()
	if err != nil {
		return err
	}
	defer func() {
		if !done {
			e := db.db.Rollback()
			if err == nil && e != nil {
				err = e
			}
		}
	}()
	err = action()
	if err == nil {
		err = db.db.Commit()
		if err == nil {
			done = true
		}
	}
	return err
}

type KVDBKeys struct {
	buf       []byte
	prefixLen int
	i         uint32
}

func NewKVDBKeys(prefix, key []byte) KVDBKeys {
	prefixLen := len(prefix) + 1 + len(key) + 1
	buf := make([]byte, prefixLen+binary.MaxVarintLen32)
	copy(buf, prefix)
	copy(buf[len(prefix)+1:], key)
	return KVDBKeys{
		buf:       buf,
		prefixLen: prefixLen,
	}
}

func (keys *KVDBKeys) Next() []byte {
	keys.i++
	binary.PutUvarint(keys.buf[keys.prefixLen:], uint64(keys.i))
	return keys.buf
}

func (db *KVDB) Put(prefix, key, value []byte) error {
	keys := NewKVDBKeys(prefix, key)
	buf := make([]byte, db.maxSize)

	for {
		data := buf
		if len(value) >= len(data) {
			k := len(data) - 1
			copy(data, value[:k])
			value = value[k:]
		} else {
			copy(data, value)
			data = data[:len(value)]
			value = nil
		}
		k := keys.Next()
		err := db.db.Set(k, data)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			break
		}
	}
	return nil
}

func (db *KVDB) Get(prefix, key []byte) ([]byte, error) {
	var buf []byte
	keys := NewKVDBKeys(prefix, key)
	for {
		k := keys.Next()
		data, err := db.db.Get(nil, k)
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		buf = append(buf, data...)
		if len(data) != db.maxSize {
			break
		} else {
			buf = buf[:len(buf)-1]
		}
	}
	return buf, nil
}

func (db *KVDB) Delete(prefix, key []byte) error {
	buf := make([]byte, db.maxSize)
	keys := NewKVDBKeys(prefix, key)
	for {
		k := keys.Next()
		data, err := db.db.Get(buf, k)
		if err != nil {
			return err
		}
		if data == nil {
			break
		}
		err = db.db.Delete(k)
		if err != nil {
			return err
		}
		if len(data) < len(buf) {
			break
		}
	}
	return nil
}

func (db *KVDB) Inc(prefix []byte, delta int64) (int64, error) {
	suffix := []byte("-seq")
	key := make([]byte, len(prefix)+len(suffix))
	copy(key, prefix)
	copy(key[len(prefix):], suffix)
	return db.db.Inc(key, delta)
}

func (db *KVDB) List(prefix []byte) ([]string, error) {
	p := make([]byte, len(prefix)+1)
	copy(p, prefix)
	n := []byte{0, 1, 0, 0, 0, 0}
	enum, _, err := db.db.Seek(p)
	if err != nil {
		return nil, err
	}
	keys := []string{}
	for {
		k, _, err := enum.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if !bytes.HasPrefix(k, p) {
			break
		}
		if !bytes.HasSuffix(k, n) {
			// List only starting key
			continue
		}
		k = k[len(p):]
		k = k[:len(k)-len(n)]
		keys = append(keys, string(k))
	}
	return keys, nil
}

func (db *KVDB) Path() string {
	return db.db.Name()
}
