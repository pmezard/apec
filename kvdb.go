package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/cznic/kv"
)

const (
	maxKVSize = 65787
)

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

func sizePrefix(prefix []byte) []byte {
	return append(prefix, '-', 'n')
}

type Tx struct {
	db      *kv.DB
	maxSize int
	write   bool
}

func (tx *Tx) Get(prefix, key []byte) ([]byte, error) {
	var buf []byte
	keys := NewKVDBKeys(prefix, key)
	for {
		k := keys.Next()
		data, err := tx.db.Get(nil, k)
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		if buf == nil && len(data) == 0 {
			buf = []byte{}
		} else {
			buf = append(buf, data...)
		}
		if len(data) != tx.maxSize {
			break
		} else {
			buf = buf[:len(buf)-1]
		}
	}
	return buf, nil
}

func (tx *Tx) List(prefix []byte) ([]string, error) {
	p := make([]byte, len(prefix)+1)
	copy(p, prefix)
	n := []byte{0, 1, 0, 0, 0, 0}
	enum, _, err := tx.db.Seek(p)
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

func (tx *Tx) Put(prefix, key, value []byte) error {
	if !tx.write {
		panic("calling Tx.Put in a read-only transaction")
	}
	keys := NewKVDBKeys(prefix, key)
	buf := make([]byte, tx.maxSize)

	// Previous data chain may be longer than the new one
	err := tx.Delete(prefix, key)
	if err != nil {
		return err
	}
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
		err := tx.db.Set(k, data)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			break
		}
	}
	_, err = tx.db.Inc(sizePrefix(prefix), 1)
	return err
}

func (tx *Tx) Delete(prefix, key []byte) error {
	if !tx.write {
		panic("calling Delete in a read-only transaction")
	}
	buf := make([]byte, tx.maxSize)
	keys := NewKVDBKeys(prefix, key)
	for {
		k := keys.Next()
		data, err := tx.db.Get(buf, k)
		if err != nil {
			return err
		}
		if data == nil {
			break
		}
		err = tx.db.Delete(k)
		if err != nil {
			return err
		}
		if len(data) < len(buf) {
			_, err := tx.db.Inc(sizePrefix(prefix), -1)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func (tx *Tx) inc(prefix []byte, delta int64) (int64, error) {
	suffix := []byte("-seq")
	key := make([]byte, len(prefix)+len(suffix))
	copy(key, prefix)
	copy(key[len(prefix):], suffix)
	return tx.db.Inc(key, delta)
}

func (tx *Tx) IncSeq(prefix []byte, delta int64) (int64, error) {
	if !tx.write {
		panic("calling Tx.Inc in a read-only transaction")
	}
	return tx.inc(prefix, delta)
}

func (tx *Tx) GetSeq(prefix []byte) (int64, error) {
	return tx.inc(prefix, 0)
}

func (tx *Tx) Size(prefix []byte) (int64, error) {
	return tx.db.Inc(sizePrefix(prefix), 0)
}

func (tx *Tx) ListPrefixes() ([][]byte, error) {
	enum, err := tx.db.SeekFirst()
	if err != nil {
		return nil, err
	}
	prefixes := [][]byte{}
	prev := []byte{}
	for {
		k, _, err := enum.Next()
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
		pos := bytes.IndexByte(k, 0)
		if pos < 0 {
			continue
		}
		if bytes.Equal(prev, k[:pos]) {
			continue
		}
		prev = make([]byte, pos)
		copy(prev, k[:pos])
		prefixes = append(prefixes, prev)
	}
	return prefixes, nil
}

func (tx *Tx) UpdateSize() error {
	if !tx.write {
		panic("calling Tx.UpdateSize in a read-only transaction")
	}
	prefixes, err := tx.ListPrefixes()
	if err != nil {
		return err
	}
	for _, p := range prefixes {
		p = append(p, 0)
		enum, _, err := tx.db.Seek(p)
		if err != nil {
			return err
		}
		count := int64(0)
		for {
			k, _, err := enum.Next()
			if err != nil {
				if err != io.EOF {
					return err
				}
				break
			}
			if !bytes.HasPrefix(k, p) {
				continue
			}
			count++
		}
		szPrefix := sizePrefix(p)
		size, err := tx.db.Inc(szPrefix, 0)
		if err != nil {
			return err
		}
		_, err = tx.db.Inc(szPrefix, size-count)
		if err != nil {
			return err
		}
	}
	return nil
}

type KVDB struct {
	lock    sync.RWMutex
	db      *kv.DB
	maxSize int
}

func OpenKVDB(path string, maxSize int) (*KVDB, error) {
	opts := &kv.Options{
		Locker: func(name string) (io.Closer, error) {
			return ioutil.NopCloser(nil), nil
		},
	}
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

func (db *KVDB) View(action func(tx *Tx) error) error {
	db.lock.RLock()
	defer db.lock.RUnlock()
	tx := &Tx{
		db:      db.db,
		maxSize: db.maxSize,
	}
	return action(tx)
}

func (db *KVDB) Update(action func(tx *Tx) error) error {
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
	tx := &Tx{
		db:      db.db,
		maxSize: db.maxSize,
		write:   true,
	}
	err = action(tx)
	if err == nil {
		err = db.db.Commit()
		if err == nil {
			done = true
		}
	}
	return err
}

func (db *KVDB) Path() string {
	return db.db.Name()
}

func (db *KVDB) FixEmptyValues(prefix []byte) (int, error) {
	fixed := 0
	err := db.Update(func(tx *Tx) error {
		keys, err := tx.List(prefix)
		if err != nil {
			return err
		}
		for _, key := range keys {
			k := []byte(key)
			data, err := tx.Get(prefix, k)
			if len(data) == 1 {
				err = tx.Put(prefix, k, nil)
				if err != nil {
					return err
				}
				fixed++
			}
		}
		return nil
	})
	return fixed, err
}

func getKVDBVersion(db *KVDB, prefix []byte) (int, error) {
	version := 0
	err := db.View(func(tx *Tx) error {
		v, err := tx.GetSeq(prefix)
		version = int(v)
		return err
	})
	return version, err
}

func setKVDBVersion(db *KVDB, prefix []byte, version int) error {
	return db.Update(func(tx *Tx) error {
		v, err := tx.GetSeq(prefix)
		if err != nil {
			return err
		}
		current := int(v)
		if current > version {
			return fmt.Errorf("cannot downgrade version from %d to %d", current, version)
		}
		if current == version {
			return nil
		}
		_, err = tx.IncSeq(prefix, int64(version-current))
		return err
	})
}
