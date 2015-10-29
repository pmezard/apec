package main

import (
	"encoding/binary"
	"encoding/json"
)

// IndexQueue represents a persistent sequence of indexing operations to
// perform.
type IndexQueue struct {
	db *KVDB
}

var (
	queuedBucket = []byte("q")
	minSeqBucket = []byte("s")
	minSeqKey    = []byte("m")
)

func OpenIndexQueue(path string) (*IndexQueue, error) {
	db, err := OpenKVDB(path, 0)
	if err != nil {
		return nil, err
	}
	return &IndexQueue{
		db: db,
	}, nil
}

func (q *IndexQueue) Close() error {
	return q.db.Close()
}

type Op uint8

const (
	AddOp Op = iota
	RemoveOp
)

// Queued describes a single indexing operation on a specified document. Seq
// field should not be set by the caller, it is returned by FetchMany and can
// be used to delete entries.
type Queued struct {
	Seq uint64 `json:"seq"`
	Id  string `json:"id"`
	Op  Op     `json:"op"`
}

func (q *IndexQueue) getMinSeq(tx *Tx) (uint64, bool, error) {
	data, err := tx.Get(minSeqBucket, minSeqKey)
	if err != nil || data == nil {
		return 0, false, err
	}
	seq, _ := binary.Uvarint(data)
	return seq, seq != 0, nil
}

func (q *IndexQueue) putMinSeq(tx *Tx, seqBytes []byte) error {
	return tx.Put(minSeqBucket, minSeqKey, seqBytes)
}

func (q *IndexQueue) QueueMany(items []Queued) error {
	return q.db.Update(func(tx *Tx) error {
		for i, item := range items {
			seq, err := tx.IncSeq(queuedBucket, 1)
			if err != nil {
				return err
			}
			item.Seq = uint64(seq)
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, item.Seq)
			if i == 0 {
				// Maybe min seq is not set yet
				_, ok, err := q.getMinSeq(tx)
				if err != nil {
					return err
				}
				if !ok {
					err = q.putMinSeq(tx, buf[:n])
					if err != nil {
						return err
					}
				}
			}
			data, err := json.Marshal(&item)
			if err != nil {
				return err
			}
			err = tx.Put(queuedBucket, buf[:n], data)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (q *IndexQueue) FetchMany(count int) ([]Queued, error) {
	queued := []Queued{}
	err := q.db.View(func(tx *Tx) error {
		seq, ok, err := q.getMinSeq(tx)
		if !ok || err != nil {
			// Nothing to fetch
			return err
		}
		item := Queued{}
		for ; count > 0; count-- {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, seq)
			data, err := tx.Get(queuedBucket, buf[:n])
			if err != nil {
				return err
			}
			if data == nil {
				break
			}
			err = json.Unmarshal(data, &item)
			if err != nil {
				return err
			}
			queued = append(queued, item)
			seq++
		}
		return nil
	})
	return queued, err
}

func (q *IndexQueue) DeleteMany(count int) error {
	return q.db.Update(func(tx *Tx) error {
		minSeq, ok, err := q.getMinSeq(tx)
		if !ok || err != nil {
			return nil
		}
		for ; count > 0; count-- {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, minSeq)
			data, err := tx.Get(queuedBucket, buf[:n])
			if err != nil {
				return err
			}
			if data == nil {
				break
			}
			err = tx.Delete(queuedBucket, buf[:n])
			if err != nil {
				return err
			}
			minSeq++
		}
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, minSeq)
		return q.putMinSeq(tx, buf[:n])
	})
}

func (q *IndexQueue) Size() int {
	size := 0
	err := q.db.View(func(tx *Tx) error {
		s, err := tx.Size(queuedBucket)
		size = int(s)
		return err
	})
	if err != nil {
		return -1
	}
	return size
}

func (q *IndexQueue) Path() string {
	return q.db.Path()
}
