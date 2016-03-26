package main

import (
	"encoding/binary"
	"encoding/json"

	"github.com/boltdb/bolt"
)

// IndexQueue represents a persistent sequence of indexing operations to
// perform.
type IndexQueue struct {
	db *bolt.DB
}

var (
	queuedBucket = []byte("q")
	minSeqBucket = []byte("s")
	minSeqKey    = []byte("m")

	queueBuckets = [][]byte{
		queuedBucket,
		minSeqBucket,
		minSeqKey,
	}
)

func OpenIndexQueue(path string) (*IndexQueue, error) {
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range queueBuckets {
			_, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
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

func (q *IndexQueue) getMinSeq(tx *bolt.Tx) (uint64, bool) {
	data := tx.Bucket(minSeqBucket).Get(minSeqKey)
	if data == nil {
		return 0, false
	}
	seq, _ := binary.Uvarint(data)
	return seq, seq != 0
}

func (q *IndexQueue) putMinSeq(tx *bolt.Tx, seqBytes []byte) error {
	return tx.Bucket(minSeqBucket).Put(minSeqKey, seqBytes)
}

func (q *IndexQueue) QueueMany(items []Queued) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		for i, item := range items {
			seq, err := tx.Bucket(queuedBucket).NextSequence()
			if err != nil {
				return err
			}
			item.Seq = seq
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, item.Seq)
			if i == 0 {
				// Maybe min seq is not set yet
				_, ok := q.getMinSeq(tx)
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
			err = tx.Bucket(queuedBucket).Put(buf[:n], data)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (q *IndexQueue) FetchMany(count int) ([]Queued, error) {
	queued := []Queued{}
	err := q.db.View(func(tx *bolt.Tx) error {
		seq, ok := q.getMinSeq(tx)
		if !ok {
			// Nothing to fetch
			return nil
		}
		item := Queued{}
		for ; count > 0; count-- {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, seq)
			data := tx.Bucket(queuedBucket).Get(buf[:n])
			if data == nil {
				break
			}
			err := json.Unmarshal(data, &item)
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
	return q.db.Update(func(tx *bolt.Tx) error {
		minSeq, ok := q.getMinSeq(tx)
		if !ok {
			return nil
		}
		for ; count > 0; count-- {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, minSeq)
			data := tx.Bucket(queuedBucket).Get(buf[:n])
			if data == nil {
				break
			}
			err := tx.Bucket(queuedBucket).Delete(buf[:n])
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
	err := q.db.View(func(tx *bolt.Tx) error {
		size = tx.Bucket(queuedBucket).Stats().KeyN
		return nil
	})
	if err != nil {
		return -1
	}
	return size
}

func (q *IndexQueue) Path() string {
	return q.db.Path()
}
