package attic

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type BoltStore struct {
	db *bolt.DB
}

var (
	metaBucket        = []byte("meta")
	offersBucket      = []byte("offers")
	deletedBucket     = []byte("deleted")
	deletedKeysBucket = []byte("deleted_keys")
)

func OpenStore(dir string) (*BoltStore, error) {
	db, err := bolt.Open(dir, 0666, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	store := &BoltStore{
		db: db,
	}
	err = store.Upgrade()
	if err != nil {
		return nil, err
	}
	db = nil
	return store, nil
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) Path() string {
	return s.db.Path()
}

func (s *BoltStore) getJson(tx *bolt.Tx, bucket []byte, key []byte,
	output interface{}) (bool, error) {
	data := tx.Bucket(bucket).Get(key)
	if data == nil {
		return false, nil
	}
	return true, json.Unmarshal(data, output)
}

func (s *BoltStore) putJson(tx *bolt.Tx, bucket []byte, key []byte,
	input interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return tx.Bucket(bucket).Put(key, data)
}

type storeMeta struct {
	Version int `json:"version"`
}

func (s *BoltStore) getVersion(tx *bolt.Tx) (int, error) {
	meta := &storeMeta{}
	_, err := s.getJson(tx, metaBucket, []byte("version"), meta)
	return meta.Version, err
}

func (s *BoltStore) setVersion(tx *bolt.Tx, version int) error {
	meta := &storeMeta{
		Version: version,
	}
	return s.putJson(tx, metaBucket, []byte("version"), meta)
}

func (s *BoltStore) Put(id string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(offersBucket).Put([]byte(id), data)
	})
}

func (s *BoltStore) Has(id string) (bool, error) {
	ok := false
	err := s.db.View(func(tx *bolt.Tx) error {
		temp := tx.Bucket(offersBucket).Get([]byte(id))
		ok = len(temp) > 0
		return nil
	})
	return ok, err
}

func (s *BoltStore) Get(id string) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		temp := tx.Bucket(offersBucket).Get([]byte(id))
		if temp != nil {
			data = make([]byte, len(temp))
			copy(data, temp)
		}
		return nil
	})
	return data, err
}

type DeletedOffer struct {
	Id   uint64 `json:"id"`
	Date string `json:"date"`
}

// deletedOffers maps its key offer identifier to deleted virtual identifiers.
// In theory, offers should be deleted only once, but I do not know APEC data
// structure. Better be safe than sorry.
type deletedOffers struct {
	Ids []DeletedOffer `json:"ids"`
}

func uint64ToBytes(id uint64) []byte {
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, &id)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (s *BoltStore) Delete(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		key := []byte(id)
		data := tx.Bucket(offersBucket).Get(key)
		if data == nil {
			return nil
		}
		// Move data in "deleted" table
		deleted := tx.Bucket(deletedBucket)
		deletedId, err := deleted.NextSequence()
		if err != nil {
			return err
		}
		err = tx.Bucket(deletedBucket).Put(uint64ToBytes(deletedId), data)
		if err != nil {
			return err
		}
		// Update offer id to deleted virtual ids mapping
		deletedKeys := &deletedOffers{}
		_, err = s.getJson(tx, deletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		deletedKeys.Ids = append(deletedKeys.Ids, DeletedOffer{
			Id:   deletedId,
			Date: time.Now().Format(time.RFC3339),
		})
		err = s.putJson(tx, deletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		// Delete the live offer
		return tx.Bucket(offersBucket).Delete(key)
	})
}

func (s *BoltStore) ListDeletedIds() ([]string, error) {
	ids := []string{}
	err := s.db.View(func(tx *bolt.Tx) error {
		deleted := tx.Bucket(deletedKeysBucket)
		return deleted.ForEach(func(k, v []byte) error {
			ids = append(ids, string(k))
			return nil
		})
	})
	return ids, err
}

func (s *BoltStore) ListDeletedOffers(id string) ([]DeletedOffer, error) {
	deletedKeys := &deletedOffers{}
	err := s.db.View(func(tx *bolt.Tx) error {
		deleted := tx.Bucket(deletedKeysBucket)
		data := deleted.Get([]byte(id))
		return json.Unmarshal(data, deletedKeys)
	})
	return []DeletedOffer(deletedKeys.Ids), err
}

func (s *BoltStore) GetDeleted(id uint64) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		temp := tx.Bucket(deletedBucket).Get(uint64ToBytes(id))
		if temp != nil {
			data = make([]byte, len(temp))
			copy(data, temp)
		}
		return nil
	})
	return data, err
}

func (s *BoltStore) List() ([]string, error) {
	var ids []string
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(offersBucket)
		size := bucket.Stats().KeyN
		ids = make([]string, 0, size)
		return bucket.ForEach(func(k, v []byte) error {
			ids = append(ids, string(k))
			return nil
		})
	})
	return ids, err
}

func (s *BoltStore) Upgrade() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var err error
		version := 0
		bucket := tx.Bucket(metaBucket)
		if bucket == nil {
			_, err = tx.CreateBucketIfNotExists(metaBucket)
			if err != nil {
				return err
			}
		} else {
			version, err = s.getVersion(tx)
			if err != nil {
				return err
			}
		}
		if version == 0 {
			fmt.Printf("upgrading store to version 1\n")
			_, err := tx.CreateBucketIfNotExists(offersBucket)
			if err != nil {
				return err
			}
			version = 1
		}
		if version == 1 {
			fmt.Printf("upgrading store to version 2\n")
			_, err = tx.CreateBucketIfNotExists(deletedBucket)
			if err != nil {
				return err
			}
			_, err = tx.CreateBucketIfNotExists(deletedKeysBucket)
			if err != nil {
				return err
			}
			version = 2
		}
		return s.setVersion(tx, version)
	})
}

func (s *BoltStore) Size() int {
	n := 0
	s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(offersBucket).Stats().KeyN
		return nil
	})
	return n
}
