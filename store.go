package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

type Store struct {
	db *bolt.DB
}

var (
	metaBucket        = []byte("meta")
	offersBucket      = []byte("offers")
	deletedBucket     = []byte("deleted")
	deletedKeysBucket = []byte("deleted_keys")
	locationsBucket   = []byte("locations")

	buckets = [][]byte{
		metaBucket,
		offersBucket,
		deletedBucket,
		deletedKeysBucket,
		locationsBucket,
	}

	storeVersion = 3
)

func UpgradeStore(path string) (*Store, error) {
	exists, err := isFile(path)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	store := &Store{
		db: db,
	}
	err = store.db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range buckets {
			_, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if !exists {
		err = store.SetVersion(storeVersion)
		if err != nil {
			return nil, err
		}
	}
	db = nil
	return store, nil
}

func OpenStore(dir string) (*Store, error) {
	store, err := UpgradeStore(dir)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			store.Close()
		}
	}()
	version, err := store.Version()
	if err != nil {
		return nil, err
	}
	if version != storeVersion {
		return nil, fmt.Errorf("expected store version %d, got %d", storeVersion, version)
	}
	ok = true
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Path() string {
	return s.db.Path()
}

func (s *Store) getJson(tx *bolt.Tx, bucket []byte, key []byte,
	output interface{}) (bool, error) {
	data := tx.Bucket(bucket).Get(key)
	if data == nil {
		return false, nil
	}
	return true, json.Unmarshal(data, output)
}

func (s *Store) putJson(tx *bolt.Tx, bucket []byte, key []byte,
	input interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return tx.Bucket(bucket).Put(key, data)
}

func (s *Store) Put(id string, data []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		key := []byte(id)
		// Invalidate cached location
		err := tx.Bucket(locationsBucket).Delete(key)
		if err != nil {
			return err
		}
		return tx.Bucket(offersBucket).Put(key, data)
	})
}

func (s *Store) Has(id string) (bool, error) {
	ok := false
	err := s.db.View(func(tx *bolt.Tx) error {
		temp := tx.Bucket(offersBucket).Get([]byte(id))
		ok = len(temp) > 0
		return nil
	})
	return ok, err
}

func (s *Store) Get(id string) ([]byte, error) {
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

func uintToBytes(id uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, id)
	return buf[:n]
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

func (s *Store) Delete(id string, now time.Time) error {
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
		err = tx.Bucket(deletedBucket).Put(uintToBytes(deletedId), data)
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
			Date: now.Format(time.RFC3339),
		})
		err = s.putJson(tx, deletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		// Delete cached location
		err = tx.Bucket(locationsBucket).Delete(key)
		if err != nil {
			return err
		}
		// Delete the live offer
		return tx.Bucket(offersBucket).Delete(key)
	})
}

func (s *Store) ListDeletedIds() ([]string, error) {
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

func (s *Store) ListDeletedOffers(id string) ([]DeletedOffer, error) {
	deletedKeys := &deletedOffers{}
	err := s.db.View(func(tx *bolt.Tx) error {
		deleted := tx.Bucket(deletedKeysBucket)
		data := deleted.Get([]byte(id))
		if data == nil {
			return nil
		}
		return json.Unmarshal(data, deletedKeys)
	})
	return []DeletedOffer(deletedKeys.Ids), err
}

func (s *Store) GetDeleted(id uint64) ([]byte, error) {
	var data []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		temp := tx.Bucket(deletedBucket).Get(uintToBytes(id))
		if temp != nil {
			data = make([]byte, len(temp))
			copy(data, temp)
		}
		return nil
	})
	return data, err
}

func (s *Store) List() ([]string, error) {
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

func (s *Store) Size() int {
	n := 0
	s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(offersBucket).Stats().KeyN
		return nil
	})
	return n
}

type storeMeta struct {
	Version int `json:"version"`
}

func (s *Store) Version() (int, error) {
	version := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		meta := &storeMeta{}
		_, err := s.getJson(tx, metaBucket, []byte("version"), meta)
		version = meta.Version
		return err
	})
	return version, err
}

func (s *Store) SetVersion(version int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		meta := &storeMeta{
			Version: version,
		}
		return s.putJson(tx, metaBucket, []byte("version"), meta)
	})
}

func (s *Store) PutLocation(id string, loc *Location, date time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		k := []byte(id)
		data := tx.Bucket(offersBucket).Get(k)
		if data == nil {
			return fmt.Errorf("cannot add location for unknown offer %s", id)
		}

		w := bytes.NewBuffer(nil)
		if loc != nil {
			err := writeBinaryLocation(w, loc)
			if err != nil {
				return err
			}
			ts := date.Unix()
			err = binary.Write(w, binary.LittleEndian, &ts)
			if err != nil {
				return err
			}
		}
		return tx.Bucket(locationsBucket).Put(k, w.Bytes())
	})
}

func (s *Store) GetLocation(id string) (*Location, time.Time, error) {
	var p *Location
	var date time.Time
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(locationsBucket).Get([]byte(id))
		if len(data) == 0 {
			if data != nil {
				date = time.Unix(1, 0)
			}
			return nil
		}
		r := bytes.NewBuffer(data)
		point, err := readBinaryLocation(r)
		if err != nil {
			return err
		}
		ts := int64(0)
		err = binary.Read(r, binary.LittleEndian, &ts)
		if err != nil {
			return err
		}
		date = time.Unix(ts, 0)
		p = point
		return nil
	})
	return p, date, err
}

func (s *Store) DeleteLocations() error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(locationsBucket)
	})
	return err
}
