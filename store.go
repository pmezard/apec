package main

import (
	"encoding/json"

	"github.com/boltdb/bolt"
)

type Store struct {
	db *bolt.DB
}

var (
	metaBucket   = []byte("meta")
	offersBucket = []byte("offers")
)

func OpenStore(dir string) (*Store, error) {
	db, err := bolt.Open(dir, 0666, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	_, err = tx.CreateBucketIfNotExists(metaBucket)
	if err != nil {
		return nil, err
	}
	store := &Store{
		db: db,
	}
	err = store.setVersion(tx, 0)
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	err = store.Upgrade()
	if err != nil {
		return nil, err
	}
	db = nil
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) getJson(tx *bolt.Tx, bucket []byte, key []byte,
	output interface{}) error {
	data := tx.Bucket(bucket).Get(key)
	return json.Unmarshal(data, output)
}

func (s *Store) putJson(tx *bolt.Tx, bucket []byte, key []byte,
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

func (s *Store) getVersion(tx *bolt.Tx) (int, error) {
	meta := &storeMeta{}
	err := s.getJson(tx, metaBucket, []byte("version"), meta)
	return meta.Version, err
}

func (s *Store) setVersion(tx *bolt.Tx, version int) error {
	meta := &storeMeta{
		Version: version,
	}
	return s.putJson(tx, metaBucket, []byte("version"), meta)
}

func (s *Store) Put(id string, data []byte) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = tx.Bucket(offersBucket).Put([]byte(id), data)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Has(id string) (bool, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	temp := tx.Bucket(offersBucket).Get([]byte(id))
	return len(temp) > 0, nil
}

func (s *Store) Get(id string) ([]byte, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	temp := tx.Bucket(offersBucket).Get([]byte(id))
	data := make([]byte, len(temp))
	copy(data, temp)
	return data, nil
}

func (s *Store) Delete(id string) error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = tx.Bucket(offersBucket).Delete([]byte(id))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) List() ([]string, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(offersBucket)
	size := bucket.Stats().KeyN
	ids := make([]string, 0, size)
	err = bucket.ForEach(func(k, v []byte) error {
		ids = append(ids, string(k))
		return nil
	})
	return ids, err
}

func (s *Store) Upgrade() error {
	tx, err := s.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	version, err := s.getVersion(tx)
	if err != nil {
		return err
	}
	if version == 0 {
		_, err := tx.CreateBucketIfNotExists(offersBucket)
		if err != nil {
			return err
		}
		version = 1
	}
	err = s.setVersion(tx, version)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Size() int {
	tx, err := s.db.Begin(false)
	if err != nil {
		return -1
	}
	defer tx.Rollback()
	return tx.Bucket(offersBucket).Stats().KeyN
}
