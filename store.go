package main

import (
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"time"
)

type Store struct {
	db *KVDB
}

var (
	kvMetaBucket        = []byte("m")
	kvOffersBucket      = []byte("o")
	kvDeletedBucket     = []byte("d")
	kvDeletedKeysBucket = []byte("dk")
)

func OpenStore(dir string) (*Store, error) {
	db, err := OpenKVDB(filepath.Join(dir, "kv"), 0)
	if err != nil {
		return nil, err
	}
	return &Store{
		db: db,
	}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Path() string {
	return s.db.Path()
}

func (s *Store) getJson(prefix []byte, key []byte, output interface{}) (
	bool, error) {

	data, err := s.db.Get(prefix, key)
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil
	}
	return true, json.Unmarshal(data, output)
}

func (s *Store) putJson(prefix []byte, key []byte, input interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return s.db.Put(prefix, key, data)
}

type kvStoreMeta struct {
	Version int `json:"version"`
}

func (s *Store) getVersion() (int, error) {
	meta := &kvStoreMeta{}
	_, err := s.getJson(kvMetaBucket, []byte("version"), meta)
	return meta.Version, err
}

func (s *Store) setVersion(version int) error {
	meta := &kvStoreMeta{
		Version: version,
	}
	return s.putJson(kvMetaBucket, []byte("version"), meta)
}

func (s *Store) Put(id string, data []byte) error {
	return s.db.Update(func() error {
		return s.db.Put(kvOffersBucket, []byte(id), data)
	})
}

func (s *Store) Has(id string) (bool, error) {
	ok := false
	err := s.db.View(func() error {
		data, err := s.db.Get(kvOffersBucket, []byte(id))
		if err != nil {
			return err
		}
		ok = data != nil
		return nil
	})
	return ok, err
}

func (s *Store) Get(id string) ([]byte, error) {
	var data []byte
	err := s.db.View(func() error {
		d, err := s.db.Get(kvOffersBucket, []byte(id))
		data = d
		return err
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
	return s.db.Update(func() error {
		key := []byte(id)
		data, err := s.db.Get(kvOffersBucket, key)
		if err != nil {
			return err
		}
		if data == nil {
			return nil
		}
		// Move data in "deleted" table
		deletedId, err := s.db.Inc(kvDeletedBucket, 1)
		if err != nil {
			return err
		}
		err = s.db.Put(kvDeletedBucket, uintToBytes(uint64(deletedId)), data)
		if err != nil {
			return err
		}
		// Update offer id to deleted virtual ids mapping
		deletedKeys := &deletedOffers{}
		_, err = s.getJson(kvDeletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		deletedKeys.Ids = append(deletedKeys.Ids, DeletedOffer{
			Id:   uint64(deletedId),
			Date: now.Format(time.RFC3339),
		})
		err = s.putJson(kvDeletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		// Delete the live offer
		return s.db.Delete(kvOffersBucket, key)
	})
}

func (s *Store) ListDeletedIds() ([]string, error) {
	var err error
	ids := []string{}
	err = s.db.View(func() error {
		ids, err = s.db.List(kvDeletedKeysBucket)
		return err
	})
	return ids, err
}

func (s *Store) ListDeletedOffers(id string) ([]DeletedOffer, error) {
	deletedKeys := &deletedOffers{}
	err := s.db.View(func() error {
		data, err := s.db.Get(kvDeletedKeysBucket, []byte(id))
		if err != nil {
			return err
		}
		return json.Unmarshal(data, deletedKeys)
	})
	return []DeletedOffer(deletedKeys.Ids), err
}

func (s *Store) GetDeleted(id uint64) ([]byte, error) {
	var err error
	var data []byte
	err = s.db.View(func() error {
		data, err = s.db.Get(kvDeletedBucket, uintToBytes(id))
		return err
	})
	return data, err
}

func (s *Store) List() ([]string, error) {
	var err error
	var ids []string
	err = s.db.View(func() error {
		ids, err = s.db.List(kvOffersBucket)
		return err
	})
	return ids, err
}

func (s *Store) Size() int {
	n := 0
	s.db.View(func() error {
		keys, err := s.db.List(kvOffersBucket)
		if err != nil {
			n = len(keys)
		}
		return err
	})
	return n
}
