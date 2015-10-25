package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Store struct {
	db *KVDB
}

var (
	kvMetaBucket        = []byte("m")
	kvOffersBucket      = []byte("o")
	kvLocationsBucket   = []byte("l")
	kvDeletedBucket     = []byte("d")
	kvDeletedKeysBucket = []byte("dk")
	storeVersion        = 3
)

func UpgradeStore(dir string) (*Store, error) {
	created := false
	path := filepath.Join(dir, "kv")
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		created = true
	}
	db, err := OpenKVDB(path, 0)
	if err != nil {
		return nil, err
	}
	store := &Store{
		db: db,
	}
	if created {
		err = store.SetVersion(storeVersion)
		if err != nil {
			store.Close()
			return nil, err
		}
	}
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

func (s *Store) getJson(tx *Tx, prefix []byte, key []byte, output interface{}) (
	bool, error) {

	data, err := tx.Get(prefix, key)
	if err != nil {
		return false, err
	}
	if data == nil {
		return false, nil
	}
	return true, json.Unmarshal(data, output)
}

func (s *Store) putJson(tx *Tx, prefix []byte, key []byte, input interface{}) error {
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return tx.Put(prefix, key, data)
}

type kvStoreMeta struct {
	Version int `json:"version"`
}

func (s *Store) getVersion(tx *Tx) (int, error) {
	meta := &kvStoreMeta{}
	_, err := s.getJson(tx, kvMetaBucket, []byte("version"), meta)
	return meta.Version, err
}

func (s *Store) setVersion(tx *Tx, version int) error {
	meta := &kvStoreMeta{
		Version: version,
	}
	return s.putJson(tx, kvMetaBucket, []byte("version"), meta)
}

func (s *Store) Put(id string, data []byte) error {
	return s.db.Update(func(tx *Tx) error {
		k := []byte(id)
		// Invalid cached location
		err := tx.Delete(kvLocationsBucket, k)
		if err != nil {
			return err
		}
		return tx.Put(kvOffersBucket, k, data)
	})
}

func (s *Store) Has(id string) (bool, error) {
	ok := false
	err := s.db.View(func(tx *Tx) error {
		data, err := tx.Get(kvOffersBucket, []byte(id))
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
	err := s.db.View(func(tx *Tx) error {
		d, err := tx.Get(kvOffersBucket, []byte(id))
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
	return s.db.Update(func(tx *Tx) error {
		key := []byte(id)
		data, err := tx.Get(kvOffersBucket, key)
		if err != nil {
			return err
		}
		if data == nil {
			return nil
		}
		// Move data in "deleted" table
		deletedId, err := tx.IncSeq(kvDeletedBucket, 1)
		if err != nil {
			return err
		}
		err = tx.Put(kvDeletedBucket, uintToBytes(uint64(deletedId)), data)
		if err != nil {
			return err
		}
		// Update offer id to deleted virtual ids mapping
		deletedKeys := &deletedOffers{}
		_, err = s.getJson(tx, kvDeletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		deletedKeys.Ids = append(deletedKeys.Ids, DeletedOffer{
			Id:   uint64(deletedId),
			Date: now.Format(time.RFC3339),
		})
		err = s.putJson(tx, kvDeletedKeysBucket, key, deletedKeys)
		if err != nil {
			return err
		}
		// Delete cached location
		err = tx.Delete(kvLocationsBucket, key)
		if err != nil {
			return err
		}
		// Delete the live offer
		return tx.Delete(kvOffersBucket, key)
	})
}

func (s *Store) ListDeletedIds() ([]string, error) {
	var err error
	ids := []string{}
	err = s.db.View(func(tx *Tx) error {
		ids, err = tx.List(kvDeletedKeysBucket)
		return err
	})
	return ids, err
}

func (s *Store) ListDeletedOffers(id string) ([]DeletedOffer, error) {
	deletedKeys := &deletedOffers{}
	err := s.db.View(func(tx *Tx) error {
		data, err := tx.Get(kvDeletedKeysBucket, []byte(id))
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
	err = s.db.View(func(tx *Tx) error {
		data, err = tx.Get(kvDeletedBucket, uintToBytes(id))
		return err
	})
	return data, err
}

func (s *Store) List() ([]string, error) {
	var err error
	var ids []string
	err = s.db.View(func(tx *Tx) error {
		ids, err = tx.List(kvOffersBucket)
		return err
	})
	return ids, err
}

func (s *Store) Size() int {
	n := 0
	s.db.View(func(tx *Tx) error {
		keys, err := tx.List(kvOffersBucket)
		if err == nil {
			n = len(keys)
		}
		return err
	})
	return n
}

func (s *Store) Version() (int, error) {
	return getKVDBVersion(s.db, kvOffersBucket)
}

func (s *Store) SetVersion(version int) error {
	return setKVDBVersion(s.db, kvOffersBucket, version)
}

func (s *Store) PutLocation(id string, loc *Location, date time.Time) error {
	return s.db.Update(func(tx *Tx) error {
		k := []byte(id)
		data, err := tx.Get(kvOffersBucket, k)
		if err != nil {
			return err
		}
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
		return tx.Put(kvLocationsBucket, k, w.Bytes())
	})
}

func (s *Store) GetLocation(id string) (*Location, time.Time, error) {
	var p *Location
	var date time.Time
	err := s.db.View(func(tx *Tx) error {
		data, err := tx.Get(kvLocationsBucket, []byte(id))
		if err != nil || len(data) == 0 {
			if data != nil {
				date = time.Unix(1, 0)
			}
			return err
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
	err := s.db.Update(func(tx *Tx) error {
		ids, err := tx.List(kvLocationsBucket)
		if err != nil {
			return err
		}
		for _, id := range ids {
			err = tx.Delete(kvLocationsBucket, []byte(id))
			if err != nil {
				return err
			}
		}
		return err
	})
	return err
}

func (s *Store) FixEmptyValues() (int, error) {
	return s.db.FixEmptyValues(kvLocationsBucket)
}
