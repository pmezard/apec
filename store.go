package main

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pmezard/apec/jstruct"
)

type Store struct {
	db *bolt.DB
}

var (
	metaBucket         = []byte("meta")
	offersBucket       = []byte("offers")
	deletedBucket      = []byte("deleted")
	deletedKeysBucket  = []byte("deleted_keys")
	locationsBucket    = []byte("locations")
	offerDatesBucket   = []byte("dates")
	initialDatesBucket = []byte("initialdates")

	buckets = [][]byte{
		metaBucket,
		offersBucket,
		deletedBucket,
		deletedKeysBucket,
		locationsBucket,
		offerDatesBucket,
		initialDatesBucket,
	}

	storeVersion = 3
)

func isFile(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

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

func (s *Store) Delete(id string, now time.Time) (uint64, error) {
	removedId := uint64(0)
	err := s.db.Update(func(tx *bolt.Tx) error {
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
		removedId = deletedId
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
	return removedId, err
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

func hashOffer(js *jstruct.JsonOffer) string {
	data := []byte(js.Title + js.HTML + js.Location + js.Account + js.Salary)
	h := md5.Sum(data)
	return hex.EncodeToString(h[:])
}

type OfferAge struct {
	Id              string
	DeletedId       uint64
	PublicationDate time.Time
	DeletionDate    time.Time
	InitialDate     time.Time
}

type sortedByStartDate []OfferAge

func (s sortedByStartDate) Len() int {
	return len(s)
}

func (s sortedByStartDate) Less(i, j int) bool {
	return s[i].PublicationDate.Before(s[j].PublicationDate)
}

func (s sortedByStartDate) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func computeInitialDate(ages []OfferAge) []OfferAge {
	tolerance := 7 * 24 * time.Hour
	sort.Sort(sortedByStartDate(ages))
	minStart := time.Time{}
	updated := []OfferAge{}
	for i, age := range ages {
		if !age.DeletionDate.IsZero() && age.DeletionDate.Before(age.PublicationDate) {
			age.DeletionDate = age.PublicationDate
		}
		if minStart.IsZero() {
			minStart = age.PublicationDate
		} else if i > 0 {
			prev := updated[i-1]
			if !prev.DeletionDate.IsZero() &&
				prev.DeletionDate.Add(tolerance).Before(age.PublicationDate) {
				minStart = age.PublicationDate
			}
		}
		age.InitialDate = minStart
		updated = append(updated, age)
	}
	return updated
}

func (s *Store) getOfferDates(tx *bolt.Tx, hash string) ([]OfferAge, error) {
	data := tx.Bucket(offerDatesBucket).Get([]byte(hash))
	if data == nil {
		return nil, nil
	}
	ages := []OfferAge{}
	err := json.Unmarshal(data, &ages)
	return ages, err
}

func (s *Store) putOfferDates(tx *bolt.Tx, hash string, ages []OfferAge) error {
	data, err := json.Marshal(&ages)
	if err != nil {
		return err
	}
	return tx.Bucket(offerDatesBucket).Put([]byte(hash), data)
}

type InitialDate struct {
	Date time.Time `json:"date"`
	Hash string    `json:"hash"`
}

func (s *Store) putInitialDate(tx *bolt.Tx, offerId, hash string, date time.Time) error {
	data, err := json.Marshal(&InitialDate{
		Date: date,
		Hash: hash,
	})
	if err != nil {
		return err
	}
	return tx.Bucket(initialDatesBucket).Put([]byte(offerId), data)
}

func (s *Store) GetInitialDate(offerId string) (time.Time, error) {
	date := time.Time{}
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(initialDatesBucket).Get([]byte(offerId))
		if data == nil {
			return nil
		}
		d := &InitialDate{}
		err := json.Unmarshal(data, d)
		if err != nil {
			return fmt.Errorf("could not decode initial date: %s", err)
		}
		date = d.Date
		return nil
	})
	return date, err
}

func (s *Store) PutOfferDate(hash string, age OfferAge) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		ages, err := s.getOfferDates(tx, hash)
		if err != nil {
			return err
		}
		// Collect active offer initial dates before the update
		before := map[string]time.Time{}
		for _, a := range ages {
			if a.DeletedId != 0 {
				continue
			}
			before[a.Id] = a.InitialDate
		}
		kept := []OfferAge{}
		for _, a := range ages {
			if a.Id == age.Id && a.DeletedId == age.DeletedId {
				continue
			}
			kept = append(kept, a)
		}
		ages = append(kept, age)
		ages = computeInitialDate(ages)
		err = s.putOfferDates(tx, hash, ages)
		if err != nil {
			return err
		}
		// Update or delete offers initial dates
		for _, a := range ages {
			if a.DeletedId != 0 {
				continue
			}
			d := before[a.Id]
			if d.IsZero() || !d.Equal(a.InitialDate) {
				err = s.putInitialDate(tx, a.Id, hash, a.InitialDate)
				if err != nil {
					return err
				}
			}
			delete(before, a.Id)
		}
		for id := range before {
			err = tx.Bucket(initialDatesBucket).Delete([]byte(id))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) PutOfferDates(hash string, ages []OfferAge) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		ages = computeInitialDate(ages)
		err := s.putOfferDates(tx, hash, ages)
		if err != nil {
			return err
		}
		// Update or delete offers initial dates
		for _, a := range ages {
			if a.DeletedId != 0 {
				continue
			}
			err = s.putInitialDate(tx, a.Id, hash, a.InitialDate)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) RemoveInitialDates() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{initialDatesBucket, offerDatesBucket}
		for _, bucket := range buckets {
			b := tx.Bucket(bucket)
			if b != nil {
				err := tx.DeleteBucket(bucket)
				if err != nil {
					return err
				}
			}
			_, err := tx.CreateBucket(bucket)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
