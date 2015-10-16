package attic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/boltdb/bolt"
)

var (
	QuotaError = errors.New("payment required")

	geoCacheBucket = []byte("cache")
	geoMetaBucket  = []byte("meta")
)

type BoltCache struct {
	db *bolt.DB
}

func NewBoltCache(dir string, logFn func(s string)) (*BoltCache, error) {
	db, err := bolt.Open(dir, 0666, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(geoCacheBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	c := &BoltCache{
		db: db,
	}
	return c, c.upgrade(logFn)
}

func (c *BoltCache) Close() error {
	return c.db.Close()
}

func (c *BoltCache) putJson(tx *bolt.Tx, bucketName []byte, key string,
	value interface{}) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return fmt.Errorf("no bucket %s", string(bucketName))
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(key), data)
}

func (c *BoltCache) getJson(tx *bolt.Tx, bucketName []byte, key string,
	value interface{}) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return fmt.Errorf("no bucket %s", string(bucketName))
	}
	data := bucket.Get([]byte(key))
	return json.Unmarshal(data, value)
}

func (c *BoltCache) Put(key string, data []byte) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(geoCacheBucket)
		return bucket.Put([]byte(key), data)
	})
}

func (c *BoltCache) Get(key string) ([]byte, error) {
	var data []byte
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(geoCacheBucket)
		temp := bucket.Get([]byte(key))
		data = make([]byte, len(temp))
		copy(data, temp)
		return nil
	})
	return data, err
}

func (c *BoltCache) ForEach(callback func(k, v []byte) error) error {
	return c.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(geoCacheBucket).ForEach(callback)
	})
}

type geoVersion struct {
	Version int `json:"version"`
}

func lowerCaseKeys(tx *bolt.Tx) error {
	keys := []string{}
	b := tx.Bucket(geoCacheBucket)
	if b == nil {
		return fmt.Errorf("cache bucket does not exist")
	}
	err := b.ForEach(func(k, v []byte) error {
		key := string(k)
		if strings.ToLower(key) != key {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	for _, key := range keys {
		lkey := strings.ToLower(key)
		k := []byte(key)
		data := b.Get(k)
		err = b.Put([]byte(lkey), data)
		if err != nil {
			return err
		}
		err = b.Delete(k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *BoltCache) upgrade(logFn func(s string)) error {
	log := func(format string, args ...interface{}) {
		if logFn != nil {
			logFn(fmt.Sprintf(format, args...))
		}
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		meta := geoVersion{}
		bucket := tx.Bucket(geoMetaBucket)
		if bucket != nil {
			err := c.getJson(tx, geoMetaBucket, "version", &meta)
			if err != nil {
				return err
			}
		} else {
			log("creating meta bucket\n")
			_, err := tx.CreateBucket(geoMetaBucket)
			if err != nil {
				return err
			}
		}
		if meta.Version == 0 {
			log("converting keys to lower case\n")
			err := lowerCaseKeys(tx)
			if err != nil {
				return err
			}
			meta.Version = 1
		}
		return c.putJson(tx, geoMetaBucket, "version", &meta)
	})
}
