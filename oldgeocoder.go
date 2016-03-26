package main

import (
	"bytes"
	"path/filepath"
)

type OldCache struct {
	db *KVDB
}

func OpenOldCache(dir string) (*OldCache, error) {
	path := filepath.Join(dir, "kv")
	exists, err := isFile(path)
	if err != nil {
		return nil, err
	}
	db, err := OpenKVDB(path, 0)
	if err != nil {
		return nil, err
	}
	c := &OldCache{
		db: db,
	}
	if !exists {
		err = c.SetVersion(geocoderVersion)
		if err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *OldCache) Close() error {
	return c.db.Close()
}

func (c *OldCache) Put(key string, data []byte, pos *Location) error {
	return c.db.Update(func(tx *Tx) error {
		k := []byte(key)
		err := tx.Put(geoCacheBucket, k, data)
		if err != nil {
			return err
		}
		w := bytes.NewBuffer(nil)
		if pos != nil {
			err = writeBinaryLocation(w, pos)
			if err != nil {
				return err
			}
		}
		return tx.Put(geoPointBucket, k, w.Bytes())
	})
}

func (c *OldCache) Get(key string) ([]byte, error) {
	var err error
	var data []byte
	err = c.db.View(func(tx *Tx) error {
		data, err = tx.Get(geoCacheBucket, []byte(key))
		return err
	})
	return data, err
}

func (c *OldCache) GetLocation(key string) (*Location, bool, error) {
	var p *Location
	found := false
	err := c.db.View(func(tx *Tx) error {
		data, err := tx.Get(geoPointBucket, []byte(key))
		found = data != nil
		if err != nil || len(data) == 0 {
			return err
		}
		point, err := readBinaryLocation(bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		p = point
		return nil
	})
	return p, found, err
}

func (c *OldCache) List() ([]string, error) {
	keys := []string{}
	var err error
	err = c.db.View(func(tx *Tx) error {
		keys, err = tx.List(geoCacheBucket)
		return err
	})
	return keys, err
}

func (c *OldCache) Version() (int, error) {
	return getKVDBVersion(c.db, geoCacheBucket)
}

func (c *OldCache) SetVersion(version int) error {
	return setKVDBVersion(c.db, geoCacheBucket, version)
}
