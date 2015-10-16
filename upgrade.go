package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pmezard/apec/attic"
)

var (
	upgradeCmd = app.Command("upgrade", "upgrade dataset schema")
)

func upgradeBoltToKV(path string) error {
	boltStore, err := attic.OpenStore(path)
	if err != nil {
		return err
	}
	defer boltStore.Close()

	tmpDir, err := ioutil.TempDir(filepath.Dir(path), "kv-")
	if err != nil {
		return err
	}
	kvStore, err := OpenStore(tmpDir)
	if err != nil {
		return err
	}
	defer kvStore.Close()

	// Migrate deleted entries first
	deletedIds, err := boltStore.ListDeletedIds()
	if err != nil {
		return err
	}
	for i, id := range deletedIds {
		keys, err := boltStore.ListDeletedOffers(id)
		if err != nil {
			return err
		}
		if (i+1)%500 == 0 {
			fmt.Println(i+1, "deleted offers migrated")
		}
		for _, key := range keys {
			data, err := boltStore.GetDeleted(key.Id)
			if err != nil {
				return err
			}
			err = kvStore.Put(id, data)
			if err != nil {
				return err
			}
			date, err := time.Parse(time.RFC3339, key.Date)
			if err != nil {
				return err
			}
			err = kvStore.Delete(id, date)
			if err != nil {
				return err
			}
		}
	}

	// Migrate live entries
	ids, err := boltStore.List()
	if err != nil {
		return err
	}
	for i, id := range ids {
		if (i+1)%500 == 0 {
			fmt.Println(i+1, "offers migrated")
		}
		data, err := boltStore.Get(id)
		if err != nil {
			return err
		}
		err = kvStore.Put(id, data)
		if err != nil {
			return err
		}
	}
	err = kvStore.Close()
	if err != nil {
		return err
	}
	err = boltStore.Close()
	if err != nil {
		return err
	}
	return nil
}

func upgradeGeocoderBoltToKv(path string) error {
	boltCache, err := attic.NewBoltCache(path, nil)
	if err != nil {
		return err
	}
	defer boltCache.Close()

	tmpDir, err := ioutil.TempDir(filepath.Dir(path), "geo-")
	if err != nil {
		return err
	}
	kvCache, err := OpenCache(tmpDir)
	if err != nil {
		return err
	}
	defer kvCache.Close()

	i := 0
	err = boltCache.ForEach(func(k, v []byte) error {
		i++
		if (i % 500) == 0 {
			fmt.Printf("%d geocoder entries migrated\n", i)
		}
		return kvCache.Put(string(k), v)
	})
	if err != nil {
		return err
	}
	err = kvCache.Close()
	if err != nil {
		return err
	}
	return boltCache.Close()
}

func upgrade(cfg *Config) error {
	storeDir := cfg.Store()
	st, err := os.Stat(storeDir)
	if err != nil {
		return err
	}
	if !st.IsDir() {
		err = upgradeBoltToKV(storeDir)
		if err != nil {
			return err
		}
	}
	st, err = os.Stat(cfg.Geocoder())
	if err != nil {
		return err
	}
	if !st.IsDir() {
		fmt.Println("migrating geocoder to kv")
		err := upgradeGeocoderBoltToKv(cfg.Geocoder())
		if err != nil {
			return err
		}
	}
	return nil
}
