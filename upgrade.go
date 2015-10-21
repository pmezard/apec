package main

import (
	"io/ioutil"
	"log"
	"path/filepath"

	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
)

var (
	upgradeCmd = app.Command("upgrade", "upgrade dataset schema")
)

func upgradeGeocoderCache(path string) error {
	cache, err := OpenCache(path)
	if err != nil {
		return err
	}
	defer cache.Close()
	version, err := cache.Version()
	if err != nil || version >= 1 {
		return err
	}
	log.Printf("migrating store from %d", version)

	tmpDir, err := ioutil.TempDir(filepath.Dir(path), "geocoder-")
	if err != nil {
		return err
	}
	next, err := OpenCache(tmpDir)
	if err != nil {
		return err
	}
	defer next.Close()

	// Migrate all entries, with cached lat/lon
	keys, err := cache.List()
	if err != nil {
		return err
	}
	points := 0
	for i, key := range keys {
		if (i+1)%1000 == 0 {
			log.Printf("%d/%d locations migrated\n", i+1, len(keys))
		}
		data, err := cache.Get(key)
		if err != nil {
			return err
		}
		loc := &jstruct.Location{}
		err = ffjson.Unmarshal(data, loc)
		if err != nil {
			return err
		}
		p := buildLocation(loc)
		if p != nil {
			points++
		}
		err = next.Put(key, data, p)
		if err != nil {
			return err
		}
	}
	err = next.SetVersion(1)
	if err != nil {
		return err
	}
	log.Printf("%d points migrated", points)
	return next.Close()
}

func upgrade(cfg *Config) error {
	return upgradeGeocoderCache(cfg.Geocoder())
}
