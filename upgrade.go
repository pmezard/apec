package main

import (
	"fmt"
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
	log.Printf("migrating geocoder from %d", version)

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

func populateStoreLocations(geocoderDir, storeDir string) error {
	store, err := OpenStore(storeDir)
	if err != nil {
		return err
	}
	defer store.Close()

	version, err := store.Version()
	if err != nil || version >= 1 {
		return err
	}
	log.Printf("migrating store from %d to 1", version)

	geocoder, err := NewGeocoder("", geocoderDir)
	if err != nil {
		return err
	}
	defer geocoder.Close()

	ids, err := store.List()
	if err != nil {
		return err
	}
	for i, id := range ids {
		if (i+1)%1000 == 0 {
			fmt.Printf("%d offers location cached\n", i+1)
		}
		_, ok, err := store.GetLocation(id)
		if ok {
			continue
		}
		offer, err := getStoreJsonOffer(store, id)
		if err != nil {
			return err
		}
		loc, _, _, err := geocodeOffer(geocoder, offer.Location, true, 0)
		if err != nil {
			return err
		}
		err = store.PutLocation(id, loc)
		if err != nil {
			return err
		}
	}
	err = store.SetVersion(1)
	if err != nil {
		return err
	}
	return store.Close()
}

func upgrade(cfg *Config) error {
	err := upgradeGeocoderCache(cfg.Geocoder())
	if err != nil {
		return err
	}
	return populateStoreLocations(cfg.Geocoder(), cfg.Store())
}
