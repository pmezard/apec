package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	err = next.Close()
	if err != nil {
		return err
	}
	err = os.RemoveAll(path)
	if err != nil {
		return err
	}
	return os.Rename(tmpDir, path)
}

func populateStoreLocations(geocoderDir, storeDir string) error {
	store, err := UpgradeStore(storeDir)
	if err != nil {
		return err
	}
	defer store.Close()

	version, err := store.Version()
	if err != nil || version >= storeVersion {
		return err
	}
	log.Printf("migrating store from %d to %d", version, storeVersion)
	if version < storeVersion {
		err = store.DeleteLocations()
		if err != nil {
			return err
		}
	}

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
		_, date, err := store.GetLocation(id)
		if !date.IsZero() {
			continue
		}
		offer, err := getStoreOffer(store, id)
		if err != nil {
			return err
		}
		loc, _, _, err := geocodeOffer(geocoder, offer.Location, true, 0)
		if err != nil {
			return err
		}
		err = store.PutLocation(id, loc, offer.Date)
		if err != nil {
			return err
		}
	}
	err = store.SetVersion(storeVersion)
	if err != nil {
		return err
	}
	return store.Close()
}

func upgrade(cfg *Config) error {
	err := upgradeGeocoderCache(cfg.Geocoder())
	if err != nil {
		return fmt.Errorf("could not upgrade geocoder: %s", err)
	}
	err = populateStoreLocations(cfg.Geocoder(), cfg.Store())
	if err != nil {
		return fmt.Errorf("could not upgrade store: %s", err)
	}
	return err
}
