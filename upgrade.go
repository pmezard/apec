package main

import (
	"fmt"
	"log"
	"path/filepath"
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
	if err != nil || version >= geocoderVersion {
		return err
	}
	log.Printf("migrating geocoder from %d to %d", version, geocoderVersion)
	fixed, err := cache.FixEmptyValues()
	if err != nil {
		return err
	}
	err = cache.SetVersion(geocoderVersion)
	if err != nil {
		return err
	}
	log.Printf("%d values fixed\n", fixed)
	return cache.Close()
}

func populateStoreLocations(geocoderDir, storeDir string) error {
	store, err := UpgradeStore(storeDir)
	if err != nil {
		return err
	}
	defer store.Close()

	version, err := store.Version()
	if err != nil || version >= 2 {
		return err
	}
	log.Printf("migrating store from %d to %d", version, 2)
	if version < 2 {
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
	err = store.SetVersion(2)
	if err != nil {
		return err
	}
	return store.Close()
}

func fixStoreEmptyValues(storeDir string) error {
	store, err := UpgradeStore(storeDir)
	if err != nil {
		return err
	}
	defer store.Close()

	version, err := store.Version()
	if err != nil || version >= 3 {
		return err
	}
	log.Printf("migrating store from %d to %d", version, 3)
	fixed, err := store.FixEmptyValues()
	if err != nil {
		return err
	}
	err = store.SetVersion(3)
	if err != nil {
		return err
	}
	log.Printf("%d store empty values fixed\n", fixed)
	return store.Close()
}

func fixStoreSize(path string) error {
	db, err := OpenKVDB(path, 0)
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Update(func(tx *Tx) error {
		return tx.UpdateSize()
	})
	if err != nil {
		return err
	}
	return db.Close()
}

func fixStoreSizes(cfg *Config) error {
	paths := []string{
		cfg.Geocoder(),
		cfg.Store(),
	}
	for _, path := range paths {
		log.Printf("upgrade size of %s", path)
		err := fixStoreSize(filepath.Join(path, "kv"))
		if err != nil {
			return err
		}
	}
	return nil
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
	err = fixStoreEmptyValues(cfg.Store())
	if err != nil {
		return err
	}
	err = fixStoreSizes(cfg)
	if err != nil {
		return err
	}
	return err
}
