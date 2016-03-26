package main

import (
	"fmt"
	"log"
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
	err = cache.SetVersion(geocoderVersion)
	if err != nil {
		return err
	}
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

/*
func migrateGeocoder(oldDir, newPath string) error {
	oldCache, err := OpenOldCache(oldDir)
	if err != nil {
		return err
	}
	defer oldCache.Close()
	newCache, err := OpenCache(newPath)
	if err != nil {
		return nil
	}
	defer newCache.Close()

	keys, err := oldCache.List()
	if err != nil {
		return err
	}
	for _, key := range keys {
		data, err := oldCache.Get(key)
		if err != nil {
			return err
		}
		res := &jstruct.Location{}
		err = ffjson.Unmarshal(data, res)
		if err != nil {
			return err
		}
		err = newCache.Put(key, data, buildLocation(res))
		if err != nil {
			return err
		}
	}
	return newCache.Close()
}
*/

/*
func removeMissing(oldDir, newPath string) error {
	oldStore, err := OpenOldStore(oldDir)
	if err != nil {
		return err
	}
	defer oldStore.Close()
	newStore, err := OpenStore(newPath)
	if err != nil {
		return err
	}
	defer newStore.Close()

	newIds, err := newStore.List()
	if err != nil {
		return err
	}
	oldIds, err := oldStore.List()
	if err != nil {
		return err
	}
	oldMap := map[string]bool{}
	for _, id := range oldIds {
		oldMap[id] = true
	}
	oldIds = nil

	n := 0
	now := time.Now()
	for _, id := range newIds {
		n += 1
		if (n % 1000) == 0 {
			fmt.Printf("%d new offers deleted\n", n)
		}
		if _, ok := oldMap[id]; ok {
			continue
		}
		err := newStore.Delete(id, now)
		if err != nil {
			return err
		}
	}
	return newStore.Close()
}

func migrateStore(oldDir, newPath string) error {
	oldStore, err := OpenOldStore(oldDir)
	if err != nil {
		return err
	}
	defer oldStore.Close()
	newStore, err := OpenStore(newPath)
	if err != nil {
		return err
	}
	defer newStore.Close()

	deletedIds, err := oldStore.ListDeletedIds()
	if err != nil {
		return err
	}
	n := 0
	for _, deletedId := range deletedIds {
		n += 1
		if (n % 1000) == 0 {
			fmt.Printf("%d deleted offers migrated\n", n)
		}
		deletedOffers, err := oldStore.ListDeletedOffers(deletedId)
		if err != nil {
			return err
		}
		for _, deleted := range deletedOffers {
			offer, err := oldStore.GetDeleted(deleted.Id)
			if err != nil {
				return err
			}
			err = newStore.Put(deletedId, offer)
			if err != nil {
				return err
			}
			date, err := time.Parse(time.RFC3339, deleted.Date)
			err = newStore.Delete(deletedId, date)
			if err != nil {
				return err
			}
		}
	}
	ids, err := oldStore.List()
	if err != nil {
		return err
	}
	n = 0
	for _, id := range ids {
		n += 1
		if (n % 1000) == 0 {
			fmt.Printf("%d offers migrated\n", n)
		}
		offer, err := oldStore.Get(id)
		if err != nil {
			return err
		}
		err = newStore.Put(id, offer)
		if err != nil {
			return err
		}
	}
	return newStore.Close()
}
*/

func upgrade(cfg *Config) error {
	/*
				err := upgradeGeocoderCache(cfg.Geocoder())
				if err != nil {
					return fmt.Errorf("could not upgrade geocoder: %s", err)
				}
			err := migrateGeocoder(cfg.Geocoder(), "newgeocoder")
				err = populateStoreLocations(cfg.Geocoder(), cfg.Store())
				if err != nil {
					return fmt.Errorf("could not upgrade store: %s", err)
				}
					err := migrateStore("offers-orig-old/offers", "newstore")
					if err != nil {
						return err
					}
					err = removeMissing("offers/offers", "newstore")
					if err != nil {
						return err
					}
					err = migrateStore("offers/offers", "newstore")
		return err
	*/
	return nil
}
