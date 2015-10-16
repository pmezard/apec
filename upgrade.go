package main

import "fmt"

var (
	upgradeCmd = app.Command("upgrade", "upgrade dataset schema")
)

func upgradeGeocoder(dir string) error {
	seen := false
	log := func(s string) {
		if !seen {
			fmt.Printf("upgrading geocoder store\n")
			seen = true
		}
		fmt.Print("  " + s)
	}
	_, err := NewCache(dir, log)
	if err != nil {
		return err
	}
	return nil
}

func upgrade(cfg *Config) error {
	storeDir := cfg.Store()
	fmt.Printf("upgrading boltdb schema\n")
	// Assume boltdb, open it just to force an upgrade
	db, err := OpenStore(storeDir)
	if err != nil {
		return err
	}
	err = db.Close()
	if err != nil {
		return err
	}
	return upgradeGeocoder(cfg.Geocoder())
}
