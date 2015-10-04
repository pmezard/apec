package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pmezard/apec/attic"
)

var (
	upgradeCmd = app.Command("upgrade", "upgrade dataset schema")
)

func upgradeFileStoreToBoltDB(storeDir string) error {
	tempFile, err := ioutil.TempFile(filepath.Dir(storeDir), "bolt-")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	err = os.Remove(tempPath)
	if err != nil {
		return err
	}
	fs, err := attic.OpenStore(storeDir)
	if err != nil {
		return fmt.Errorf("could not open file store: %s", err)
	}
	db, err := OpenStore(tempPath)
	if err != nil {
		return fmt.Errorf("could not open bolt store: %s", err)
	}
	defer db.Close()
	offers := fs.List()
	for i, id := range offers {
		if (i+1)%500 == 0 {
			fmt.Printf("%d/%d offers migrated\n", i+1, len(offers))
		}
		data, err := fs.Get(id)
		if err != nil {
			return fmt.Errorf("could not fetch offer %s: %s", id, err)
		}
		err = db.Put(id, data)
		if err != nil {
			return err
		}
	}
	fmt.Printf("%d offers migrated\n", len(offers))
	err = db.Close()
	if err != nil {
		return err
	}
	err = os.Rename(storeDir, storeDir+"-old")
	if err != nil {
		return err
	}
	return os.Rename(tempPath, storeDir)
}

func upgrade(cfg *Config) error {
	storeDir := cfg.Store()
	st, err := os.Stat(storeDir)
	if err != nil {
		return err
	}
	if st.IsDir() {
		fmt.Printf("upgrading filestore to boltdb\n")
		err = upgradeFileStoreToBoltDB(storeDir)
		if err != nil {
			return err
		}
	} else {
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
	}
	return nil
}
