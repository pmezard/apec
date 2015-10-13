package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func assertErr(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("%s", err)
	}
}

func openTempStore(t *testing.T) *Store {
	dir, err := ioutil.TempDir("", "apec-")
	if err != nil {
		t.Fatalf("could not create store temporary directory: %s", err)
	}
	path := filepath.Join(dir, "index")
	store, err := OpenStore(path)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("could not open store on %s: %s", path, err)
	}
	return store
}

func closeAndDeleteStore(t *testing.T, store *Store) {
	err := store.Close()
	os.RemoveAll(store.Path())
	if err != nil {
		t.Fatalf("could not close store on %s: %s", store.Path(), err)
	}
}

func TestOfferDeletion(t *testing.T) {
	store := openTempStore(t)
	defer closeAndDeleteStore(t, store)

	data := []byte("dummy")
	id := "id1"

	// Delete missing offer
	err := store.Delete(id)
	if err != nil {
		t.Fatalf("error while deleted missing entry: %s", err)
	}

	// Add something, deleted it and check the records
	err = store.Put(id, data)
	if err != nil {
		t.Fatalf("could not write entry: %s", err)
	}
	err = store.Delete(id)
	if err != nil {
		t.Fatalf("could not deleted created entry: %s", err)
	}
	data2, err := store.Get(id)
	if err != nil {
		t.Fatalf("error while getting missing entry: %s", err)
	}
	if data2 != nil {
		t.Fatalf("deleted data is still available: %v", data2)
	}
	deletedIds, err := store.ListDeletedIds()
	if err != nil {
		t.Fatalf("could not list deleted ids: %s", err)
	}
	if len(deletedIds) == 0 || deletedIds[0] != id {
		t.Fatalf("unexpected deleted ids: %v", deletedIds)
	}
	deletedOffers, err := store.ListDeletedOffers(id)
	if err != nil {
		t.Fatalf("could not list deleted offers for %s: %s", id, err)
	}
	if len(deletedOffers) == 0 {
		t.Fatalf("deleted data was not recorded")
	}
}
