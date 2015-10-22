package main

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"
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
	store, err := OpenStore(dir)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("could not open store on %s: %s", dir, err)
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

	now := time.Now()
	data := []byte("dummy")
	id := "id1"

	// Delete missing offer
	err := store.Delete(id, now)
	if err != nil {
		t.Fatalf("error while deleted missing entry: %s", err)
	}

	// Add something, deleted it and check the records
	err = store.Put(id, data)
	if err != nil {
		t.Fatalf("could not write entry: %s", err)
	}
	err = store.Delete(id, now)
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
	deletedData, err := store.GetDeleted(deletedOffers[0].Id)
	if err != nil {
		t.Fatalf("could not get deleted data: %s", err)
	}
	if bytes.Compare(deletedData, data) != 0 {
		t.Fatalf("deleted data does not match data: %x != %x", deletedData, data)
	}
}

func TestOfferSize(t *testing.T) {
	store := openTempStore(t)
	defer closeAndDeleteStore(t, store)

	now := time.Now()
	data := []byte("dummy")
	id := "id1"

	size := store.Size()
	if size != 0 {
		t.Fatalf("empty store has %d items", size)
	}

	err := store.Put(id, data)
	if err != nil {
		t.Fatalf("could not write entry: %s", err)
	}
	size = store.Size()
	if size != 1 {
		t.Fatalf("store should have 1 element, got %d", size)
	}

	err = store.Delete(id, now)
	if err != nil {
		t.Fatalf("could not delete %s: %s", id, err)
	}
	size = store.Size()
	if size != 0 {
		t.Fatalf("empty store has %d items", size)
	}
}

func TestOfferLocation(t *testing.T) {
	store := openTempStore(t)
	defer closeAndDeleteStore(t, store)

	id := "1"
	now := time.Now()
	data := []byte("dummy")
	loc := &Location{
		City: "Paris",
	}

	err := store.PutLocation(id, loc, now)
	if err == nil {
		t.Fatalf("adding location to missing offers should have failed")
	}

	// Test non-nil location
	err = store.Put(id, data)
	if err != nil {
		t.Fatal(err)
	}
	err = store.PutLocation(id, loc, now)
	if err != nil {
		t.Fatal(err)
	}
	loc2, date, err := store.GetLocation(id)
	if err != nil {
		t.Fatal(err)
	}
	if loc2 == nil || date.IsZero() {
		t.Fatalf("unexpected nil location")
	}

	// Resetting the data should invalidate the location
	err = store.Put(id, data)
	if err != nil {
		t.Fatal(err)
	}
	loc2, date, err = store.GetLocation(id)
	if err != nil {
		t.Fatal(err)
	}
	if loc2 != nil || !date.IsZero() {
		t.Fatal("location should have been reset by Put()")
	}

	// Test empty location
	err = store.PutLocation(id, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	loc2, date, err = store.GetLocation(id)
	if err != nil {
		t.Fatal(err)
	}
	if loc2 != nil || date.IsZero() {
		t.Fatal("could not retrieve empty location")
	}

	// Deleting an offer remove its location
	err = store.Delete(id, now)
	if err != nil {
		t.Fatal(err)
	}
	loc2, date, err = store.GetLocation(id)
	if err != nil {
		t.Fatal(err)
	}
	if loc2 != nil || !date.IsZero() {
		t.Fatal("location should have been removed by Delete()")
	}
}
