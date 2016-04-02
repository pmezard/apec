package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

func openTempStore(t *testing.T) *Store {
	dir, err := ioutil.TempDir("", "apec-")
	if err != nil {
		t.Fatalf("could not create store temporary directory: %s", err)
	}
	path := filepath.Join(dir, "store")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("could not open store on %s: %s", path, err)
	}
	return store
}

func closeAndDeleteStore(t *testing.T, store *Store) {
	err := store.Close()
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

	// List missing deleted offers
	deletedOffers, err = store.ListDeletedOffers("missing")
	if err != nil {
		t.Fatal(err)
	}
	if len(deletedOffers) > 0 {
		t.Fatalf("missing deleted offers returned non-empty list")
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

func TestOfferAge(t *testing.T) {
	store := openTempStore(t)
	defer closeAndDeleteStore(t, store)

	baseDate, err := time.Parse("2006-01-02T15:04:05", "2016-01-01T13:00:00")
	if err != nil {
		t.Fatalf("could not parse base date: %s", err)
	}
	day := time.Hour * 24

	// Missing date
	d, err := store.GetInitialDate("o1")
	if err != nil {
		t.Fatalf("error fetching missing initial date: %s", err)
	}
	if !d.IsZero() {
		t.Fatalf("missing date must be zero: %v", d)
	}

	putDate := func(hash string, date OfferAge, expected time.Time) error {
		err = store.PutOfferDate(hash, date)
		if err != nil {
			return err
		}
		d, err = store.GetInitialDate(date.Id)
		if err != nil {
			return err
		}
		if d != expected {
			return fmt.Errorf("got %v, expected %v", d, expected)
		}
		return nil
	}

	// Add one live date
	a1_1 := OfferAge{
		Id:              "o1",
		PublicationDate: baseDate,
	}
	err = putDate("h1", a1_1, baseDate)
	if err != nil {
		t.Fatal(err)
	}

	// Update it in the future
	a1_1 = OfferAge{
		Id:              "o1",
		PublicationDate: baseDate.Add(30 * day),
	}
	err = putDate("h1", a1_1, baseDate.Add(30*day))
	if err != nil {
		t.Fatal(err)
	}

	// Move live date in the past
	a1_1 = OfferAge{
		Id:              "o1",
		PublicationDate: baseDate.Add(29 * day),
	}
	err = putDate("h1", a1_1, baseDate.Add(29*day))
	if err != nil {
		t.Fatal(err)
	}

	// Add deleted date in tolerance range
	a1_2 := OfferAge{
		Id:              "o1",
		DeletedId:       1,
		PublicationDate: baseDate.Add(24 * day),
		DeletionDate:    baseDate.Add(25 * day),
	}
	err = putDate("h1", a1_2, baseDate.Add(24*day))
	if err != nil {
		t.Fatal(err)
	}

	// Add it again with a different date, check deduplication
	a1_2 = OfferAge{
		Id:              "o1",
		DeletedId:       1,
		PublicationDate: baseDate.Add(25 * day),
		DeletionDate:    baseDate.Add(26 * day),
	}
	err = putDate("h1", a1_2, baseDate.Add(25*day))
	if err != nil {
		t.Fatal(err)
	}

	// Add a deleted value out of range
	a1_3 := OfferAge{
		Id:              "o1",
		DeletedId:       3,
		PublicationDate: baseDate.Add(3 * day),
		DeletionDate:    baseDate.Add(15 * day),
	}
	err = putDate("h1", a1_3, baseDate.Add(25*day))
	if err != nil {
		t.Fatal(err)
	}

	// Insert another one to link both ranges
	a1_4 := OfferAge{
		Id:              "o1",
		DeletedId:       4,
		PublicationDate: baseDate.Add(16 * day),
		DeletionDate:    baseDate.Add(21 * day),
	}
	err = putDate("h1", a1_4, baseDate.Add(3*day))
	if err != nil {
		t.Fatal(err)
	}

	// Add live offer in the future, past tolerance
	a1_5 := OfferAge{
		Id:              "o1",
		PublicationDate: baseDate.Add(60 * day),
	}
	err = putDate("h1", a1_5, baseDate.Add(60*day))
	if err != nil {
		t.Fatal(err)
	}

	// Another deleted offer does not link with the previous ones
	a1_6 := OfferAge{
		Id:              "o1",
		DeletedId:       6,
		PublicationDate: baseDate.Add(58 * day),
		DeletionDate:    baseDate.Add(59 * day),
	}
	err = putDate("h1", a1_6, baseDate.Add(58*day))
	if err != nil {
		t.Fatal(err)
	}
}
