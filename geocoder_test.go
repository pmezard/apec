package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
)

func addCacheEntry(t *testing.T, cache *Cache, key, path string) {
	path = filepath.Join("testdata", path)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("could not read test file %s: %s", path, err)
	}
	loc := &jstruct.Location{}
	err = ffjson.Unmarshal(data, loc)
	if err != nil {
		t.Fatalf("could not parse json data from %s: %s", path, err)
	}
	p := buildLocation(loc)
	err = cache.Put(key, data, p)
	if err != nil {
		t.Fatalf("could not cache from %s: %s", path, err)
	}
}

func checkCacheLocation(t *testing.T, cache *Cache, key string,
	there bool, expected *Location) {
	loc, ok, err := cache.GetLocation(key)
	if err != nil {
		t.Fatalf("cannot retrieve location for %s: %s", key, err)
	}
	if !there {
		if ok {
			t.Fatalf("%s is not expected in the cache", key)
		}
		return
	}
	if expected == nil {
		if loc != nil {
			t.Fatalf("cached locations points differ: %+v\n!=\n%+v", expected, loc)
		}
		return
	}
	if !reflect.DeepEqual(*expected, *loc) {
		t.Fatalf("cached locations differ: %+v\n!=\n%+v", *expected, *loc)
	}
}

func TestGeocoderCacheLocation(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "apec-")
	if err != nil {
		t.Fatalf("could not create geocoder cache directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "geocoder")
	cache, err := OpenCache(path)
	if err != nil {
		t.Fatalf("could not create cache: %s", err)
	}
	defer cache.Close()

	// Add entry with results
	addCacheEntry(t, cache, "results", "geo_results.json")
	addCacheEntry(t, cache, "noresult", "geo_noresult.json")

	// Check what was stored
	checkCacheLocation(t, cache, "results", true, &Location{
		City:    "Paris",
		County:  "Paris",
		State:   "Ile-de-France",
		Country: "France",
		Lat:     48.8565056,
		Lon:     2.3521334,
	})
	checkCacheLocation(t, cache, "noresult", true, nil)
	checkCacheLocation(t, cache, "missing", false, nil)
}

func TestGeocoderNew(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "apec-")
	if err != nil {
		t.Fatalf("could not create geocoder cache directory: %s", err)
	}
	defer os.RemoveAll(tmpDir)
	path := filepath.Join(tmpDir, "geocoder")

	g, err := NewGeocoder("some_key", path)
	if err != nil {
		t.Fatal(err)
	}
	g.Close()
}
