package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createTempKVDB(t *testing.T, maxSize int) *KVDB {
	tmpDir, err := ioutil.TempDir("", "apec-")
	if err != nil {
		t.Fatalf("cannot create temporary directory for test KVDB: %s", err)
	}
	path := filepath.Join(tmpDir, "db")
	db, err := OpenKVDB(path, maxSize)
	if err != nil {
		t.Fatalf("cannot open KVDB: %s", err)
	}
	return db
}

func closeAndDeleteKVDB(t *testing.T, db *KVDB) {
	err1 := db.Close()
	err2 := os.RemoveAll(db.Path())
	if err1 != nil {
		t.Fatalf("cannot close KVDB: %s", err1)
	}
	if err2 != nil {
		t.Fatalf("cannot remove KVDB directory: %s", err2)
	}
}

type KVContent struct {
	Key   []byte
	Value []byte
}

type PrefixContent struct {
	Name []byte
	KV   []KVContent
}

type DBContent struct {
	Prefixes []PrefixContent
}

func checkView(t *testing.T, db *KVDB, action func(tx *Tx) error) {
	err := db.View(action)
	if err != nil {
		t.Fatal(err)
	}
}

func checkUpdate(t *testing.T, db *KVDB, action func(tx *Tx) error) {
	err := db.Update(action)
	if err != nil {
		t.Fatal(err)
	}
}

func checkKVDBPut(t *testing.T, db *KVDB, parts ...[]byte) {
	checkUpdate(t, db, func(tx *Tx) error {
		if len(parts)%3 != 0 {
			return fmt.Errorf("parts must be a sequence of prefix, key, value")
		}
		for i := 0; i < len(parts)/3; i++ {
			prefix := parts[3*i]
			key := parts[3*i+1]
			value := parts[3*i+2]
			err := tx.Put(prefix, key, value)
			if err != nil {
				return fmt.Errorf("failed to put %s:%s:%s: %s",
					string(prefix), string(key), string(value), err)
			}
		}
		return nil
	})
}

func checkKVDBDel(t *testing.T, db *KVDB, parts ...[]byte) {
	checkUpdate(t, db, func(tx *Tx) error {
		if len(parts)%2 != 0 {
			return fmt.Errorf("parts must be a sequence of prefix, key")
		}
		for i := 0; i < len(parts)/2; i++ {
			prefix := parts[2*i]
			key := parts[2*i+1]
			err := tx.Delete(prefix, key)
			if err != nil {
				return fmt.Errorf("failed to delete %s:%s: %s",
					string(prefix), string(key), err)
			}
		}
		return nil
	})
}

func checkKVDBContent(t *testing.T, db *KVDB, content DBContent) {
	checkView(t, db, func(tx *Tx) error {
		for _, prefix := range content.Prefixes {
			name := string(prefix.Name)
			keys, err := tx.List(prefix.Name)
			if err != nil {
				return fmt.Errorf("could not list keys for %s: %s", name, err)
			}
			got := strings.Join(keys, ",")
			parts := []string{}
			for _, k := range prefix.KV {
				parts = append(parts, string(k.Key))
			}
			wanted := strings.Join(parts, ",")
			if got != wanted {
				return fmt.Errorf("%s keys mismatch: %q != %q", name, got, wanted)
			}
			for _, k := range prefix.KV {
				v, err := tx.Get(prefix.Name, k.Key)
				if err != nil {
					return fmt.Errorf("failed to retrieve %s:%s: %s", name,
						string(k.Key), err)
				}
				if !bytes.Equal(v, k.Value) {
					return fmt.Errorf("content mismatch for %s:%s: %q != %q",
						name, string(k.Key), string(v), string(k.Value))
				}
			}
		}
		return nil
	})
}

func testCRUD(t *testing.T, db *KVDB) {
	p1 := []byte("prefix1")
	p2 := []byte("prefix2")
	k1 := []byte("some key 1")
	v1 := []byte("some value 1")
	k2 := []byte("some key 2")
	v2 := []byte("some value 2")
	v3 := []byte("some value 3")

	// Empty db is empty
	checkView(t, db, func(tx *Tx) error {
		keys, err := tx.List(p1)
		if err != nil {
			t.Fatalf("cannot list empty prefix: %s", err)
		}
		if len(keys) != 0 {
			t.Fatalf("empty db has keys: %+v", keys)
		}
		return nil
	})

	// Retrieving missing value returns nil
	checkView(t, db, func(tx *Tx) error {
		v, err := tx.Get(p1, k1)
		if err != nil {
			t.Fatalf("retrieving missing value fails: %s", err)
		}
		if v != nil {
			t.Fatalf("missing value must be nil not %+v", v)
		}
		return nil
	})

	// Deleting missing value is fine
	checkUpdate(t, db, func(tx *Tx) error {
		err := tx.Delete(p1, k1)
		if err != nil {
			t.Fatalf("deleting missing value failed with: %s", err)
		}
		return nil
	})

	// Add 3 values in 2 different tables
	checkKVDBPut(t, db,
		p1, k1, v1,
		p1, k2, v2,
		p2, k1, v3)
	checkKVDBContent(t, db, DBContent{
		Prefixes: []PrefixContent{
			{
				Name: p1,
				KV: []KVContent{
					{Key: k1, Value: v1},
					{Key: k2, Value: v2},
				},
			},
			{
				Name: p2,
				KV: []KVContent{
					{Key: k1, Value: v3},
				},
			},
		},
	})

	// Remove one which key exists in two prefixes
	checkKVDBDel(t, db, p1, k1)
	checkKVDBContent(t, db, DBContent{
		Prefixes: []PrefixContent{
			{
				Name: p1,
				KV: []KVContent{
					{Key: k2, Value: v2},
				},
			},
			{
				Name: p2,
				KV: []KVContent{
					{Key: k1, Value: v3},
				},
			},
		},
	})

	// Overwrite values
	checkKVDBPut(t, db,
		p1, k1, v3,
		p2, k1, v1,
		p2, k2, v1)
	checkKVDBContent(t, db, DBContent{
		Prefixes: []PrefixContent{
			{
				Name: p1,
				KV: []KVContent{
					{Key: k1, Value: v3},
					{Key: k2, Value: v2},
				},
			},
			{
				Name: p2,
				KV: []KVContent{
					{Key: k1, Value: v1},
					{Key: k2, Value: v1},
				},
			},
		},
	})
}

func TestKVDBLarge(t *testing.T) {
	db := createTempKVDB(t, 1024)
	defer closeAndDeleteKVDB(t, db)
	testCRUD(t, db)
}

func TestKVDBSmall(t *testing.T) {
	// maxSize is smaller than inserted values
	db := createTempKVDB(t, 3)
	defer closeAndDeleteKVDB(t, db)
	testCRUD(t, db)
}

func TestKVDBInc(t *testing.T) {
	db := createTempKVDB(t, 100)
	defer closeAndDeleteKVDB(t, db)

	// Check it actually increments
	p := []byte("prefix")
	checkUpdate(t, db, func(tx *Tx) error {
		n, err := tx.Inc(p, 2)
		if err != nil {
			t.Fatalf("could not increment from nothing: %s", err)
		}
		if n != 2 {
			t.Fatalf("unexpected increment: %d != 2", n)
		}
		return nil
	})

	// Again
	checkUpdate(t, db, func(tx *Tx) error {
		n, err := tx.Inc(p, 1)
		if err != nil {
			t.Fatalf("could not increment from 2: %s", err)
		}
		if n != 3 {
			t.Fatalf("increment returned %d, expected 3", n)
		}
		return nil
	})

	// Check it does not mess with key enumerations
	checkView(t, db, func(tx *Tx) error {
		keys, err := tx.List(p)
		if err != nil {
			t.Fatalf("could not list keys: %s", err)
		}
		if len(keys) != 0 {
			t.Fatalf("increments have some effect on regular keys: %+v", keys)
		}
		return nil
	})
}
