package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
)

var (
	duplicatesCmd = app.Command("duplicates", "compute statistics on duplicate offers")
)

func enumerateStoredOffers(store *Store,
	callback func(offer *jstruct.JsonOffer, do *DeletedOffer) error) error {

	// Enumerate deleted offers
	ids, err := store.ListDeletedIds()
	if err != nil {
		return err
	}
	for _, id := range ids {
		deletedIds, err := store.ListDeletedOffers(id)
		if err != nil {
			return err
		}
		for _, deleted := range deletedIds {
			data, err := store.GetDeleted(deleted.Id)
			if err != nil {
				return err
			}
			js := &jstruct.JsonOffer{}
			err = ffjson.Unmarshal(data, js)
			if err != nil {
				return err
			}
			err = callback(js, &deleted)
			if err != nil {
				return err
			}
		}
	}
	// Enumerate valid offers
	ids, err = store.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		offer, err := getStoreJsonOffer(store, id)
		if err != nil {
			return err
		}
		err = callback(offer, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func hashOffer(js *jstruct.JsonOffer) string {
	data := []byte(js.Title + js.HTML + js.Location + js.Account + js.Salary)
	h := md5.Sum(data)
	return hex.EncodeToString(h[:])
}

type Collision struct {
	Id          string
	DeletedId   uint64
	Date        string
	DeletedDate string
}

type sortedCollisions [][]Collision

func (s sortedCollisions) Len() int {
	return len(s)
}

func (s sortedCollisions) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedCollisions) Less(i, j int) bool {
	return len(s[i]) < len(s[j])
}

func duplicatesFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	collisions := map[string][]Collision{}
	err = enumerateStoredOffers(store, func(offer *jstruct.JsonOffer,
		do *DeletedOffer) error {

		h := hashOffer(offer)
		c := Collision{
			Id:   offer.Id,
			Date: offer.Date,
		}
		if do != nil {
			c.DeletedId = do.Id
			c.DeletedDate = do.Date
		}
		collisions[h] = append(collisions[h], c)
		return nil
	})
	if err != nil {
		return err
	}
	groups := [][]Collision{}
	for _, v := range collisions {
		if len(v) < 2 {
			continue
		}
		groups = append(groups, v)
	}
	sort.Sort(sortedCollisions(groups))

	for _, v := range groups {
		for _, e := range v {
			if e.DeletedId == 0 {
				fmt.Printf("%s: %s\n", e.Id, e.Date)
			} else {
				fmt.Printf("%s: %s %s\n", e.Id, e.Date, e.DeletedDate)
			}
		}
		fmt.Println()
	}
	return nil
}
