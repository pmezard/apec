package main

import (
	"fmt"
	"time"

	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
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
		if offer == nil {
			fmt.Printf("skipping %s\n", id)
			data, err := store.Get(id)
			if err != nil {
				fmt.Printf("error %s: %s\n", id, err)
			} else if len(data) == 0 {
				fmt.Printf("error %s: nil\n", id)
			} else {
				fmt.Printf("error %s: %s\n", id, string(data))
			}
			continue
		}
		err = callback(offer, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

type sortedOfferAges [][]OfferAge

func (s sortedOfferAges) Len() int {
	return len(s)
}

func (s sortedOfferAges) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedOfferAges) Less(i, j int) bool {
	return len(s[i]) < len(s[j])
}

var (
	duplicatesCmd     = app.Command("duplicates", "compute statistics on duplicate offers")
	duplicatesReindex = duplicatesCmd.Flag("reindex", "reindex initial dates").Bool()
)

func duplicatesFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	dateLayout := "2006-01-02T15:04:05.000+0000"
	if *duplicatesReindex {
		fmt.Println("remove initial")
		err = store.RemoveInitialDates()
		if err != nil {
			return err
		}

		deletedLayout := "2006-01-02T15:04:05-07:00"

		fmt.Println("enumerating")
		collisions := map[string][]OfferAge{}
		indexed := 0
		err = enumerateStoredOffers(store, func(offer *jstruct.JsonOffer,
			do *DeletedOffer) error {
			indexed++
			if (indexed % 500) == 0 {
				fmt.Printf("%d dates listed\n", indexed)
			}

			date, err := time.Parse(dateLayout, offer.Date)
			if err != nil {
				return fmt.Errorf("cannot parse offer date: %s", err)
			}
			hash := hashOffer(offer)
			age := OfferAge{
				Id:              offer.Id,
				PublicationDate: date,
			}
			if do != nil {
				date, err := time.Parse(deletedLayout, do.Date)
				if err != nil {
					return fmt.Errorf("cannot parse deleted offer date: %s", err)
				}
				age.DeletedId = do.Id
				age.DeletionDate = date
			}
			collisions[hash] = append(collisions[hash], age)
			return nil
		})
		if err != nil {
			return err
		}

		prevBlock := indexed / 1000
		for hash, ages := range collisions {
			err = store.PutOfferDates(hash, ages)
			if err != nil {
				return err
			}
			indexed -= len(ages)
			if (indexed / 1000) < prevBlock {
				fmt.Printf("remaining %d\n", indexed)
				prevBlock = indexed / 1000
			}
		}
	}

	ids, err := store.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		o, err := getStoreJsonOffer(store, id)
		if err != nil {
			return err
		}
		d, err := store.GetInitialDate(id)
		if err != nil {
			return err
		}
		if d.IsZero() {
			date := "nil"
			if o != nil {
				date = o.Date
			}
			data, err := store.Get(id)
			if err != nil {
				return err
			}
			fmt.Printf("cannot get %s initial date, %s %d\n", id, date, len(data))
			continue
		}
		pub, err := time.Parse(dateLayout, o.Date)
		if err != nil {
			return err
		}
		delta := pub.Sub(d) / (24 * time.Hour)
		fmt.Printf("%s: pub=%s, init=%s, delta=%dj\n", id,
			pub.Format("2006-01-02"), d.Format("2006-01-02"), delta)
	}
	return nil
}
