package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

var (
	dumpDeletedCmd = app.Command("dump-deleted", "dump deleted offer records")
)

func dumpDeletedOffersFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	ids, err := store.ListDeletedIds()
	if err != nil {
		return err
	}
	for _, id := range ids {
		offers, err := store.ListDeletedOffers(id)
		if err != nil {
			return err
		}
		dates := []string{}
		for _, o := range offers {
			dates = append(dates, o.Date)
		}
		fmt.Printf("%s: %s\n", id, strings.Join(dates, ", "))
	}
	return nil
}

var (
	changesCmd = app.Command("changes", "print offers changes per day")
)

func printChanges(w io.Writer, store *Store, reverse bool) error {
	changes := map[string]struct {
		Added   int
		Removed int
	}{}

	// Collect publication dates (not really additions but...)
	ids, err := store.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		o, err := getStoreOffer(store, id)
		if err != nil {
			return err
		}
		k := o.Date.Format("2006-01-02")
		ch := changes[k]
		ch.Added += 1
		changes[k] = ch
	}

	// Collect deletions
	ids, err = store.ListDeletedIds()
	if err != nil {
		return err
	}
	for _, id := range ids {
		offers, err := store.ListDeletedOffers(id)
		if err != nil {
			return err
		}
		for _, o := range offers {
			d, err := time.Parse(time.RFC3339, o.Date)
			if err != nil {
				return err
			}
			k := d.Format("2006-01-02")
			ch := changes[k]
			ch.Removed += 1
			changes[k] = ch
		}
	}

	// Print everything in ascending date order
	dates := []string{}
	for k := range changes {
		dates = append(dates, k)
	}
	sort.Strings(dates)
	if reverse {
		for i := 0; i < len(dates)/2; i++ {
			j := len(dates) - i - 1
			dates[i], dates[j] = dates[j], dates[i]
		}
	}

	for _, d := range dates {
		ch := changes[d]
		fmt.Fprintf(w, "%s: +%d, -%d offers\n", d, ch.Added, ch.Removed)
	}
	return nil
}

func changesFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	return printChanges(os.Stdout, store, false)
}
