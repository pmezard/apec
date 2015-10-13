package main

import (
	"fmt"
	"strings"
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
