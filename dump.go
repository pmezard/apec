package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/tokenizers/exception"
	"github.com/blevesearch/bleve/analysis/tokenizers/unicode"
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

var (
	changesCmd = app.Command("changes", "print offers changes per day")
)

func changesFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	return printChanges(os.Stdout, store, false)
}

var (
	debugQueryCmd   = app.Command("debugquery", "debug bleve queries")
	debugQueryQuery = debugQueryCmd.Arg("query", "query to debug").Required().String()
)

func debugQueryFn(cfg *Config) error {
	index, err := bleve.Open(cfg.Index())
	if err != nil {
		return err
	}
	defer index.Close()
	q := bleve.NewQueryStringQuery(*debugQueryQuery)
	s, err := bleve.DumpQuery(index.Mapping(), q)
	fmt.Println(s)
	return err
}

var (
	analyzeCmd = app.Command("analyze", "process input with bleve analyzer")
	analyzeArg = analyzeCmd.Arg("text", "text to analyze").Required().String()
)

func analyzeFn(cfg *Config) error {
	reExc, err := regexp.Compile(`(?i)c\+\+`)
	if err != nil {
		return err
	}
	uni := unicode.NewUnicodeTokenizer()
	tokenizer := exception.NewExceptionsTokenizer(reExc, uni)
	tokens := tokenizer.Tokenize([]byte(*analyzeArg))
	for _, t := range tokens {
		fmt.Println(t)
	}
	return nil
}

var (
	kvdbPrefixesCmd = app.Command("debugkvdbprefixes", "print kvdb store prefixes")
	kvdbPrefixesArg = kvdbPrefixesCmd.Arg("path", "path to store").Required().String()
)

func kvdbPrefixesFn(cfg *Config) error {
	db, err := OpenKVDB(*kvdbPrefixesArg, 0)
	if err != nil {
		return err
	}
	var prefixes [][]byte
	err = db.View(func(tx *Tx) error {
		prefixes, err = tx.ListPrefixes()
		return err
	})
	if err != nil {
		return err
	}
	for _, p := range prefixes {
		fmt.Println(string(p))
	}
	return nil
}

var (
	geocodedCmd = app.Command("geocoded", "print geocoded locations")
)

func geocodedFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	ids, err := store.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		offer, err := getStoreJsonOffer(store, id)
		if err != nil {
			return err
		}
		place := offer.Location

		loc, _, err := store.GetLocation(id)
		if err != nil {
			return err
		}
		result := "?"
		if loc != nil {
			result = loc.String()
		}
		fmt.Printf("%s: %q => %s\n", id, place, result)
	}
	return nil
}

var (
	listDeletedCmd = app.Command("list-deleted", "list deleted offers")
)

func listDeletedFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	deleted, err := store.ListDeletedIds()
	if err != nil {
		return err
	}
	for _, id := range deleted {
		entries, err := store.ListDeletedOffers(id)
		if err != nil {
			return err
		}
		fmt.Printf("%s: ", id)
		for i, e := range entries {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s", e.Date)
		}
		fmt.Println()
	}
	return nil
}
