package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/tokenizer/exception"
	"github.com/blevesearch/bleve/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/search/query"
	"github.com/pmezard/apec/jstruct"
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
	q, err := makeSearchQuery(*debugQueryQuery, nil)
	if err != nil {
		return err
	}
	s, err := query.DumpQuery(index.Mapping(), q)
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
	geocodedCmd = app.Command("geocoded", "print geocoded locations")
	geocodedIds = geocodedCmd.Flag("ids", "display offer identifiers").
			Default("true").Bool()
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
		if *geocodedIds {
			fmt.Printf("%s: %q => %s\n", id, place, result)
		} else {
			fmt.Printf("%q => %s\n", place, result)
		}
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

var (
	dumpOfferCmd = app.Command("dump-offer",
		"print active and deleted versions of an offer")
	dumpOfferIds = dumpOfferCmd.Arg("id", "offer identifier").Required().Strings()
)

func printJsonOffer(store *Store, id string, deletedId uint64) error {
	js := &jstruct.JsonOffer{}
	var data []byte
	var err error
	if deletedId == 0 {
		data, err = store.Get(id)
	} else {
		data, err = store.GetDeleted(deletedId)
	}
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, js)
	if err != nil {
		return err
	}
	s, err := json.MarshalIndent(js, "", " ")
	if err != nil {
		return err
	}
	_, err = fmt.Printf("%s\n", s)
	return err
}

func dumpOfferFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	for _, dumpOfferId := range *dumpOfferIds {
		deletedIds, err := store.ListDeletedOffers(dumpOfferId)
		if err != nil {
			return err
		}
		for _, id := range deletedIds {
			err = printJsonOffer(store, dumpOfferId, id.Id)
			if err != nil {
				return err
			}
		}
		data, err := store.Get(dumpOfferId)
		if err != nil {
			return err
		}
		if data != nil {
			err = printJsonOffer(store, dumpOfferId, 0)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

var (
	dumpOffersCmd    = app.Command("offers", "dump all offers in jsonl")
	dumpOffersActive = dumpOffersCmd.Flag("active", "dump only active offers").Bool()
	dumpOffersPrefix = dumpOffersCmd.Flag("prefix", "output name prefix").
				Default("offers").String()
)

func addDeletedDate(data []byte, date string) ([]byte, error) {
	doc := map[string]interface{}{}
	err := json.Unmarshal(data, &doc)
	if err != nil {
		return nil, err
	}
	doc["deletionDate"] = date
	return json.Marshal(&doc)
}

func enumerateOffersBytes(store *Store, callback func(data []byte) error) error {
	// Enumerate deleted offers
	ids, err := store.ListDeletedIds()
	if err != nil {
		return err
	}
	if !*dumpOffersActive {
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
				data, err = addDeletedDate(data, deleted.Date)
				if err != nil {
					return err
				}
				err = callback(data)
				if err != nil {
					return err
				}
			}
		}
	}
	// Enumerate valid offers
	ids, err = store.List()
	if err != nil {
		return err
	}
	for _, id := range ids {
		data, err := store.Get(id)
		if err != nil {
			return err
		}
		if data != nil {
			err = callback(data)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type OfferWriter struct {
	prefix     string
	suffix     string
	fp         *os.File
	maxPerFile int
	written    int
	index      int
}

func NewOfferWriter(prefix, suffix string) (*OfferWriter, error) {
	w := &OfferWriter{
		prefix:     prefix,
		suffix:     suffix,
		maxPerFile: 50000,
	}
	err := w.rotate()
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *OfferWriter) rotate() error {
	w.Close()
	path := fmt.Sprintf("%s-%03d%s", w.prefix, w.index, w.suffix)
	fmt.Println("opening", path)
	fp, err := os.Create(path)
	if err != nil {
		return err
	}
	w.fp = fp
	w.index += 1
	w.written = 0
	return nil
}

func (w *OfferWriter) Close() error {
	if w.fp != nil {
		return w.fp.Close()
	}
	return nil
}

func (w *OfferWriter) WriteBytes(data []byte) error {
	if bytes.ContainsAny(data, "\n") {
		return fmt.Errorf("EOL found in json line")
	}
	parts := [][]byte{
		data,
		[]byte("\n"),
	}
	for _, buf := range parts {
		_, err := w.fp.Write(buf)
		if err != nil {
			return err
		}
	}
	w.written += 1
	if w.written >= w.maxPerFile {
		err := w.rotate()
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpOffersFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	w, err := NewOfferWriter(*dumpOffersPrefix, ".jsonl")
	if err != nil {
		return err
	}
	err = enumerateOffersBytes(store, func(data []byte) error {
		return w.WriteBytes(data)
	})
	return w.Close()
}
