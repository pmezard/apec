package main

import (
	"encoding/json"
	"fmt"
	"github.com/blevesearch/bleve"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func loadOffer(store *Store, id string) (*jsonOffer, error) {
	data, err := store.Get(id)
	if err != nil {
		return nil, err
	}
	offer := &jsonOffer{}
	err = json.Unmarshal(data, offer)
	return offer, err
}

type offerResult struct {
	Id    string
	Offer *jsonOffer
	Err   error
}

func loadOffers(store *Store) ([]*jsonOffer, error) {
	ids := store.List()
	pending := make(chan string, len(ids))
	for _, id := range ids {
		pending <- id
	}
	close(pending)

	results := make(chan offerResult, len(ids))
	running := &sync.WaitGroup{}
	jobs := 4
	for i := 0; i < jobs; i++ {
		running.Add(1)
		go func() {
			defer running.Done()
			for id := range pending {
				offer, err := loadOffer(store, id)
				results <- offerResult{
					Id:    id,
					Offer: offer,
					Err:   err,
				}
			}
		}()
	}
	go func() {
		running.Wait()
		close(results)
	}()

	offers := []*jsonOffer{}
	for r := range results {
		if r.Err != nil {
			fmt.Printf("loading error for %s: %s\n", r.Id, r.Err)
			continue
		}
		offers = append(offers, r.Offer)
	}
	return offers, nil
}

type Offer struct {
	Id        string `json:"id"`
	HTML      string `json:"html"`
	Title     string `json:"title"`
	MinSalary int    `json:"min_salary"`
	MaxSalary int    `json:"max_salary"`
}

var (
	reSalaryNum   = regexp.MustCompile(`(\d+(?:\.\d+)?)`)
	reSalaryUndef = regexp.MustCompile(`^(?:.*(definir|negoc|profil|experience|a voir|determiner|attract|precise|selon|competitif).*|nc|-)$`)
)

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

var (
	cleaner = transform.Chain(norm.NFD,
		transform.RemoveFunc(isMn),
		norm.NFC)
)

func normString(s string) string {
	result, _, _ := transform.String(cleaner, s)
	return result
}

func parseSalary(s string) (int, int, error) {
	s = strings.ToLower(normString(s))
	m := reSalaryNum.FindAllStringSubmatch(s, -1)
	if m != nil {
		values := []int{}
		for _, n := range m {
			v, err := strconv.ParseFloat(n[0], 32)
			if err != nil {
				return -1, -1, err
			}
			if v >= 1000 {
				v = v / 1000.
			}
			values = append(values, int(v))
		}
		switch len(values) {
		case 1:
			return values[0], values[0], nil
		case 2:
			return values[0], values[1], nil
		}
		return 0, 0, fmt.Errorf("too many numbers")
	}
	return 0, 0, nil
}

func convertOffers(offers []*jsonOffer) ([]*Offer, error) {
	result := make([]*Offer, 0, len(offers))
	for _, o := range offers {
		r := &Offer{
			Id:    o.Id,
			HTML:  o.HTML,
			Title: o.Title,
		}
		min, max, err := parseSalary(o.Salary)
		if err == nil {
			r.MinSalary = min
			r.MaxSalary = max
		} else {
			fmt.Printf("error: cannot parse salary %q: %s\n", o.Salary, err)
		}
		result = append(result, r)
	}
	return result, nil
}

func NewOfferIndex(dir string) (bleve.Index, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	html := bleve.NewTextFieldMapping()
	html.Store = false
	//html.IncludeInAll = false
	html.IncludeTermVectors = false

	num := bleve.NewNumericFieldMapping()
	num.Store = false
	num.IncludeTermVectors = false
	num.IncludeInAll = false

	offer := bleve.NewDocumentStaticMapping()
	offer.Dynamic = false
	offer.AddFieldMappingsAt("html", html)
	offer.AddFieldMappingsAt("title", html)
	offer.AddFieldMappingsAt("min_salary", num)
	offer.AddFieldMappingsAt("max_salary", num)

	m := bleve.NewIndexMapping()
	m.AddDocumentMapping("offer", offer)
	m.DefaultMapping = offer

	index, err := bleve.New(filepath.Join(dir, "index"), m)
	if err != nil {
		return nil, err
	}
	return index, nil
}

var (
	indexCmd      = app.Command("index", "index APEC offers")
	indexStoreDir = indexCmd.Arg("store", "data store directory").Required().String()
	indexIndexDir = indexCmd.Arg("index", "index directory").Required().String()
	indexMaxSize  = indexCmd.Flag("max-count", "maximum number of items to index").
			Short('n').Default("0").Int()
)

func indexOffers() error {
	store, err := OpenStore(*indexStoreDir)
	if err != nil {
		return err
	}
	index, err := NewOfferIndex(*indexIndexDir)
	if err != nil {
		return err
	}
	rawOffers, err := loadOffers(store)
	if err != nil {
		return err
	}
	offers, err := convertOffers(rawOffers)
	if err != nil {
		return err
	}
	if *indexMaxSize > 0 && len(offers) > *indexMaxSize {
		offers = offers[:*indexMaxSize]
	}
	start := time.Now()
	for i, offer := range offers {
		if (i+1)%500 == 0 {
			now := time.Now()
			elapsed := float64(now.Sub(start)) / float64(time.Second)
			fmt.Printf("%d indexed, %.1f/s\n", i+1, float64(i+1)/elapsed)
		}
		err = index.Index(offer.Id, offer)
		if err != nil {
			return err
		}
	}
	err = index.Close()
	if err != nil {
		return err
	}
	end := time.Now()
	fmt.Printf("%d documents indexed in %.2fs\n", len(offers),
		float64(end.Sub(start))/float64(time.Second))
	return nil
}
