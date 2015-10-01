package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/blevesearch/bleve"

	"golang.org/x/text/encoding/charmap"
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
	sort.Strings(ids)
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
	Account   string
	Id        string `json:"id"`
	HTML      string `json:"html"`
	Title     string `json:"title"`
	MinSalary int    `json:"min_salary"`
	MaxSalary int    `json:"max_salary"`
	Date      time.Time
	URL       string
	Location  string `json:"location"`
	City      string `json:"city"`
	County    string `json:"county"`
	State     string `json:"state"`
	Country   string `json:"country"`
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

const (
	ApecURL = "https://cadres.apec.fr/offres-emploi-cadres/offre.html?numIdOffre="
)

func convertOffer(offer *jsonOffer) (*Offer, error) {
	r := &Offer{
		Account:  offer.Account,
		Id:       offer.Id,
		HTML:     offer.HTML,
		Title:    offer.Title,
		URL:      ApecURL + offer.Id,
		Location: offer.Location,
	}
	min, max, err := parseSalary(offer.Salary)
	if err != nil {
		return nil, fmt.Errorf("cannot parse salary %q: %s", offer.Salary, err)
	}
	d, err := time.Parse("2006-01-02T15:04:05.000+0000", offer.Date)
	if err != nil {
		return nil, err
	}
	r.Date = d
	r.MinSalary = min
	r.MaxSalary = max
	return r, nil
}

func convertOffers(offers []*jsonOffer) ([]*Offer, error) {
	result := make([]*Offer, 0, len(offers))
	for _, o := range offers {
		r, err := convertOffer(o)
		if err != nil {
			fmt.Printf("error: cannot parse salary %q: %s\n", o.Salary, err)
			continue
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

	textAll := bleve.NewTextFieldMapping()
	textAll.Store = false
	textAll.IncludeTermVectors = false

	text := bleve.NewTextFieldMapping()
	text.Store = false
	text.IncludeInAll = false
	text.IncludeTermVectors = false

	offer := bleve.NewDocumentStaticMapping()
	offer.Dynamic = false
	offer.AddFieldMappingsAt("html", textAll)
	offer.AddFieldMappingsAt("title", textAll)
	offer.AddFieldMappingsAt("city", text)
	offer.AddFieldMappingsAt("county", text)
	offer.AddFieldMappingsAt("state", text)
	offer.AddFieldMappingsAt("country", text)

	m := bleve.NewIndexMapping()
	m.AddDocumentMapping("offer", offer)
	m.DefaultMapping = offer

	index, err := bleve.New(filepath.Join(dir, "index"), m)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func fixLocation(s string) string {
	if !utf8.ValidString(s) {
		fmt.Printf("invalid: %s\n", s)
		u, _, err := transform.String(charmap.Windows1252.NewDecoder(), s)
		if err != nil {
			fmt.Printf("invalid: %s\n", s)
			return s
		}
		if s != u {
			fmt.Printf("recoded: %s => %s\n", s, u)
		}
		s = u
	}
	s = strings.TrimSpace(s)
	l := strings.ToLower(s)
	if l == "idf" {
		return "Ile-de-France"
	}
	return s
}

func geocodeOffer(geocoder *Geocoder, offer *Offer) (string, *Location, error) {
	q := fixLocation(offer.Location)
	loc, err := geocoder.Geocode(q, "fr")
	if err != nil {
		return q, nil, err
	}
	if len(loc.Results) == 0 {
		return q, loc, nil
	}
	res := loc.Results[0].Component
	offer.City = res.City
	offer.County = res.County
	offer.State = res.State
	offer.Country = res.Country
	return q, loc, nil
}

var (
	indexCmd     = app.Command("index", "index APEC offers")
	indexDataDir = indexCmd.Flag("data", "data directory").Default("offers").String()
	indexMaxSize = indexCmd.Flag("max-count", "maximum number of items to index").
			Short('n').Default("0").Int()
	indexGeocoderKey = indexCmd.Flag("geocoding-key", "geocoder API key").String()
)

func indexOffers() error {
	dirs := NewDataDirs(*indexDataDir)
	var geocoder *Geocoder
	if *indexGeocoderKey != "" {
		g, err := NewGeocoder(*indexGeocoderKey, dirs.Geocoder())
		if err != nil {
			return err
		}
		geocoder = g
		defer func() {
			if geocoder != nil {
				geocoder.Close()
			}
		}()
	}
	store, err := OpenStore(dirs.Store())
	if err != nil {
		return err
	}
	index, err := NewOfferIndex(dirs.Index())
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
		if geocoder != nil {
			q, loc, err := geocodeOffer(geocoder, offer)
			if err != nil {
				fmt.Printf("error: geocoding %s: %s\n", q, err)
				if err == QuotaError {
					geocoder = nil
					break
				}
			} else if !loc.Cached {
				result := "no result"
				if len(loc.Results) > 0 {
					result = loc.Results[0].Component.String()
				}
				fmt.Printf("geocoding %s => %s (quota: %d/%d)\n", q, result,
					loc.Rate.Remaining, loc.Rate.Limit)
				time.Sleep(1 * time.Second)
			}
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
