package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer"
	"github.com/blevesearch/bleve/analysis/char_filters/html_char_filter"
	"github.com/blevesearch/bleve/analysis/language/fr"
	"github.com/blevesearch/bleve/analysis/token_filters/lower_case_filter"
	bleveuni "github.com/blevesearch/bleve/analysis/tokenizers/unicode"
	"github.com/blevesearch/bleve/index/store/boltdb"
	"github.com/blevesearch/bleve/index/upside_down"

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
	err := os.RemoveAll(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	frTokens := []string{
		lower_case_filter.Name,
		fr.ElisionName,
		fr.StopName,
		fr.LightStemmerName,
	}
	fr := map[string]interface{}{
		"type":          custom_analyzer.Name,
		"tokenizer":     bleveuni.Name,
		"token_filters": frTokens,
	}
	frHtml := map[string]interface{}{
		"type": custom_analyzer.Name,
		"char_filters": []string{
			html_char_filter.Name,
		},
		"tokenizer":     bleveuni.Name,
		"token_filters": frTokens,
	}
	m := bleve.NewIndexMapping()
	err = m.AddCustomAnalyzer("fr", fr)
	if err != nil {
		return nil, fmt.Errorf("failed to register analyzer fr: %s", err)
	}
	err = m.AddCustomAnalyzer("fr_html", frHtml)
	if err != nil {
		return nil, fmt.Errorf("failed to register analyzer fr_html: %s", err)
	}

	html := bleve.NewTextFieldMapping()
	html.Store = false
	html.IncludeTermVectors = false
	html.Analyzer = "fr_html"

	textFr := bleve.NewTextFieldMapping()
	textFr.Store = false
	textFr.IncludeTermVectors = false
	textFr.Analyzer = "fr"

	text := bleve.NewTextFieldMapping()
	text.Store = false
	text.IncludeInAll = false
	text.IncludeTermVectors = false

	offer := bleve.NewDocumentStaticMapping()
	offer.Dynamic = false
	offer.AddFieldMappingsAt("html", textFr)
	offer.AddFieldMappingsAt("title", textFr)
	offer.AddFieldMappingsAt("city", text)
	offer.AddFieldMappingsAt("county", text)
	offer.AddFieldMappingsAt("state", text)
	offer.AddFieldMappingsAt("country", text)

	m.AddDocumentMapping("offer", offer)
	m.DefaultMapping = offer

	index, err := bleve.NewUsing(dir, m, upside_down.Name, boltdb.Name,
		map[string]interface{}{
			"nosync": true,
		})
	if err != nil {
		return nil, err
	}
	return index, nil
}

var (
	indexCmd     = app.Command("index", "index APEC offers")
	indexMaxSize = indexCmd.Flag("max-count", "maximum number of items to index").
			Short('n').Default("0").Int()
	indexNoIndex = indexCmd.Flag("no-index", "disable indexing").Bool()
	indexVerbose = indexCmd.Flag("verbose", "verbose mode").Short('v').Bool()
)

func indexOffers(cfg *Config) error {
	var geocoder *Geocoder
	key := cfg.GeocodingKey()
	if key != "" {
		g, err := NewGeocoder(key, cfg.Geocoder())
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
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	var index bleve.Index
	if !*indexNoIndex {
		index, err = NewOfferIndex(cfg.Index())
		if err != nil {
			return err
		}
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
	rejected := 0
	indexed := 0
	for i, offer := range offers {
		if (i+1)%500 == 0 {
			now := time.Now()
			elapsed := float64(now.Sub(start)) / float64(time.Second)
			fmt.Printf("%d indexed, %.1f/s\n", i+1, float64(i+1)/elapsed)
		}
		if geocoder != nil {
			q, loc, err := geocodeOffer(geocoder, offer, rejected > 0)
			if err != nil {
				fmt.Printf("error: geocoding %s: %s\n", q, err)
				if err != QuotaError {
					return err
				}
				rejected += 1
			} else if loc == nil {
				rejected += 1
			} else if !loc.Cached || *indexVerbose {
				result := "no result"
				if len(loc.Results) > 0 {
					result = loc.Results[0].Component.String()
				}
				if !loc.Cached {
					fmt.Printf("geocoding %s => %s => %s (quota: %d/%d)\n",
						offer.Location, q, result, loc.Rate.Remaining, loc.Rate.Limit)
					time.Sleep(1 * time.Second)
				} else {
					fmt.Printf("geocoding %s => %s => %s\n", offer.Location, q, result)
				}
			}
		} else {
			rejected += 1
		}

		if index != nil {
			err = index.Index(offer.Id, offer)
			if err != nil {
				return err
			}
			indexed += 1
		}
	}
	if index != nil {
		err = index.Close()
		if err != nil {
			return err
		}
	}
	end := time.Now()
	fmt.Printf("%d/%d documents indexed in %.2fs\n", indexed, len(offers),
		float64(end.Sub(start))/float64(time.Second))
	fmt.Printf("%d rejected geocoding\n", rejected)
	return nil
}
