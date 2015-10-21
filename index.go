package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzers/custom_analyzer"
	"github.com/blevesearch/bleve/analysis/char_filters/html_char_filter"
	"github.com/blevesearch/bleve/analysis/language/fr"
	"github.com/blevesearch/bleve/analysis/token_filters/lower_case_filter"
	bleveuni "github.com/blevesearch/bleve/analysis/tokenizers/unicode"
	"github.com/blevesearch/bleve/index/store/boltdb"
	"github.com/blevesearch/bleve/index/upside_down"
)

type offerResult struct {
	Id    string
	Offer *jsonOffer
	Err   error
}

func loadOffers(store *Store) ([]*jsonOffer, error) {
	ids, err := store.List()
	if err != nil {
		return nil, err
	}
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
				offer, err := getStoreJsonOffer(store, id)
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
	Id        string    `json:"id"`
	HTML      string    `json:"html"`
	Title     string    `json:"title"`
	MinSalary int       `json:"min_salary"`
	MaxSalary int       `json:"max_salary"`
	Date      time.Time `json:"date"`
	URL       string
	Location  string `json:"location"`
	City      string `json:"city"`
	County    string `json:"county"`
	State     string `json:"state"`
	Country   string `json:"country"`
}

const (
	ApecURL = "https://cadres.apec.fr/home/mes-offres/recherche-des-offres-demploi/" +
		"liste-des-offres-demploi/detail-de-loffre-demploi.html?numIdOffre="
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

func getStoreJsonOffer(store *Store, id string) (*jsonOffer, error) {
	data, err := store.Get(id)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	js := &jsonOffer{}
	err = json.Unmarshal(data, js)
	return js, err
}

func getStoreOffer(store *Store, id string) (*Offer, error) {
	js, err := getStoreJsonOffer(store, id)
	if err != nil || js == nil {
		return nil, err
	}
	return convertOffer(js)
}

func convertOffers(offers []*jsonOffer) ([]*Offer, error) {
	result := make([]*Offer, 0, len(offers))
	for _, o := range offers {
		r, err := convertOffer(o)
		if err != nil {
			return nil, err
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

	htmlFr := bleve.NewTextFieldMapping()
	htmlFr.Store = false
	htmlFr.IncludeInAll = false
	htmlFr.IncludeTermVectors = false
	htmlFr.Analyzer = "fr_html"

	textFr := bleve.NewTextFieldMapping()
	textFr.Store = false
	textFr.IncludeInAll = false
	textFr.IncludeTermVectors = false
	textFr.Analyzer = "fr"

	textAll := bleve.NewTextFieldMapping()
	textAll.Store = false
	textAll.IncludeInAll = true
	textAll.IncludeTermVectors = false

	date := bleve.NewDateTimeFieldMapping()
	date.Index = false
	date.Store = true
	date.IncludeInAll = false
	date.IncludeTermVectors = false

	offer := bleve.NewDocumentStaticMapping()
	offer.Dynamic = false
	offer.AddFieldMappingsAt("html", htmlFr)
	offer.AddFieldMappingsAt("title", textFr)
	offer.AddFieldMappingsAt("date", date)

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

func OpenOfferIndex(path string) (bleve.Index, error) {
	return bleve.OpenUsing(path, map[string]interface{}{
		"nosync": false,
	})
}

var (
	indexCmd     = app.Command("index", "index APEC offers")
	indexMaxSize = indexCmd.Flag("max-count", "maximum number of items to index").
			Short('n').Default("0").Int()
	// Work around kingpin messing with boolean flags starting with --no-xxx (#54)
	indexIndex = indexCmd.Flag("index", "enable indexing (use --no-index to disable it)").
			Default("true").Bool()
	indexVerbose  = indexCmd.Flag("verbose", "verbose mode").Short('v').Bool()
	indexMinQuota = indexCmd.Flag("min-quota",
		"stop geocoding when call quota moves below supplied value").Default("500").Int()
)

func indexOffers(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()
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

	rejected := 0
	geocodingKey := cfg.GeocodingKey()
	if geocodingKey != "" {
		geocoder, err := NewGeocoder(geocodingKey, cfg.Geocoder())
		if err != nil {
			return err
		}
		defer geocoder.Close()
		rejected, err = geocodeOffers(geocoder, offers, *indexMinQuota, *indexVerbose)
		if err != nil {
			return err
		}
		fmt.Printf("%d rejected geocoding\n", rejected)
	}
	if *indexIndex {
		index, err := NewOfferIndex(cfg.Index())
		if err != nil {
			return err
		}
		start := time.Now()
		indexed := 0
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
			indexed += 1
		}
		err = index.Close()
		if err != nil {
			return err
		}
		end := time.Now()
		fmt.Printf("%d/%d documents indexed in %.2fs\n", indexed, len(offers),
			float64(end.Sub(start))/float64(time.Second))
	}
	return nil
}
