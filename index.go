package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/analysis/char/html"
	"github.com/blevesearch/bleve/analysis/lang/fr"
	"github.com/blevesearch/bleve/analysis/token/lowercase"
	"github.com/blevesearch/bleve/analysis/token/stop"
	"github.com/blevesearch/bleve/analysis/tokenizer/exception"
	bleveuni "github.com/blevesearch/bleve/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/analysis/tokenmap"
	"github.com/blevesearch/bleve/index/store/boltdb"
	"github.com/blevesearch/bleve/index/upsidedown"
	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
)

func loadOffers(store *Store) ([]*jstruct.JsonOffer, error) {
	type offerResult struct {
		Id    string
		Offer *jstruct.JsonOffer
		Err   error
	}

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

	offers := []*jstruct.JsonOffer{}
	for r := range results {
		if r.Err != nil {
			fmt.Printf("loading error for %s: %s\n", r.Id, r.Err)
			continue
		}
		if r.Offer == nil {
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
}

const (
	ApecURL = "https://cadres.apec.fr/home/mes-offres/recherche-des-offres-demploi/" +
		"liste-des-offres-demploi/detail-de-loffre-demploi.html?numIdOffre="
)

func convertOffer(offer *jstruct.JsonOffer) (*Offer, error) {
	r := &Offer{
		Account:  offer.Account,
		Id:       offer.Id,
		HTML:     offer.HTML,
		Title:    offer.Title,
		URL:      ApecURL + offer.Id,
		Location: offer.Location,
	}
	if r.Location == "" && len(offer.Locations) > 0 {
		r.Location = offer.Locations[0].Name
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

func getStoreJsonOffer(store *Store, id string) (*jstruct.JsonOffer, error) {
	data, err := store.Get(id)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	js := &jstruct.JsonOffer{}
	err = ffjson.Unmarshal(data, js)
	return js, err
}

func getStoreOffer(store *Store, id string) (*Offer, error) {
	js, err := getStoreJsonOffer(store, id)
	if err != nil || js == nil {
		return nil, err
	}
	return convertOffer(js)
}

func convertOffers(offers []*jstruct.JsonOffer) ([]*Offer, error) {
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

var (
	indexExceptions = []string{
		"c++",
		"c#",
	}
	stopWords = []interface{}{
		"h", // The H in H/F
		"f", // The F in H/F
		// Generic garbage
		"mision",
		"post",
		"entrepris",
		"suivi",
	}
)

func NewOfferIndex(dir string) (bleve.Index, error) {
	err := os.RemoveAll(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	parts := []string{}
	for _, exc := range indexExceptions {
		parts = append(parts, regexp.QuoteMeta(exc))
	}
	pattern := strings.Join(parts, "|")
	pattern = "(?i)(?:" + pattern + ")"

	m := bleve.NewIndexMapping()
	apecTokenizer := "apec"
	err = m.AddCustomTokenizer(apecTokenizer, map[string]interface{}{
		"type":       exception.Name,
		"exceptions": []string{pattern},
		"tokenizer":  bleveuni.Name,
	})
	if err != nil {
		return nil, err
	}

	apecTokens := "apec_tokens"
	err = m.AddCustomTokenMap(apecTokens, map[string]interface{}{
		"type":   tokenmap.Name,
		"tokens": stopWords,
	})
	if err != nil {
		return nil, err
	}

	apecStop := "apec_stop"
	err = m.AddCustomTokenFilter(apecStop, map[string]interface{}{
		"type":           stop.Name,
		"stop_token_map": apecTokens,
	})
	if err != nil {
		return nil, err
	}

	frTokens := []string{
		lowercase.Name,
		fr.ElisionName,
		fr.StopName,
		fr.LightStemmerName,
		apecStop,
	}
	fr := map[string]interface{}{
		"type":          custom.Name,
		"tokenizer":     apecTokenizer,
		"token_filters": frTokens,
	}
	frHtml := map[string]interface{}{
		"type": custom.Name,
		"char_filters": []string{
			html.Name,
		},
		"tokenizer":     apecTokenizer,
		"token_filters": frTokens,
	}
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

	index, err := bleve.NewUsing(dir, m, upsidedown.Name, boltdb.Name,
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
	indexMinQuota = indexCmd.Flag("min-quota",
		"stop geocoding when call quota moves below supplied value").Default("500").Int()
	indexDocId = indexCmd.Flag("id", "index only specified document").String()
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
	if *indexDocId != "" {
		kept := []*jstruct.JsonOffer{}
		for _, o := range rawOffers {
			if o.Id == *indexDocId {
				kept = append(kept, o)
			}
		}
		rawOffers = kept
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
		rejected, err = geocodeOffers(store, geocoder, offers, *indexMinQuota)
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
