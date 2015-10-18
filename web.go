package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
)

type offerData struct {
	Account  string
	Title    string
	Date     string
	Salary   string
	URL      string
	Location string
}

type datedOffer struct {
	Date string
	Id   string
}

type sortedDatedOffers []datedOffer

func (s sortedDatedOffers) Len() int {
	return len(s)
}

func (s sortedDatedOffers) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedDatedOffers) Less(i, j int) bool {
	return s[i].Date > s[j].Date
}

func formatOffers(templ *template.Template, store *Store, datedOffers []datedOffer,
	query string, searchDuration time.Duration, w http.ResponseWriter,
	r *http.Request) error {

	start := time.Now()
	offers := []*offerData{}
	maxDisplayed := 1000
	sort.Sort(sortedDatedOffers(datedOffers))
	for _, doc := range datedOffers {
		if len(offers) >= maxDisplayed {
			break
		}
		offer, err := getStoreOffer(store, doc.Id)
		if err != nil {
			return err
		}
		if offer == nil {
			continue
		}
		salary := ""
		if offer.MinSalary > 0 {
			if offer.MaxSalary != offer.MinSalary {
				salary = fmt.Sprintf("(%d - %d kEUR)",
					offer.MinSalary, offer.MaxSalary)
			} else {
				salary = fmt.Sprintf("(%d kEUR)", offer.MinSalary)
			}
		}
		offers = append(offers, &offerData{
			Account:  offer.Account,
			Title:    offer.Title,
			Date:     offer.Date.Format("2006-01-02"),
			URL:      offer.URL,
			Salary:   salary,
			Location: offer.Location,
		})
	}
	end := time.Now()
	second := float64(time.Second)
	data := struct {
		Offers            []*offerData
		Displayed         int
		Total             int
		Query             string
		SearchDuration    string
		RenderingDuration string
	}{
		Offers:            offers,
		Displayed:         len(offers),
		Total:             len(datedOffers),
		Query:             query,
		SearchDuration:    fmt.Sprintf("%0.3f", float64(searchDuration)/second),
		RenderingDuration: fmt.Sprintf("%0.3f", float64(end.Sub(start))/second),
	}
	h := w.Header()
	h.Set("Content-Type", "text/html")
	templ.Execute(w, &data)
	return nil
}

func findOffersFromText(index bleve.Index, query string) ([]datedOffer, error) {
	if query == "" {
		return nil, nil
	}
	datedOffers := []datedOffer{}
	q := bleve.NewQueryStringQuery(query)
	rq := bleve.NewSearchRequest(q)
	rq.Size = 20000
	rq.Fields = []string{"date"}
	res, err := index.Search(rq)
	if err != nil {
		return nil, err
	}
	for _, doc := range res.Hits {
		date, ok := doc.Fields["date"].(string)
		if !ok {
			return nil, fmt.Errorf("could not retrieve date for %s", doc.ID)
		}
		datedOffers = append(datedOffers, datedOffer{
			Date: date,
			Id:   doc.ID,
		})
	}
	return datedOffers, nil
}

func findOffersFromLocation(query string, spatial *SpatialIndex, geocoder *Geocoder) (
	[]datedOffer, error) {

	parts := strings.Split(query, ",")
	lat, lon, radius := float64(0), float64(0), float64(0)
	if len(parts) == 2 {
		loc, err := geocoder.GeocodeFromCache(strings.ToLower(parts[0]), "fr")
		if err != nil {
			return nil, err
		}
		if loc == nil || len(loc.Results) == 0 || loc.Results[0].Geometry == nil {
			return nil, fmt.Errorf("could not geocode %s", query)
		}
		g := loc.Results[0].Geometry
		lat = g.Lat
		lon = g.Lon
		radius, err = strconv.ParseFloat(parts[1], 64)
		if err != nil {
			return nil, err
		}
	} else if len(parts) == 3 {
		floats := []float64{}
		for _, p := range parts {
			f, err := strconv.ParseFloat(p, 64)
			if err != nil {
				return nil, err
			}
			floats = append(floats, f)
		}
		lat = floats[0]
		lon = floats[1]
		radius = floats[2]
	} else {
		return nil, fmt.Errorf("location query must be like: lat,lng,radius or name,radius")
	}
	datedOffers, err := spatial.FindNearest(lat, lon, radius)
	return datedOffers, err
}

func serveQuery(templ *template.Template, store *Store, index bleve.Index,
	spatial *SpatialIndex, geocoder *Geocoder, w http.ResponseWriter, r *http.Request) error {

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	query := values.Get("q")
	var datedOffers []datedOffer
	prefix := "loc:"
	start := time.Now()
	if strings.HasPrefix(query, prefix) {
		datedOffers, err = findOffersFromLocation(query[len(prefix):], spatial, geocoder)
	} else {
		datedOffers, err = findOffersFromText(index, query)
	}
	searchDuration := time.Now().Sub(start)
	if err != nil {
		return err
	}
	err = formatOffers(templ, store, datedOffers, query, searchDuration, w, r)
	formatDuration := time.Now().Sub(start) - searchDuration
	log.Printf("query '%s' returned %d entries in %.3fs, formatted in %.3fs",
		query, len(datedOffers), float64(searchDuration)/float64(time.Second),
		float64(formatDuration)/float64(time.Second))
	return err
}

func handleQuery(templ *template.Template, store *Store, index bleve.Index,
	spatial *SpatialIndex, geocoder *Geocoder, w http.ResponseWriter, r *http.Request) {
	err := serveQuery(templ, store, index, spatial, geocoder, w, r)
	if err != nil {
		log.Printf("error: query failed with: %s", err)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(400)
		fmt.Fprintf(w, "error: %s\n", err)
	}
}

func enforcePost(rq *http.Request, w http.ResponseWriter) bool {
	if rq.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return true
	}
	return false
}

type GeocodingHandler struct {
	geocoder *Geocoder
	store    *Store
	spatial  *SpatialIndex
	lock     sync.Mutex
	running  bool
}

func NewGeocodingHandler(store *Store, geocoder *Geocoder,
	spatial *SpatialIndex) *GeocodingHandler {

	return &GeocodingHandler{
		store:    store,
		geocoder: geocoder,
		spatial:  spatial,
	}
}

func (h *GeocodingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if enforcePost(r, w) {
		return
	}
	h.Geocode()
	w.Write([]byte("OK"))
}

func (h *GeocodingHandler) Geocode() {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.running {
		return
	}
	h.running = true
	go func() {
		defer func() {
			h.lock.Lock()
			defer h.lock.Unlock()
			h.running = false
		}()
		log.Println("geocoding started")
		err := h.geocode(500)
		if err != nil {
			log.Printf("error: geocoding failed: %s", err)
		}
		log.Println("geocoding stopped")
	}()
}

func (h *GeocodingHandler) geocode(minQuota int) error {
	ids, err := h.store.List()
	if err != nil {
		return err
	}
	rejected := 0
	for i, id := range ids {
		offer, err := getStoreOffer(h.store, id)
		if err != nil {
			return err
		}
		if offer == nil {
			continue
		}
		q, loc, err := geocodeOffer(h.geocoder, offer, rejected > 0)
		if err != nil {
			log.Printf("error: geocoding %s: %s\n", q, err)
			if err != QuotaError {
				return err
			}
			rejected += 1
		} else if loc == nil {
			rejected += 1
		} else if !loc.Cached {
			result := "no result"
			if len(loc.Results) > 0 {
				result = loc.Results[0].Component.String()
			}
			prefix := fmt.Sprintf("geocoding %d/%d %s => %s => %s", i+1, len(ids),
				offer.Location, q, result)
			if !loc.Cached {
				offerLoc, err := makeOfferLocation(offer.Id, offer.Date, loc)
				if err != nil {
					log.Printf("error: cannot make offer location for %s: %s", id, err)
				} else if offerLoc != nil {
					h.spatial.Remove(offer.Id)
					h.spatial.Add(offerLoc)
				}
				log.Printf("%s (quota: %d/%d)\n", prefix, loc.Rate.Remaining,
					loc.Rate.Limit)
				if loc.Rate.Remaining <= minQuota {
					// Try to preserve quota for test purpose. This is not
					// perfect as it consumes one geocoding token per function
					// call. I do not know how to query quota directly yet.
					rejected += 1
				}
				time.Sleep(1 * time.Second)
			} else {
				log.Printf("%s\n", prefix)
			}
		}
	}
	return nil
}

var (
	webCmd  = app.Command("web", "APEC web frontend")
	webHttp = webCmd.Flag("http", "http server address").Default(":8081").String()
)

func web(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return fmt.Errorf("cannot open data store: %s", err)
	}
	defer store.Close()
	index, err := OpenOfferIndex(cfg.Index())
	if err != nil {
		return fmt.Errorf("cannot open index: %s", err)
	}
	defer index.Close()
	templ, err := template.ParseGlob("web/*.tmpl")
	if err != nil {
		return err
	}
	geocoder, err := NewGeocoder(cfg.GeocodingKey(), cfg.Geocoder())
	if err != nil {
		return fmt.Errorf("cannot open geocoder: %s", err)
	}
	spatial := NewSpatialIndex()
	queue, err := OpenIndexQueue(cfg.Queue())
	if err != nil {
		return err
	}
	defer queue.Close()
	indexer := NewIndexer(store, index, geocoder, queue)
	defer indexer.Close()
	indexer.Sync()

	spatialIndexer := NewSpatialIndexer(store, spatial, geocoder)
	defer spatialIndexer.Close()
	spatialIndexer.Sync()

	geocodingHandler := NewGeocodingHandler(store, geocoder, spatial)
	geocodingHandler.Geocode()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(templ, store, index, spatial, geocoder, w, r)
	})
	http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		if enforcePost(r, w) {
			return
		}
		indexer.Sync()
		spatialIndexer.Sync()
		w.Write([]byte("OK"))
	})

	crawlingLock := sync.Mutex{}
	crawling := false
	http.HandleFunc("/crawl", func(w http.ResponseWriter, r *http.Request) {
		if enforcePost(r, w) {
			return
		}
		crawlingLock.Lock()
		defer crawlingLock.Unlock()
		if !crawling {
			crawling = true
			go func() {
				defer func() {
					crawlingLock.Lock()
					crawling = false
					crawlingLock.Unlock()
				}()
				err := crawl(store, 0, nil)
				if err != nil {
					log.Printf("error: crawling failed with: %s", err)
					return
				}
				indexer.Sync()
				spatialIndexer.Sync()
				geocodingHandler.Geocode()
			}()
		}
		w.Write([]byte("OK"))
	})
	http.Handle("/geocode", geocodingHandler)
	return http.ListenAndServe(*webHttp, nil)
}
