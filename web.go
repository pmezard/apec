package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

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
	query string, w http.ResponseWriter, r *http.Request) error {

	offers := []*offerData{}
	maxDisplayed := 1000
	sort.Sort(sortedDatedOffers(datedOffers))
	for _, doc := range datedOffers {
		if len(offers) >= maxDisplayed {
			break
		}
		data, err := store.Get(doc.Id)
		if err != nil {
			return err
		}
		o := &jsonOffer{}
		err = json.Unmarshal(data, o)
		if err != nil {
			return err
		}
		offer, err := convertOffer(o)
		if err != nil {
			fmt.Printf("error: cannot convert offer: %s\n", err)
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
	data := struct {
		Offers    []*offerData
		Displayed int
		Total     int
		Query     string
	}{
		Offers:    offers,
		Displayed: len(offers),
		Total:     len(datedOffers),
		Query:     query,
	}
	h := w.Header()
	h.Set("Content-Type", "text/html")
	templ.Execute(w, &data)
	return nil
}

func findOffersFromText(index bleve.Index, query string) ([]datedOffer, error) {
	q := bleve.NewQueryStringQuery(query)
	rq := bleve.NewSearchRequest(q)
	rq.Size = 250
	rq.Fields = []string{"date"}
	datedOffers := []datedOffer{}
	for {
		if query == "" {
			break
		}
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
		if len(res.Hits) < rq.Size {
			break
		}
		rq.From += rq.Size
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
	if strings.HasPrefix(query, prefix) {
		datedOffers, err = findOffersFromLocation(query[len(prefix):], spatial, geocoder)
	} else {
		datedOffers, err = findOffersFromText(index, query)
	}
	log.Printf("query '%s' returned %d entries", query, len(datedOffers))
	if err != nil {
		return err
	}
	return formatOffers(templ, store, datedOffers, query, w, r)
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
		return err
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

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(templ, store, index, spatial, geocoder, w, r)
	})
	http.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		indexer.Sync()
		spatialIndexer.Sync()
		w.Write([]byte("OK"))
	})
	return http.ListenAndServe(*webHttp, nil)
}
