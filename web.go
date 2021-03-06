package main

import (
	"fmt"
	"html/template"
	"image/png"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"
	"github.com/jonas-p/go-shp"
	"github.com/pmezard/apec/blevext"
	"github.com/pmezard/apec/shpdraw"
)

type Templates struct {
	Search  *template.Template
	Density *template.Template
}

func loadTemplates() (*Templates, error) {
	var err error
	t := &Templates{}
	t.Search, err = template.ParseFiles("web/search.tmpl")
	if err != nil {
		return nil, err
	}
	t.Density, err = template.ParseFiles("web/density.tmpl")
	if err != nil {
		return nil, err
	}
	return t, nil
}

type offerData struct {
	Account  string
	Title    string
	Date     string
	Salary   string
	URL      string
	Location string
	Age      string
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

func formatOffers(templ *Templates, store *Store, datedOffers []datedOffer,
	where, what string, spatialDuration, textDuration time.Duration, w http.ResponseWriter,
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
		age := "    "
		initialDate, err := store.GetInitialDate(doc.Id)
		if err != nil {
			return err
		}
		if !initialDate.IsZero() {
			age = fmt.Sprintf("%3dj", start.Sub(initialDate)/(24*time.Hour))
		}
		offers = append(offers, &offerData{
			Account:  offer.Account,
			Title:    offer.Title,
			Date:     offer.Date.Format("2006-01-02"),
			URL:      offer.URL,
			Salary:   salary,
			Location: offer.Location,
			Age:      age,
		})
	}
	end := time.Now()
	data := struct {
		Offers            []*offerData
		Displayed         int
		Total             int
		Where             string
		What              string
		SpatialDuration   string
		TextDuration      string
		RenderingDuration string
	}{
		Offers:            offers,
		Displayed:         len(offers),
		Total:             len(datedOffers),
		Where:             where,
		What:              what,
		SpatialDuration:   ftime(spatialDuration),
		TextDuration:      ftime(textDuration),
		RenderingDuration: ftime(end.Sub(start)),
	}
	h := w.Header()
	h.Set("Content-Type", "text/html")
	templ.Search.Execute(w, &data)
	return nil
}

func makeSearchQuery(queryString string, ids []string) (query.Query, error) {
	nodes, err := blevext.Parse(queryString)
	if err != nil {
		return nil, err
	}

	addIdsFilter := func(q query.Query) query.Query {
		if len(ids) == 0 {
			return q
		}
		return bleve.NewConjunctionQuery(
			query.NewDocIDQuery(ids),
			q,
		)
	}

	var makeQuery func(*blevext.Node) (query.Query, error)
	makeQuery = func(n *blevext.Node) (query.Query, error) {
		if n == nil {
			return bleve.NewMatchAllQuery(), nil
		}
		switch n.Kind {
		case blevext.NodeAnd, blevext.NodeOr:
			left, err := makeQuery(n.Children[0])
			if err != nil {
				return nil, err
			}
			right, err := makeQuery(n.Children[1])
			if err != nil {
				return nil, err
			}
			if n.Kind == blevext.NodeOr {
				q := query.NewDisjunctionQuery([]query.Query{left, right})
				q.Min = 1
				return q, nil
			}
			return query.NewConjunctionQuery([]query.Query{left, right}), nil
		case blevext.NodeString, blevext.NodePhrase:
			fn := func(s string) query.FieldableQuery {
				return bleve.NewMatchQuery(s)
			}
			if n.Kind == blevext.NodePhrase {
				fn = func(s string) query.FieldableQuery {
					return blevext.NewAllMatchQuery(s)
				}
			}
			htmlQuery := fn(n.Value)
			htmlQuery.SetField("html")
			titleQuery := fn(n.Value)
			titleQuery.SetField("title")
			q := query.NewDisjunctionQuery([]query.Query{
				addIdsFilter(htmlQuery),
				addIdsFilter(titleQuery),
			})
			q.Min = 1
			return q, nil
		}
		return nil, fmt.Errorf("unknown query node type: %d", n.Kind)
	}
	return makeQuery(nodes)
}

func findOffersFromText(index bleve.Index, query string, ids []string) (
	[]datedOffer, error) {

	if query == "" {
		return nil, nil
	}
	datedOffers := []datedOffer{}
	q, err := makeSearchQuery(query, ids)
	if err != nil {
		return nil, err
	}
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

	if query == "" {
		return spatial.FindAll(), nil
	}
	lat, lon, radius := float64(0), float64(0), float64(0)
	if strings.HasPrefix(query, "wgs84:") {
		parts := strings.Split(query[len("wgs84:"):], ",")
		if len(parts) < 2 || len(parts) > 3 {
			return nil, fmt.Errorf("invalid coordinates: %s", query)
		}
		floats := []float64{}
		for _, p := range parts {
			f, err := strconv.ParseFloat(p, 64)
			if err != nil {
				return nil, err
			}
			floats = append(floats, f)
		}
		if len(floats) == 2 {
			floats = append(floats, float64(30000))
		}
		lat = floats[0]
		lon = floats[1]
		radius = floats[2]
	} else {
		parts := strings.Split(query, ",")
		if len(parts) != 1 && len(parts) != 2 {
			return nil, fmt.Errorf("invalid location string: %s", query)
		}
		loc, ok, err := geocoder.GetCachedLocation(strings.ToLower(parts[0]), "fr")
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("could not geocode %s", query)
		}
		lat = loc.Lat
		lon = loc.Lon
		radius = float64(30000)
		if len(parts) == 2 {
			radius, err = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			if err != nil {
				return nil, err
			}
		}
	}
	datedOffers, err := spatial.FindNearest(lat, lon, radius)
	return datedOffers, err
}

func serveQuery(templ *Templates, store *Store, index bleve.Index,
	spatial *SpatialIndex, geocoder *Geocoder, w http.ResponseWriter, r *http.Request) error {

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	what := strings.TrimSpace(values.Get("what"))
	where := strings.TrimSpace(values.Get("where"))

	whereStart := time.Now()
	offers, err := findOffersFromLocation(where, spatial, geocoder)
	if err != nil {
		return err
	}
	spatialCount := len(offers)
	whatStart := time.Now()
	textCount := 0
	if len(what) > 0 && len(offers) > 0 {
		ids := make([]string, len(offers))
		for i, offer := range offers {
			ids[i] = offer.Id
		}
		sort.Strings(ids)
		offers, err = findOffersFromText(index, what, ids)
		if err != nil {
			return err
		}
		textCount = len(offers)
	}
	formatStart := time.Now()
	spatialDuration := whatStart.Sub(whereStart)
	textDuration := formatStart.Sub(whatStart)
	err = formatOffers(templ, store, offers, where, what, spatialDuration,
		textDuration, w, r)
	end := time.Now()
	formatDuration := end.Sub(formatStart)
	log.Printf("spatial '%s': %d in %s, text: '%s': %d in %s, format: %d in %s\n",
		where, spatialCount, ftime(spatialDuration),
		what, textCount, ftime(textDuration),
		len(offers), ftime(formatDuration))
	return err
}

func handleQuery(templ *Templates, store *Store, index bleve.Index,
	spatial *SpatialIndex, geocoder *Geocoder, w http.ResponseWriter, r *http.Request) {
	err := serveQuery(templ, store, index, spatial, geocoder, w, r)
	if err != nil {
		log.Printf("error: query failed with: %s", err)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(400)
		fmt.Fprintf(w, "error: %s\n", err)
	}
}

func handleDensity(templ *Templates, store *Store, index bleve.Index,
	box shp.Box, w http.ResponseWriter, r *http.Request) error {

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	what := strings.TrimSpace(values.Get("what"))
	size := strings.TrimSpace(values.Get("size"))
	if size == "" {
		size = "500"
	}
	sz, err := strconv.ParseFloat(size, 64)
	if err != nil {
		return err
	}
	u := "densitymap?" + r.URL.RawQuery
	data := struct {
		URL    string
		What   string
		Size   string
		X0, Y0 float64
		DX, DY float64
	}{
		URL:  u,
		What: what,
		Size: size,
		X0:   box.MinX,
		Y0:   box.MaxY,
		DX:   (box.MaxX - box.MinX) / sz,
		DY:   -(box.MaxY - box.MinY) / sz,
	}
	h := w.Header()
	h.Set("Content-Type", "text/html")
	return templ.Density.Execute(w, &data)
}

func ftime(d time.Duration) string {
	return fmt.Sprintf("%.3fs", float64(d)/float64(time.Second))
}

func handleDensityMap(templ *Templates, store *Store, index bleve.Index,
	spatial *SpatialIndex, box shp.Box, shapes []shp.Shape,
	w http.ResponseWriter, r *http.Request) error {

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	what := strings.TrimSpace(values.Get("what"))
	gridSize := 500
	size := strings.TrimSpace(values.Get("size"))
	if size != "" {
		n, err := strconv.ParseInt(size, 10, 32)
		if err == nil {
			gridSize = int(n)
		}
	}
	start := time.Now()
	points, err := listPoints(store, index, spatial, what)
	if err != nil {
		return err
	}
	listTime := time.Now()
	grid := makeMapGrid(points, box, gridSize, gridSize)
	grid = convolveGrid(grid)
	gridTime := time.Now()
	img := drawGrid(grid)
	drawTime := time.Now()
	err = drawShapes(box, shapes, img)
	if err != nil {
		return err
	}
	shapesTime := time.Now()
	h := w.Header()
	h.Set("Content-Type", "image/png")
	err = png.Encode(w, img)
	end := time.Now()
	log.Printf("densitymap: size: %d, '%s': %d points, total: %s, list: %s, grid: %s, "+
		"draw: %s, shapes: %s, encode: %s", gridSize, what, len(points),
		ftime(end.Sub(start)),
		ftime(listTime.Sub(start)),
		ftime(gridTime.Sub(listTime)),
		ftime(drawTime.Sub(gridTime)),
		ftime(shapesTime.Sub(drawTime)),
		ftime(end.Sub(shapesTime)))
	return err
}

func enforcePost(rq *http.Request, w http.ResponseWriter) bool {
	if rq.Method != "POST" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("method not allowed\n"))
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
	w.Write([]byte("OK\n"))
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
	log.Printf("geocoding %d offers", len(ids))
	shuffle(ids)
	for _, id := range ids {
		loc, _, err := h.store.GetLocation(id)
		if err != nil {
			return err
		}
		if loc != nil {
			continue
		}
		offer, err := getStoreOffer(h.store, id)
		if err != nil {
			return err
		}
		if offer == nil {
			continue
		}
		pos, _, off, err := geocodeOffer(h.geocoder, offer.Location, false, 0)
		if err != nil {
			return err
		}
		if pos == nil {
			continue
		}
		if off {
			break
		}
		err = h.store.PutLocation(id, pos, offer.Date)
		if err != nil {
			return err
		}
		offerLoc, err := makeOfferLocation(offer.Id, offer.Date, pos)
		if err != nil {
			log.Printf("error: cannot make offer location for %s: %s", id, err)
		} else if offerLoc != nil {
			h.spatial.Remove(offer.Id)
			h.spatial.Add(offerLoc)
		}
	}
	return nil
}

func handleChanges(store *Store, w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	err := printChanges(w, store, true)
	if err != nil {
		log.Printf("error: %s", err)
	}
}

var (
	webCmd        = app.Command("web", "APEC web frontend")
	webHttp       = webCmd.Flag("http", "http server address").Default(":8081").String()
	webPublicPath = webCmd.Flag("public-path", "base URL path for public content").
			String()
	webAdminPath = webCmd.Flag("admin-path", "base URL path for admin content").
			String()
)

func web(cfg *Config) error {
	publicURL := *webPublicPath
	adminURL := *webAdminPath

	home, err := ioutil.ReadFile("web/home.html")
	if err != nil {
		return err
	}
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
	templ, err := loadTemplates()
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
	indexer := NewIndexer(store, index, queue)
	defer indexer.Close()
	indexer.Sync()

	spatialIndexer := NewSpatialIndexer(store, spatial, geocoder)
	defer spatialIndexer.Close()
	spatialIndexer.Sync()

	geocodingHandler := NewGeocodingHandler(store, geocoder, spatial)

	box := makeFranceBox()
	shapes, err := shpdraw.LoadAndFilterShapes("shp/TM_WORLD_BORDERS-0.3.shp", box)
	if err != nil {
		return err
	}

	// Public handlers
	http.HandleFunc(publicURL+"/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(home)
	})
	jsPrefix := publicURL + "/js/"
	http.Handle(jsPrefix, http.StripPrefix(jsPrefix, http.FileServer(http.Dir("web/js"))))
	http.HandleFunc(publicURL+"/search", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(templ, store, index, spatial, geocoder, w, r)
	})
	http.HandleFunc(publicURL+"/density", func(w http.ResponseWriter, r *http.Request) {
		err := handleDensity(templ, store, index, box, w, r)
		if err != nil {
			log.Printf("error: density failed with: %s", err)
		}
	})
	http.HandleFunc(publicURL+"/densitymap", func(w http.ResponseWriter, r *http.Request) {
		err := handleDensityMap(templ, store, index, spatial, box, shapes, w, r)
		if err != nil {
			log.Printf("error: density failed with: %s", err)
		}
	})
	// Admin handlers
	http.HandleFunc(adminURL+"/changes", func(w http.ResponseWriter, r *http.Request) {
		handleChanges(store, w, r)
	})
	http.HandleFunc(adminURL+"/sync", func(w http.ResponseWriter, r *http.Request) {
		if enforcePost(r, w) {
			return
		}
		indexer.Sync()
		spatialIndexer.Sync()
		w.Write([]byte("OK"))
	})

	crawlingLock := sync.Mutex{}
	crawling := false
	http.HandleFunc(adminURL+"/crawl", func(w http.ResponseWriter, r *http.Request) {
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
	http.Handle(adminURL+"/geocode", geocodingHandler)

	http.HandleFunc(adminURL+"/panic", func(w http.ResponseWriter, r *http.Request) {
		// Evade HTTP handler recover
		go func() {
			panic("now")
		}()
	})

	return http.ListenAndServe(*webHttp, nil)
}
