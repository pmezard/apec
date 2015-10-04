package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"

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

func serveQuery(templ *template.Template, store *Store, index bleve.Index,
	w http.ResponseWriter, r *http.Request) error {

	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return err
	}
	query := values.Get("q")
	q := bleve.NewQueryStringQuery(query)
	rq := bleve.NewSearchRequest(q)
	rq.Size = 250
	rq.Fields = []string{"date"}
	offers := []*offerData{}
	maxDisplayed := 1000
	sortedOffers := []datedOffer{}
	total := 0
	for {
		if query == "" {
			break
		}
		res, err := index.Search(rq)
		if err != nil {
			return err
		}
		total = int(res.Total)
		for _, doc := range res.Hits {
			date, ok := doc.Fields["date"].(string)
			if !ok {
				return fmt.Errorf("could not retrieve date for %s", doc.ID)
			}
			sortedOffers = append(sortedOffers, datedOffer{
				Date: date,
				Id:   doc.ID,
			})
		}
		if len(res.Hits) < rq.Size {
			break
		}
		rq.From += rq.Size
	}
	sort.Sort(sortedDatedOffers(sortedOffers))

	for _, doc := range sortedOffers {
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
		Total:     total,
		Query:     query,
	}
	h := w.Header()
	h.Set("Content-Type", "text/html")
	templ.Execute(w, &data)
	return nil
}

func handleQuery(templ *template.Template, store *Store, index bleve.Index,
	w http.ResponseWriter, r *http.Request) {
	err := serveQuery(templ, store, index, w, r)
	if err != nil {
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
	index, err := bleve.Open(cfg.Index())
	if err != nil {
		return fmt.Errorf("cannot open index: %s", err)
	}
	defer index.Close()
	templ, err := template.ParseGlob("web/*.tmpl")
	if err != nil {
		return err
	}
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleQuery(templ, store, index, w, r)
	})
	return http.ListenAndServe(*webHttp, nil)
}
