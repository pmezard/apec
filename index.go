package main

import (
	"encoding/json"
	"fmt"
	"github.com/blevesearch/bleve"
	"os"
	"path/filepath"
	"sync"
	"time"
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

func NewOfferIndex(dir string) (bleve.Index, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	html := bleve.NewTextFieldMapping()
	html.Store = false
	html.IncludeInAll = false
	html.IncludeTermVectors = false

	title := bleve.NewDocumentStaticMapping()
	title.AddFieldMappingsAt("text", html)

	offer := bleve.NewDocumentStaticMapping()
	offer.AddFieldMappingsAt("texteHtml", html)
	offer.AddFieldMappingsAt("intitule", html)
	offer.AddSubDocumentMapping("Title", title)
	offer.Dynamic = false

	m := bleve.NewIndexMapping()
	m.AddDocumentMapping("offer", offer)
	m.DefaultMapping = offer

	index, err := bleve.New(filepath.Join(dir, "index"), m)
	if err != nil {
		return nil, err
	}
	return index, nil
}

type OfferTitle struct {
	Text string `json:"text"`
}

type Offer struct {
	HTML  string `json:"texteHtml"`
	Title OfferTitle
}

var (
	indexCmd      = app.Command("index", "index APEC offers")
	indexStoreDir = indexCmd.Arg("store", "data store directory").Required().String()
	indexIndexDir = indexCmd.Arg("index", "index directory").Required().String()
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
	offers, err := loadOffers(store)
	if err != nil {
		return err
	}
	if len(offers) > 20 {
		offers = offers[:20]
	}
	start := time.Now()
	for i, offer := range offers {
		if (i+1)%500 == 0 {
			now := time.Now()
			elapsed := float64(now.Sub(start)) / float64(time.Second)
			fmt.Printf("%d indexed, %.1f/s\n", i+1, float64(i+1)/elapsed)
		}
		err = index.Index(offer.Id, &Offer{
			HTML: offer.HTML,
			Title: OfferTitle{
				Text: offer.Title,
			},
		})
		//err = index.Index(offer.Id, offer)
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
