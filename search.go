package main

import (
	"encoding/json"
	"fmt"
	"github.com/blevesearch/bleve"
	"path/filepath"
)

type jsonOffer struct {
	Id          string `json:"numeroOffre"`
	Title       string `json:"intitule"`
	Date        string `json:"dataPublication"`
	Salary      string `json:"salaireTexte"`
	PartialTime bool   `json:"tempsPartiel"`
	Location    string `json:"lieuTexte"`
	HTML        string `json:"texteHtml"`
}

func (offer *jsonOffer) Type() string {
	return "offer"
}

func printOffer(store *Store, id string) error {
	data, err := store.Get(id)
	if err != nil {
		return err
	}
	offer := &jsonOffer{}
	err = json.Unmarshal(data, offer)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s\n", offer.Id, offer.Title)
	return nil
}

var (
	searchCmd      = app.Command("search", "search APEC index")
	searchStoreDir = searchCmd.Arg("store", "data store directory").Required().String()
	searchIndexDir = searchCmd.Arg("index", "index directory").Required().String()
	searchQuery    = searchCmd.Arg("query", "search query").Required().String()
)

func search() error {
	store, err := OpenStore(*searchStoreDir)
	if err != nil {
		return err
	}
	index, err := bleve.Open(filepath.Join(*searchIndexDir, "index"))
	if err != nil {
		return err
	}
	fmt.Println(index.Fields())
	defer index.Close()
	q := bleve.NewQueryStringQuery(*searchQuery)
	rq := bleve.NewSearchRequest(q)
	rq.Size = 100
	for {
		res, err := index.Search(rq)
		if err != nil {
			return err
		}
		for _, doc := range res.Hits {
			printOffer(store, doc.ID)
		}
		if len(res.Hits) < rq.Size {
			break
		}
		rq.From += rq.Size
	}
	_ = store
	return nil
}
