package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/blevesearch/bleve"
)

type jsonOffer struct {
	Id          string `json:"numeroOffre"`
	Title       string `json:"intitule"`
	Date        string `json:"datePublication"`
	Salary      string `json:"salaireTexte"`
	PartialTime bool   `json:"tempsPartiel"`
	Location    string `json:"lieuTexte"`
	HTML        string `json:"texteHtml"`
	Account     string `json:"nomCompteEtablissement"`
}

func (offer *jsonOffer) Type() string {
	return "offer"
}

type offersByDate []*jsonOffer

func (s offersByDate) Len() int {
	return len(s)
}

func (s offersByDate) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s offersByDate) Less(i, j int) bool {
	return s[i].Date < s[j].Date
}

func formatDate(s string) string {
	parts := strings.SplitN(s, "T", 2)
	return parts[0]
}

func printOffers(store *Store, ids []string) error {
	// Load offer documents
	offers := []*jsonOffer{}
	for _, id := range ids {
		data, err := store.Get(id)
		if err != nil {
			return err
		}
		offer := &jsonOffer{}
		err = json.Unmarshal(data, offer)
		if err != nil {
			return err
		}
		offers = append(offers, offer)
	}
	// Sort by ascending publication date
	sorted := offersByDate(offers)
	sort.Sort(sorted)
	for _, offer := range sorted {
		fmt.Printf("%s %s %s %s (%s)\n", offer.Id, offer.Title, offer.Salary,
			offer.Account, formatDate(offer.Date))
		fmt.Printf("    https://cadres.apec.fr/offres-emploi-cadres/offre.html?numIdOffre=%s\n",
			offer.Id)
	}
	return nil
}

var (
	searchCmd     = app.Command("search", "search APEC index")
	searchQuery   = searchCmd.Arg("query", "search query").Required().String()
	searchDataDir = searchCmd.Flag("data", "data directory").Default("offers").String()
)

func search() error {
	dirs := NewDataDirs(*searchDataDir)
	store, err := OpenStore(dirs.Store())
	if err != nil {
		return err
	}
	index, err := bleve.Open(dirs.Index())
	if err != nil {
		return err
	}
	defer index.Close()
	q := bleve.NewQueryStringQuery(*searchQuery)
	rq := bleve.NewSearchRequest(q)
	rq.Size = 100
	ids := []string{}
	for {
		res, err := index.Search(rq)
		if err != nil {
			return err
		}
		for _, doc := range res.Hits {
			ids = append(ids, doc.ID)
		}
		if len(res.Hits) < rq.Size {
			break
		}
		rq.From += rq.Size
	}
	return printOffers(store, ids)
}
