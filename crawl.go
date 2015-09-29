package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

func doHTTP(url string, input io.Reader) (io.ReadCloser, error) {
	method := "GET"
	if input != nil {
		method = "POST"
	}
	rq, err := http.NewRequest(method, url, input)
	if err != nil {
		return nil, err
	}
	rq.Header.Set("User-Agent", "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)")
	if input != nil {
		rq.Header.Set("Content-Type", "application/json")
	}
	rsp, err := http.DefaultClient.Do(rq)
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode != http.StatusOK {
		rsp.Body.Close()
		return nil, fmt.Errorf("got %d fetching %s", rsp.StatusCode, url)
	}
	return rsp.Body, nil
}

func doJson(url string, input interface{}, output interface{}) error {
	var post io.Reader
	if input != nil {
		body := &bytes.Buffer{}
		err := json.NewEncoder(body).Encode(input)
		if err != nil {
			return err
		}
		post = body
	}
	result, err := doHTTP(url, post)
	if err != nil {
		return err
	}
	defer result.Close()
	return json.NewDecoder(result).Decode(output)
}

type SearchPaging struct {
	Range      int `json:"range"`
	StartIndex int `json:"startIndex"`
}

type SearchSorts struct {
	Direction string `json:"direction"`
	Type      string `json:"type"`
}

type SearchFilters struct {
	EnableFilter    bool          `json:"activeFiltre"`
	Functions       []int         `json:"fonctions"`
	Places          []int         `json:"lieux"`
	Keywords        string        `json:"motsCles"`
	Experience      []int         `json:"niveauxExperience"`
	Paging          SearchPaging  `json:"pagination"`
	MinSalary       int           `json:"salaireMinimum"`
	MaxSalary       int           `json:"salaireMaximum"`
	Sectors         []int         `json:"secteursActivite"`
	Sorts           []SearchSorts `json:"sorts"`
	ClientType      string        `json:"typeClient"`
	ContractTypes   []int         `json:"typesContrat"`
	ConventionTypes []int         `json:"typesConvention"`
}

func searchOffers(start, count int) ([]string, error) {
	filter := &SearchFilters{
		EnableFilter: true,
		Functions:    []int{},
		Places:       []int{ /*705*/ },
		Experience:   []int{},
		Paging: SearchPaging{
			Range:      count,
			StartIndex: start,
		},
		MinSalary: 60,
		MaxSalary: 120,
		Sectors:   []int{},
		Sorts: []SearchSorts{
			{
				Direction: "DESCENDING",
				Type:      "DATE",
			},
		},
		ClientType:      "CADRE",
		ContractTypes:   []int{},
		ConventionTypes: []int{},
	}
	results := &struct {
		Results []struct {
			URI string `json:"@uriOffre"`
		} `json:"resultats"`
	}{}
	url := "https://cadres.apec.fr/cms/webservices/rechercheOffre/ids"
	err := doJson(url, filter, results)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for _, uri := range results.Results {
		parts := strings.Split(uri.URI, "numeroOffre=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid offer identifier: %s", uri.URI)
		}
		ids = append(ids, parts[1])
	}
	return ids, nil
}

func getOffer(id string) ([]byte, error) {
	u := "https://cadres.apec.fr/cms/webservices/offre/public?numeroOffre=" + id
	output, err := doHTTP(u, nil)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	return ioutil.ReadAll(output)
}

var (
	crawlCmd      = app.Command("crawl", "crawl APEC offers")
	crawlStoreDir = crawlCmd.Arg("store", "data store directory").Required().String()
)

func enumerateOffers(callback func([]string) error) error {
	start := 0
	count := 250
	baseDelay := 5 * time.Second
	maxDelay := 5 * time.Minute
	delay := baseDelay
	for ; ; time.Sleep(delay) {
		time.Sleep(delay)
		fmt.Printf("fetching from %d to %d\n", start, start+count)
		ids, err := searchOffers(start, count)
		if err != nil {
			fmt.Printf("fetching failed with: %s\n", err)
			delay *= 2
			if delay > maxDelay {
				return err
			}
			continue
		}
		delay = baseDelay
		start += count
		err = callback(ids)
		if err != nil {
			return err
		}
		if len(ids) < count {
			break
		}
	}
	return nil
}

func crawlOffers() error {
	store, err := CreateStore(*crawlStoreDir)
	if err != nil {
		return err
	}
	added, deleted := 0, 0
	seen := map[string]bool{}
	err = enumerateOffers(func(ids []string) error {
		for _, id := range ids {
			seen[id] = true
			if store.Has(id) {
				fmt.Printf("skipping %s\n", id)
				continue
			}
			fmt.Printf("fetching %s\n", id)
			data, err := getOffer(id)
			if err != nil {
				return err
			}
			time.Sleep(time.Second)
			written, err := store.Write(id, data)
			if err != nil {
				return err
			}
			if !written {
				fmt.Printf("racing %s\n", id)
				continue
			}
			added += 1
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range store.List() {
		if !seen[id] {
			fmt.Printf("deleting %s\n", id)
			store.Delete(id)
			deleted += 1
		}
	}
	fmt.Printf("%d added, %d deleted, %d total\n", added, deleted, store.Size())
	return nil
}
