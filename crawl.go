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
		return nil, fmt.Errorf("got %s fetching %s", rsp.Status, url)
	}
	return rsp.Body, nil
}

func tryHTTP(url string, baseDelay time.Duration, loops int,
	input io.Reader) (io.ReadCloser, error) {

	delay := baseDelay
	for {
		output, err := doHTTP(url, input)
		if err == nil {
			return output, nil
		}
		fmt.Printf("fetching failed with: %s\n", err)
		loops -= 1
		if loops <= 0 {
			return nil, err
		}
		time.Sleep(delay)
		delay *= 2
	}
}

func doJson(url string, baseDelay time.Duration, loops int, input interface{},
	output interface{}) error {

	var post io.Reader
	if input != nil {
		body := &bytes.Buffer{}
		err := json.NewEncoder(body).Encode(input)
		if err != nil {
			return err
		}
		post = body
	}
	result, err := tryHTTP(url, baseDelay, loops, post)
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

func searchOffers(start, count, minSalary int, locations []int) ([]string, error) {
	filter := &SearchFilters{
		EnableFilter: true,
		Functions:    []int{},
		Places:       locations,
		Experience:   []int{},
		Paging: SearchPaging{
			Range:      count,
			StartIndex: start,
		},
		MinSalary: minSalary,
		MaxSalary: 1000,
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
	err := doJson(url, 5*time.Second, 5, filter, results)
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
	output, err := tryHTTP(u, time.Second, 5, nil)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	return ioutil.ReadAll(output)
}

func enumerateOffers(minSalary int, locations []int, callback func([]string) error) error {
	start := 0
	count := 250
	delay := 5 * time.Second
	for ; ; time.Sleep(delay) {
		fmt.Printf("fetching from %d to %d\n", start, start+count)
		ids, err := searchOffers(start, count, minSalary, locations)
		if err != nil {
			return err
		}
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

func crawlOffers(store *Store, ids []string) (int, error) {
	added := 0
	for _, id := range ids {
		ok, err := store.Has(id)
		if err != nil {
			return added, err
		}
		if ok {
			continue
		}
		fmt.Printf("fetching %s\n", id)
		data, err := getOffer(id)
		if err != nil {
			return added, err
		}
		time.Sleep(time.Second)
		err = store.Put(id, data)
		if err != nil {
			return added, err
		}
		added += 1
	}
	return added, nil
}

func crawl(store *Store, minSalary int, locations []int) error {
	idsChan := make(chan []string)
	stopListing := make(chan bool)
	listingDone := make(chan error)
	crawlingDone := make(chan error)

	// List offers in one goroutine. This reduces the races between our
	// enumeration and possible web site updates.
	seen := map[string]bool{}
	go func() {
		pending := []string{}
		err := enumerateOffers(minSalary, locations, func(ids []string) error {
			for _, id := range ids {
				seen[id] = true
			}
			pending = append(pending, ids...)
			select {
			case <-stopListing:
				return fmt.Errorf("offer enumeration was interrupted")
			case idsChan <- pending:
				pending = nil
			default:
			}
			return nil
		})
		close(idsChan)
		listingDone <- err
	}()

	// Crawl offers in another goroutine
	added := 0
	go func() {
		for ids := range idsChan {
			n, err := crawlOffers(store, ids)
			added += n
			if n < len(ids) {
				fmt.Printf("%d known offers ignored\n", len(ids)-n)
			}
			if err != nil {
				crawlingDone <- err
				break
			}
		}
		close(crawlingDone)
	}()

	// Shutdown everything cleanly
	crawlingErr := <-crawlingDone
	close(stopListing)
	listingErr := <-listingDone
	if listingErr != nil {
		return listingErr
	}
	if crawlingErr != nil {
		return crawlingErr
	}

	// Delete unseen offers
	deleted := 0
	ids, err := store.List()
	if err != nil {
		return err
	}
	now := time.Now()
	for _, id := range ids {
		if !seen[id] {
			fmt.Printf("deleting %s\n", id)
			store.Delete(id, now)
			deleted += 1
		}
	}
	fmt.Printf("%d added, %d deleted, %d total\n", added, deleted, store.Size())
	return nil
}

var (
	crawlCmd       = app.Command("crawl", "crawl APEC offers")
	crawlMinSalary = crawlCmd.Flag("min-salary", "minimum salary in kEUR").Default("0").Int()
	crawlLocations = crawlCmd.Flag("location", "offer location code").Ints()
)

func crawlFn(cfg *Config) error {
	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	var closeErr error
	defer func() {
		closeErr = store.Close()
	}()
	err = crawl(store, *crawlMinSalary, *crawlLocations)
	if err != nil {
		return err
	}
	return closeErr
}
