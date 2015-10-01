package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

type DataDirs struct {
	RootDir string
}

func NewDataDirs(rootDir string) *DataDirs {
	return &DataDirs{
		RootDir: rootDir,
	}
}

func (d *DataDirs) Store() string {
	return filepath.Join(d.RootDir, "offers")
}

func (d *DataDirs) Index() string {
	return filepath.Join(d.RootDir, "index")
}

func (d *DataDirs) Geocoder() string {
	return filepath.Join(d.RootDir, "geocoder")
}

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

var (
	crawlCmd       = app.Command("crawl", "crawl APEC offers")
	crawlDataDir   = crawlCmd.Flag("data", "data directory").Default("offers").String()
	crawlMinSalary = crawlCmd.Arg("min-salary", "minimum salary in kEUR").Default("50").Int()
	crawlLocations = crawlCmd.Flag("location", "offer location code").Ints()
)

func crawlOffers() error {
	dirs := NewDataDirs(*crawlDataDir)
	store, err := CreateStore(dirs.Store())
	if err != nil {
		return err
	}
	added, deleted := 0, 0
	seen := map[string]bool{}
	err = enumerateOffers(*crawlMinSalary, *crawlLocations, func(ids []string) error {
		for _, id := range ids {
			seen[id] = true
			if store.Has(id) {
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
