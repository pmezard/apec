package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/boltdb/bolt"
)

var (
	QuotaError = errors.New("payment required")

	bucketName = []byte("cache")
)

type Cache struct {
	db *bolt.DB
}

func NewCache(dir string) (*Cache, error) {
	db, err := bolt.Open(dir, 0666, nil)
	if err != nil {
		return nil, err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		_, err = tx.CreateBucket(bucketName)
		if err != nil {
			return nil, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	return &Cache{
		db: db,
	}, nil
}

func (c *Cache) Close() error {
	return c.db.Close()
}

func (c *Cache) Put(key string, data []byte) error {
	tx, err := c.db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	bucket := tx.Bucket(bucketName)
	err = bucket.Put([]byte(key), data)
	if err != nil {
		return err
	}
	err = tx.Commit()
	tx = nil
	return err
}

func (c *Cache) Get(key string) ([]byte, error) {
	tx, err := c.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(bucketName)
	temp := bucket.Get([]byte(key))
	data := make([]byte, len(temp))
	copy(data, temp)
	return data, nil
}

type Geocoder struct {
	key   string
	cache *Cache
}

func NewGeocoder(key, cacheDir string) (*Geocoder, error) {
	cache, err := NewCache(cacheDir)
	if err != nil {
		return nil, err
	}
	return &Geocoder{
		key:   key,
		cache: cache,
	}, nil
}

func (g *Geocoder) Close() error {
	return g.cache.Close()
}

type LocRate struct {
	Limit     int `json:"limit"`
	Remaining int `json:"remaining"`
}

type LocComponent struct {
	City        string `json:"city"`
	PostCode    string `json:"postcode"`
	County      string `json:"county"`
	State       string `json:"state"`
	Country     string `json:"country"`
	CountryCode string `json:"country_code"`
}

func (c *LocComponent) String() string {
	values := []struct {
		Field string
		Value string
	}{
		{"city", c.City},
		{"postcode", c.PostCode},
		{"county", c.County},
		{"state", c.State},
		{"country", c.Country},
	}
	s := ""
	written := false
	for _, v := range values {
		if v.Value == "" {
			continue
		}
		if written {
			s += ", "
		}
		s += fmt.Sprintf("%s: %s", v.Field, v.Value)
		written = true
	}
	return s
}

type LocResult struct {
	Component LocComponent `json:"components"`
}

type Location struct {
	Cached  bool
	Rate    LocRate     `json:"rate"`
	Results []LocResult `json:"results"`
}

func (g *Geocoder) Geocode(q, countryCode string) (*Location, error) {
	countryCode = strings.ToLower(countryCode)
	if countryCode == "" {
		countryCode = "unk"
	}
	key := q + "-" + countryCode
	data, err := g.cache.Get(key)
	if err != nil {
		return nil, err
	}
	res := &Location{}
	if len(data) == 0 {
		r, err := g.rawGeocode(q, countryCode)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		data, err = ioutil.ReadAll(&io.LimitedReader{
			R: r,
			N: 4 * 1024 * 1024,
		})
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(data, res)
		if err != nil {
			return nil, err
		}
		err = g.cache.Put(key, data)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	err = json.Unmarshal(data, res)
	res.Cached = true
	return res, err
}

func (g *Geocoder) rawGeocode(q, countryCode string) (io.ReadCloser, error) {
	u := fmt.Sprintf("http://api.opencagedata.com/geocode/v1/json?q=%s&key=%s",
		url.QueryEscape(q), url.QueryEscape(g.key))
	if countryCode != "unk" {
		u += "&countrycode=" + url.QueryEscape(countryCode)
	}
	rsp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode != 200 {
		rsp.Body.Close()
		if rsp.StatusCode == 402 {
			return nil, QuotaError
		}
		return nil, fmt.Errorf("geocoding failed with %s", rsp.Status)
	}
	return rsp.Body, nil
}

var (
	geocodeCmd     = app.Command("geocode", "geocode location with OpenCage")
	geocodeKey     = geocodeCmd.Arg("key", "OpenCage API key").Required().String()
	geocodeQuery   = geocodeCmd.Arg("query", "geocoding query").Required().String()
	geocodeDataDir = geocodeCmd.Flag("data", "data directory").Default("offers").String()
)

func geocode() error {
	dirs := NewDataDirs(*geocodeDataDir)
	geocoder, err := NewGeocoder(*geocodeKey, dirs.Geocoder())
	if err != nil {
		return err
	}
	defer geocoder.Close()
	loc, err := geocoder.Geocode(*geocodeQuery, "fr")
	if err != nil {
		return err
	}
	if loc.Cached {
		fmt.Printf("cached: true\n")
	}
	fmt.Printf("remaining: %d\n", loc.Rate.Remaining)
	for _, res := range loc.Results {
		comp := res.Component
		fmt.Printf("%s\n", comp.String())
	}
	return nil
}
