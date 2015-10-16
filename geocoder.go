package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
)

var (
	QuotaError = errors.New("payment required")

	geoCacheBucket = []byte("c")
)

type Cache struct {
	db *KVDB
}

func OpenCache(dir string) (*Cache, error) {
	db, err := OpenKVDB(filepath.Join(dir, "kv"), 0)
	if err != nil {
		return nil, err
	}
	c := &Cache{
		db: db,
	}
	return c, nil
}

func (c *Cache) Close() error {
	return c.db.Close()
}

func (c *Cache) Put(key string, data []byte) error {
	return c.db.Update(func(tx *Tx) error {
		return tx.Put(geoCacheBucket, []byte(key), data)
	})
}

func (c *Cache) Get(key string) ([]byte, error) {
	var err error
	var data []byte
	err = c.db.View(func(tx *Tx) error {
		data, err = tx.Get(geoCacheBucket, []byte(key))
		return err
	})
	return data, err
}

type Geocoder struct {
	key   string
	cache *Cache
}

func NewGeocoder(key, cacheDir string) (*Geocoder, error) {
	cache, err := OpenCache(cacheDir)
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

type LocGeom struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lng"`
}

type LocResult struct {
	Component LocComponent `json:"components"`
	Geometry  *LocGeom     `json:"geometry"`
}

type Location struct {
	Cached  bool
	Rate    LocRate     `json:"rate"`
	Results []LocResult `json:"results"`
}

func makeKeyAndCountryCode(q, code string) (string, string) {
	code = strings.ToLower(code)
	if code == "" {
		code = "unk"
	}
	return q + "-" + code, code
}

func (g *Geocoder) GeocodeFromCache(q, countryCode string) (*Location, error) {
	key, countryCode := makeKeyAndCountryCode(q, countryCode)
	data, err := g.cache.Get(key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	res := &Location{}
	err = json.Unmarshal(data, res)
	res.Cached = true
	return res, err
}

func (g *Geocoder) Geocode(q, countryCode string, offline bool) (*Location, error) {
	res, err := g.GeocodeFromCache(q, countryCode)
	if err != nil || res != nil || offline {
		return res, err
	}
	r, err := g.rawGeocode(q, countryCode)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	data, err := ioutil.ReadAll(&io.LimitedReader{
		R: r,
		N: 4 * 1024 * 1024,
	})
	if err != nil {
		return nil, err
	}
	res = &Location{}
	err = json.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	key, _ := makeKeyAndCountryCode(q, countryCode)
	err = g.cache.Put(key, data)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, res)
	return res, err
}

func (g *Geocoder) rawGeocode(q, countryCode string) (io.ReadCloser, error) {
	u := fmt.Sprintf("http://api.opencagedata.com/geocode/v1/json?q=%s&key=%s",
		url.QueryEscape(q), url.QueryEscape(g.key))
	if countryCode != "" {
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
	geocodeCmd   = app.Command("geocode", "geocode location with OpenCage")
	geocodeQuery = geocodeCmd.Arg("query", "geocoding query").Required().String()
)

func geocode(cfg *Config) error {
	key := cfg.GeocodingKey()
	if key == "" {
		return fmt.Errorf("geocoding key is not set, please configure APEC_GEOCODING_KEY")
	}
	geocoder, err := NewGeocoder(key, cfg.Geocoder())
	if err != nil {
		return err
	}
	defer geocoder.Close()
	loc, err := geocoder.Geocode(*geocodeQuery, "fr", false)
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
