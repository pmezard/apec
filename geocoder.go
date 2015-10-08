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

	geoCacheBucket = []byte("cache")
	geoMetaBucket  = []byte("meta")
)

type Cache struct {
	db *bolt.DB
}

func NewCache(dir string, logFn func(s string)) (*Cache, error) {
	db, err := bolt.Open(dir, 0666, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(geoCacheBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	c := &Cache{
		db: db,
	}
	return c, c.upgrade(logFn)
}

func (c *Cache) Close() error {
	return c.db.Close()
}

func (c *Cache) putJson(tx *bolt.Tx, bucketName []byte, key string,
	value interface{}) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return fmt.Errorf("no bucket %s", string(bucketName))
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(key), data)
}

func (c *Cache) getJson(tx *bolt.Tx, bucketName []byte, key string,
	value interface{}) error {
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return fmt.Errorf("no bucket %s", string(bucketName))
	}
	data := bucket.Get([]byte(key))
	return json.Unmarshal(data, value)
}

func (c *Cache) Put(key string, data []byte) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(geoCacheBucket)
		return bucket.Put([]byte(key), data)
	})
}

func (c *Cache) Get(key string) ([]byte, error) {
	var data []byte
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(geoCacheBucket)
		temp := bucket.Get([]byte(key))
		data = make([]byte, len(temp))
		copy(data, temp)
		return nil
	})
	return data, err
}

type geoVersion struct {
	Version int `json:"version"`
}

func lowerCaseKeys(tx *bolt.Tx) error {
	keys := []string{}
	b := tx.Bucket(geoCacheBucket)
	if b == nil {
		return fmt.Errorf("cache bucket does not exist")
	}
	err := b.ForEach(func(k, v []byte) error {
		key := string(k)
		if strings.ToLower(key) != key {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	for _, key := range keys {
		lkey := strings.ToLower(key)
		k := []byte(key)
		data := b.Get(k)
		err = b.Put([]byte(lkey), data)
		if err != nil {
			return err
		}
		err = b.Delete(k)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) upgrade(logFn func(s string)) error {
	log := func(format string, args ...interface{}) {
		if logFn != nil {
			logFn(fmt.Sprintf(format, args...))
		}
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		meta := geoVersion{}
		bucket := tx.Bucket(geoMetaBucket)
		if bucket != nil {
			err := c.getJson(tx, geoMetaBucket, "version", &meta)
			if err != nil {
				return err
			}
		} else {
			log("creating meta bucket\n")
			_, err := tx.CreateBucket(geoMetaBucket)
			if err != nil {
				return err
			}
		}
		if meta.Version == 0 {
			log("converting keys to lower case\n")
			err := lowerCaseKeys(tx)
			if err != nil {
				return err
			}
			meta.Version = 1
		}
		return c.putJson(tx, geoMetaBucket, "version", &meta)
	})
}

type Geocoder struct {
	key   string
	cache *Cache
}

func NewGeocoder(key, cacheDir string) (*Geocoder, error) {
	cache, err := NewCache(cacheDir, nil)
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
