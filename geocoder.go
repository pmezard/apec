package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
)

var (
	QuotaError = errors.New("payment required")

	geoCacheBucket = []byte("c")
	geoPointBucket = []byte("p")
	geoMetaBucket  = []byte("m")

	geoBuckets = [][]byte{
		geoCacheBucket,
		geoPointBucket,
		geoMetaBucket,
	}

	geocoderVersion = 2
)

type Location struct {
	City     string
	County   string
	State    string
	Country  string
	PostCode string
	Lat      float64
	Lon      float64
}

func (l *Location) String() string {
	values := []struct {
		Field string
		Value string
	}{
		{"city", l.City},
		{"postcode", l.PostCode},
		{"county", l.County},
		{"state", l.State},
		{"country", l.Country},
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

func buildLocation(loc *jstruct.Location) *Location {
	var p *Location
	if loc != nil && len(loc.Results) > 0 && loc.Results[0].Geometry != nil {
		r := loc.Results[0].Component
		g := loc.Results[0].Geometry
		p = &Location{
			City:     r.City,
			PostCode: r.PostCode,
			County:   r.County,
			State:    r.State,
			Country:  r.Country,
			Lat:      g.Lat,
			Lon:      g.Lon,
		}
	}
	return p
}

type Cache struct {
	db *bolt.DB
}

func OpenCache(path string) (*Cache, error) {
	exists, err := isFile(path)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range geoBuckets {
			_, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	c := &Cache{
		db: db,
	}
	if !exists {
		err = c.SetVersion(geocoderVersion)
		if err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Cache) Close() error {
	return c.db.Close()
}

func writeBinaryString(w io.Writer, buf []byte, s string) error {
	binary.PutVarint(buf, int64(len(s)))
	_, err := w.Write(buf[:binary.MaxVarintLen32])
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(s))
	return err
}

func readBinaryString(r io.Reader, buf []byte) (string, error) {
	_, err := io.ReadFull(r, buf[:binary.MaxVarintLen32])
	if err != nil {
		return "", err
	}
	l64, n := binary.Varint(buf[:binary.MaxVarintLen32])
	if n <= 0 {
		return "", fmt.Errorf("could not decode string length: %d", n)
	}
	l := int(l64)
	if len(buf) < l {
		buf = make([]byte, l)
	}
	_, err = io.ReadFull(r, buf[:l])
	if err != nil {
		return "", err
	}
	return string(buf[:l]), nil
}

func writeBinaryLocation(w io.Writer, pos *Location) (err error) {
	checkErr := func(e error) {
		if err == nil && e != nil {
			err = e
		}
	}
	buf := make([]byte, binary.MaxVarintLen32)
	checkErr(writeBinaryString(w, buf, pos.City))
	checkErr(writeBinaryString(w, buf, pos.County))
	checkErr(writeBinaryString(w, buf, pos.State))
	checkErr(writeBinaryString(w, buf, pos.Country))
	e := binary.Write(w, binary.LittleEndian,
		&struct{ Lat, Lon float64 }{Lat: pos.Lat, Lon: pos.Lon})
	if err == nil {
		err = e
	}
	return
}

func readBinaryLocation(r io.Reader) (loc *Location, err error) {
	checkErr := func(s string, e error) string {
		if err == nil && e != nil {
			err = e
		}
		return s
	}
	buf := make([]byte, binary.MaxVarintLen32)
	loc = &Location{}
	loc.City = checkErr(readBinaryString(r, buf))
	loc.County = checkErr(readBinaryString(r, buf))
	loc.State = checkErr(readBinaryString(r, buf))
	loc.Country = checkErr(readBinaryString(r, buf))
	pos := &struct{ Lat, Lon float64 }{}
	e := binary.Read(r, binary.LittleEndian, pos)
	if err == nil {
		err = e
	}
	loc.Lat = pos.Lat
	loc.Lon = pos.Lon
	return
}

func (c *Cache) Put(key string, data []byte, pos *Location) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		k := []byte(key)
		err := tx.Bucket(geoCacheBucket).Put(k, data)
		if err != nil {
			return err
		}
		w := bytes.NewBuffer(nil)
		if pos != nil {
			err = writeBinaryLocation(w, pos)
			if err != nil {
				return err
			}
		}
		return tx.Bucket(geoPointBucket).Put(k, w.Bytes())
	})
}

func (c *Cache) Get(key string) ([]byte, error) {
	var data []byte
	err := c.db.View(func(tx *bolt.Tx) error {
		data = tx.Bucket(geoCacheBucket).Get([]byte(key))
		return nil
	})
	return data, err
}

func (c *Cache) GetLocation(key string) (*Location, bool, error) {
	var p *Location
	found := false
	err := c.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(geoPointBucket).Get([]byte(key))
		found = data != nil
		if len(data) == 0 {
			return nil
		}
		point, err := readBinaryLocation(bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		p = point
		return nil
	})
	return p, found, err
}

func (c *Cache) List() ([]string, error) {
	keys := []string{}
	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(geoCacheBucket)
		size := bucket.Stats().KeyN
		keys = make([]string, 0, size)
		return bucket.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))
			return nil
		})
	})
	return keys, err
}

func (c *Cache) Version() (int, error) {
	version := 0
	err := c.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(geoMetaBucket).Get([]byte("version"))
		if data != nil {
			v, n := binary.Varint(data)
			if n > 0 {
				version = int(v)
			}
		}
		return nil
	})
	return version, err
}

func (c *Cache) SetVersion(version int) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		buf := make([]byte, 10)
		n := binary.PutVarint(buf, int64(version))
		return tx.Bucket(geoMetaBucket).Put([]byte("version"), buf[:n])
	})
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
	defer func() {
		if cache != nil {
			cache.Close()
		}
	}()
	version, err := cache.Version()
	if err != nil {
		return nil, err
	}
	if version != geocoderVersion {
		return nil, fmt.Errorf("please upgrade geocoder cache from %d to %d",
			version, geocoderVersion)
	}
	g := &Geocoder{
		key:   key,
		cache: cache,
	}
	cache = nil
	return g, nil
}

func NewOldGeocoder(key, cacheDir string) (*Geocoder, error) {
	cache, err := OpenCache(cacheDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cache != nil {
			cache.Close()
		}
	}()
	version, err := cache.Version()
	if err != nil {
		return nil, err
	}
	if version != geocoderVersion {
		return nil, fmt.Errorf("please upgrade geocoder cache from %d to %d",
			version, geocoderVersion)
	}
	g := &Geocoder{
		key:   key,
		cache: cache,
	}
	cache = nil
	return g, nil
}

func (g *Geocoder) Close() error {
	return g.cache.Close()
}

func makeKeyAndCountryCode(q, code string) (string, string) {
	code = strings.ToLower(code)
	if code == "" {
		code = "unk"
	}
	return q + "-" + code, code
}

func (g *Geocoder) geocodeFromCache(q, countryCode string) (*jstruct.Location, error) {
	key, countryCode := makeKeyAndCountryCode(q, countryCode)
	data, err := g.cache.Get(key)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	res := &jstruct.Location{}
	err = ffjson.Unmarshal(data, res)
	res.Cached = true
	return res, err
}

func (g *Geocoder) GetCachedLocation(q, countryCode string) (*Location, bool, error) {
	key, _ := makeKeyAndCountryCode(q, countryCode)
	return g.cache.GetLocation(key)
}

func (g *Geocoder) Geocode(q, countryCode string, offline bool) (
	*jstruct.Location, error) {

	res, err := g.geocodeFromCache(q, countryCode)
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
	res = &jstruct.Location{}
	err = ffjson.Unmarshal(data, res)
	if err != nil {
		return nil, err
	}
	key, _ := makeKeyAndCountryCode(q, countryCode)
	err = g.cache.Put(key, data, buildLocation(res))
	if err != nil {
		return nil, err
	}
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

func shuffle(values []string) {
	if len(values) < 2 {
		return
	}
	for i := len(values) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		values[i], values[j] = values[j], values[i]
	}
}

var (
	geocodeCmd = app.Command("geocode", "geocode offers without location")
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

	store, err := OpenStore(cfg.Store())
	if err != nil {
		return err
	}
	defer store.Close()

	ids, err := store.List()
	if err != nil {
		return err
	}
	shuffle(ids)
	for _, id := range ids {
		loc, _, err := store.GetLocation(id)
		if err != nil {
			return err
		}
		if loc != nil {
			continue
		}
		offer, err := getStoreOffer(store, id)
		if err != nil {
			return err
		}
		if offer == nil {
			continue
		}
		pos, _, off, err := geocodeOffer(geocoder, offer.Location, false, 100)
		if err != nil {
			return err
		}
		if pos == nil {
			continue
		}
		if off {
			break
		}
		err = store.PutLocation(id, pos, offer.Date)
		if err != nil {
			return err
		}
	}
	err = store.Close()
	if err != nil {
		return err
	}
	return geocoder.Close()
}
