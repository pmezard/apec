package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/pmezard/apec/jstruct"
	"github.com/pquerna/ffjson/ffjson"
)

var (
	QuotaError = errors.New("payment required")

	geoCacheBucket = []byte("c")
	geoPointBucket = []byte("p")
)

type Location struct {
	City    string
	County  string
	State   string
	Country string
	Lat     float64
	Lon     float64
}

func buildLocation(loc *jstruct.Location) *Location {
	var p *Location
	if loc != nil && len(loc.Results) > 0 && loc.Results[0].Geometry != nil {
		r := loc.Results[0].Component
		g := loc.Results[0].Geometry
		p = &Location{
			City:    r.City,
			County:  r.County,
			State:   r.State,
			Country: r.Country,
			Lat:     g.Lat,
			Lon:     g.Lon,
		}
	}
	return p
}

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
	return c.db.Update(func(tx *Tx) error {
		k := []byte(key)
		err := tx.Put(geoCacheBucket, k, data)
		if err != nil {
			return err
		}
		w := bytes.NewBuffer(nil)
		if pos != nil {
			err = writeBinaryLocation(w, pos)
			if err != nil {
				return err
			}
		} else {
			// BUG: does kv support empty values?
			err = w.WriteByte('\x00')
			if err != nil {
				return err
			}
		}
		return tx.Put(geoPointBucket, k, w.Bytes())
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

func (c *Cache) GetLocation(key string) (*Location, bool, error) {
	var p *Location
	found := false
	err := c.db.View(func(tx *Tx) error {
		data, err := tx.Get(geoPointBucket, []byte(key))
		found = data != nil
		if err != nil || len(data) <= 1 {
			return err
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
	var err error
	err = c.db.View(func(tx *Tx) error {
		keys, err = tx.List(geoCacheBucket)
		return err
	})
	return keys, err
}

func (c *Cache) Version() (int, error) {
	return getKVDBVersion(c.db, geoCacheBucket)
}

func (c *Cache) SetVersion(version int) error {
	return setKVDBVersion(c.db, geoCacheBucket, version)
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
	if version != 1 {
		return nil, fmt.Errorf("please upgrade geocoder cache from %d to %d", version, 1)
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
