package main

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/patrick-higgins/rtreego"
)

type OfferLoc struct {
	Id   string
	Date time.Time
	Loc  rtreego.Rect
}

func (l *OfferLoc) Bounds() *rtreego.Rect {
	return &l.Loc
}

var (
	locExtent = [2]float64{1e-6, 1e-6}
)

func getOfferLocation(store *Store, geocoder *Geocoder, id string) (*OfferLoc, error) {
	data, err := store.Get(id)
	if err != nil {
		return nil, err
	}
	js := &jsonOffer{}
	err = json.Unmarshal(data, js)
	if err != nil {
		return nil, err
	}
	offer, err := convertOffer(js)
	if err != nil {
		return nil, err
	}
	_, loc, err := geocodeOffer(geocoder, offer, true)
	if err != nil {
		return nil, err
	}
	if loc == nil || len(loc.Results) == 0 || loc.Results[0].Geometry == nil {
		return nil, nil
	}
	g := loc.Results[0].Geometry
	lon := g.Lon - locExtent[0]/2
	lat := g.Lat - locExtent[1]/2
	rect, err := rtreego.NewRect(rtreego.Point{lon, lat}, locExtent)
	if err != nil {
		return nil, err
	}
	return &OfferLoc{
		Id:   id,
		Date: offer.Date,
		Loc:  rect,
	}, nil
}

func buildSpatialIndex(store *Store, geocoder *Geocoder) (*rtreego.Rtree, error) {
	ids, err := store.List()
	if err != nil {
		return nil, err
	}
	rt := rtreego.NewTree(2, 25)
	for i, id := range ids {
		if (i+1)%500 == 0 {
			fmt.Printf("%d/%d spatially indexed\n", i+1, len(ids))
		}
		loc, err := getOfferLocation(store, geocoder, id)
		if err != nil {
			return nil, err
		}
		if loc == nil {
			continue
		}
		rt.Insert(loc)
	}
	fmt.Printf("%d spatially indexed\n", rt.Size())
	return rt, nil
}

func makeGeoRect(lat, lon, radius float64) (rtreego.Rect, error) {
	earth := float64(6371000)
	dlat := (radius / (math.Pi * earth)) * 180.0
	r := earth * math.Cos((math.Pi*lat)/180.0)
	dlon := (radius / (math.Pi * r)) * 180.
	lon -= dlon
	lat -= dlat
	return rtreego.NewRect(rtreego.Point{lon, lat}, [2]float64{2 * dlon, 2 * dlat})
}

func findNearestOffers(rt *rtreego.Rtree, lat, lon, maxDist float64) (
	[]datedOffer, error) {

	query, err := makeGeoRect(lat, lon, maxDist)
	if err != nil {
		return nil, err
	}
	offers := []datedOffer{}
	results := rt.SearchIntersect(&query)
	for _, r := range results {
		loc := r.(*OfferLoc)
		offers = append(offers, datedOffer{
			Date: loc.Date.Format(time.RFC3339),
			Id:   loc.Id,
		})
	}
	return offers, nil
}
