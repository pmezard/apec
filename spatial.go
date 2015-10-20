package main

import (
	"math"
	"sync"
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

func makeOfferLocation(id string, date time.Time, loc *Location) (*OfferLoc, error) {
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
		Date: date,
		Loc:  rect,
	}, nil
}

func getOfferLocation(store *Store, geocoder *Geocoder, id string) (*OfferLoc, error) {
	offer, err := getStoreOffer(store, id)
	if err != nil {
		return nil, err
	}
	if offer == nil {
		return nil, nil
	}
	_, loc, err := geocodeOffer(geocoder, offer, true)
	if err != nil {
		return nil, err
	}
	return makeOfferLocation(offer.Id, offer.Date, loc)
}

type SpatialIndex struct {
	lock  sync.RWMutex
	rtree *rtreego.Rtree
	known map[string]*OfferLoc
}

func NewSpatialIndex() *SpatialIndex {
	return &SpatialIndex{
		rtree: rtreego.NewTree(2, 25),
		known: map[string]*OfferLoc{},
	}
}

func (s *SpatialIndex) Add(o *OfferLoc) {
	s.lock.Lock()
	defer s.lock.Unlock()
	prev := s.known[o.Id]
	if prev != nil {
		s.rtree.Delete(prev)
	}
	s.rtree.Insert(o)
	s.known[o.Id] = o
}

func (s *SpatialIndex) Remove(id string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	o := s.known[id]
	if o != nil {
		s.rtree.Delete(o)
		delete(s.known, id)
	}
}

func (s *SpatialIndex) List() []string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	ids := make([]string, len(s.known))
	for id := range s.known {
		ids = append(ids, id)
	}
	return ids
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

func (s *SpatialIndex) FindNearest(lat, lon, maxDist float64) ([]datedOffer, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	query, err := makeGeoRect(lat, lon, maxDist)
	if err != nil {
		return nil, err
	}
	offers := []datedOffer{}
	results := s.rtree.SearchIntersect(&query)
	for _, r := range results {
		loc := r.(*OfferLoc)
		offers = append(offers, datedOffer{
			Date: loc.Date.Format(time.RFC3339),
			Id:   loc.Id,
		})
	}
	return offers, nil
}
