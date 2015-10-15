package main

import (
	"log"
	"sort"
)

type SpatialIndexer struct {
	store    *Store
	index    *SpatialIndex
	geocoder *Geocoder
	reset    chan bool
	stop     chan chan bool
}

func NewSpatialIndexer(store *Store, index *SpatialIndex,
	geocoder *Geocoder) *SpatialIndexer {

	idx := &SpatialIndexer{
		store:    store,
		index:    index,
		geocoder: geocoder,
		reset:    make(chan bool, 1),
		stop:     make(chan chan bool),
	}
	go idx.dispatch()
	return idx
}

func (idx *SpatialIndexer) Close() {
	done := make(chan bool)
	idx.stop <- done
	<-done
}

func (idx *SpatialIndexer) Sync() {
	select {
	case idx.reset <- true:
	default:
	}
}

func (idx *SpatialIndexer) dispatch() {
	for {
		select {
		case <-idx.reset:
			err := idx.sync()
			if err != nil {
				log.Printf("error: spatial indexer reset failed: %s", err)
				continue
			}
		case done := <-idx.stop:
			close(done)
			return
		}
	}
}

func diffIds(from []string, to []string) ([]string, []string) {
	sort.Strings(from)
	sort.Strings(to)

	added := []string{}
	for _, id := range from {
		i := sort.SearchStrings(to, id)
		if i >= len(to) || to[i] != id {
			added = append(added, id)
		}
	}
	removed := []string{}
	for _, id := range to {
		i := sort.SearchStrings(from, id)
		if i >= len(from) || from[i] != id {
			removed = append(removed, id)
		}
	}
	return added, removed
}

func (idx *SpatialIndexer) sync() error {
	// For now we can live with loading both set of ids and diffing them
	stored, err := idx.store.List()
	if err != nil {
		return err
	}
	indexed := idx.index.List()
	added, removed := diffIds(stored, indexed)

	log.Printf("spatially indexing %d, removing %d", len(added), len(removed))
	for i, id := range removed {
		if (i+1)%500 == 0 {
			log.Printf("%d spatially removed", i+1)
		}
		idx.index.Remove(id)
	}
	for i, id := range added {
		if (i+1)%500 == 0 {
			log.Printf("%d spatially indexed", i+1)
		}
		loc, err := getOfferLocation(idx.store, idx.geocoder, id)
		if err != nil {
			return err
		}
		if loc != nil {
			idx.index.Add(loc)
		}
	}
	log.Printf("spatial indexation done")
	return nil
}
