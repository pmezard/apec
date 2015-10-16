package main

import (
	"fmt"
	"log"

	"github.com/blevesearch/bleve"
)

// Indexer is an online asynchronous indexer.
type Indexer struct {
	store    *Store
	index    bleve.Index
	queue    *IndexQueue
	geocoder *Geocoder
	reset    chan bool
	work     chan bool
	stop     chan chan bool
}

// NewIndexer creates a new Indexer assuming it is the soler writer for
// supplied store and index.
func NewIndexer(store *Store, index bleve.Index, geocoder *Geocoder,
	queue *IndexQueue) *Indexer {

	idx := &Indexer{
		store:    store,
		index:    index,
		geocoder: geocoder,
		queue:    queue,
		reset:    make(chan bool, 1),
		work:     make(chan bool, 1),
		stop:     make(chan chan bool),
	}
	go idx.dispatch()
	return idx
}

// Close signals and waits for the indexing goroutine to terminate.
func (idx *Indexer) Close() {
	done := make(chan bool)
	idx.stop <- done
	<-done
}

// Sync makes the indexer to compare the index and store again and synchronize
// them if necessary. The synchronization is performed asynchronously.
func (idx *Indexer) Sync() {
	select {
	case idx.reset <- true:
	default:
	}
}

func (idx *Indexer) dispatch() {
	for {
		select {
		case <-idx.reset:
			log.Printf("collecting index updates")
			err := idx.resetQueue()
			if err != nil {
				log.Printf("error: indexer reset failed: %s", err)
				continue
			}
			log.Printf("collection done")
			idx.signalWork()
		case <-idx.work:
			log.Printf("indexing documents, %d updates remaining", idx.queue.Size())
			err := idx.indexSome()
			if err != nil {
				log.Println("error: indexation failed: %s", err)
			}
			log.Printf("indexation done")
		case done := <-idx.stop:
			close(done)
			return
		}
	}
}

func listIndexIds(index bleve.Index) ([]string, error) {
	idx, _, err := index.Advanced()
	if err != nil {
		return nil, err
	}
	reader, err := idx.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	idReader, err := reader.DocIDReader("", "")
	if err != nil {
		return nil, err
	}
	defer idReader.Close()
	ids := []string{}
	for {
		id, err := idReader.Next()
		if err != nil {
			return nil, err
		}
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (idx *Indexer) resetQueue() error {
	ops := []Queued{}

	// For now we can live with loading both set of ids and diffing them
	stored, err := idx.store.List()
	if err != nil {
		return err
	}
	indexed, err := listIndexIds(idx.index)
	if err != nil {
		return err
	}
	added, removed := diffIds(stored, indexed)

	for _, id := range removed {
		ops = append(ops, Queued{Id: id, Op: RemoveOp})
	}
	for _, id := range added {
		ops = append(ops, Queued{Id: id, Op: AddOp})
	}
	log.Printf("queuing %d additions, %d removals", len(added), len(removed))

	// Update queue
	err = idx.queue.DeleteMany(idx.queue.Size())
	if err != nil {
		return err
	}
	return idx.queue.QueueMany(ops)
}

func (idx *Indexer) signalWork() {
	select {
	case idx.work <- true:
	default:
	}
}

func (idx *Indexer) indexSome() error {
	count := 10
	queued, err := idx.queue.FetchMany(count)
	if err != nil {
		return err
	}
	if len(queued) >= count {
		idx.signalWork()
	}
	for _, q := range queued {
		if q.Op == AddOp {
			log.Printf("indexing %s", q.Id)
			offer, err := getStoreOffer(idx.store, q.Id)
			if err != nil {
				return err
			}
			if offer != nil {
				_, _, err = geocodeOffer(idx.geocoder, offer, true)
				if err != nil {
					return err
				}
				err = idx.index.Index(offer.Id, offer)
				if err != nil {
					return err
				}
			}
		} else if q.Op == RemoveOp {
			log.Printf("deleting %s", q.Id)
			err = idx.index.Delete(q.Id)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("unknown operation: %v", q.Op)
		}
		err = idx.queue.DeleteMany(1)
		if err != nil {
			return err
		}
	}
	return nil
}
