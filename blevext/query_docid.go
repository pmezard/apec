package blevext

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type docIDQuery struct {
	IDs      []string `json:"ids"`
	BoostVal float64  `json:"boost,omitempty"`
}

// NewTermQuery creates a new Query for finding an
// exact term match in the index.
func NewDocIDQuery(ids []string) *docIDQuery {
	return &docIDQuery{
		IDs:      ids,
		BoostVal: 1.0,
	}
}

func (q *docIDQuery) Boost() float64 {
	return q.BoostVal
}

func (q *docIDQuery) SetBoost(b float64) bleve.Query {
	q.BoostVal = b
	return q
}

func (q *docIDQuery) Field() string {
	return ""
}

func (q *docIDQuery) SetField(f string) bleve.Query {
	return q
}

func (q *docIDQuery) Searcher(i index.IndexReader, m *bleve.IndexMapping, explain bool) (search.Searcher, error) {
	return NewDocIDSearcher(q.IDs, q.BoostVal, explain)
}

func (q *docIDQuery) Validate() error {
	return nil
}
