package blevext

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
)

type allMatchQuery struct {
	Match    string  `json:"match"`
	FieldVal string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

// NewAllMatchQuery is like NewMatchQuery but all extracted terms must match.
func NewAllMatchQuery(match string) *allMatchQuery {
	return &allMatchQuery{
		Match:    match,
		BoostVal: 1.0,
	}
}

func (q *allMatchQuery) Boost() float64 {
	return q.BoostVal
}

func (q *allMatchQuery) SetBoost(b float64) {
	q.BoostVal = b
}

func (q *allMatchQuery) Field() string {
	return q.FieldVal
}

func (q *allMatchQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *allMatchQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {

	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	analyzerName := m.AnalyzerNameForPath(field)
	analyzer := m.AnalyzerNamed(analyzerName)
	tokens := analyzer.Analyze([]byte(q.Match))
	if len(tokens) == 0 {
		noneQuery := bleve.NewMatchNoneQuery()
		return noneQuery.Searcher(i, m, explain)
	}

	tqs := make([]query.Query, len(tokens))
	for i, token := range tokens {
		tq := bleve.NewTermQuery(string(token.Term))
		tq.SetField(field)
		tq.SetBoost(q.BoostVal)
		tqs[i] = tq
	}
	allQuery := bleve.NewConjunctionQuery(tqs...)
	allQuery.SetBoost(q.BoostVal)
	return allQuery.Searcher(i, m, explain)
}

func (q *allMatchQuery) Validate() error {
	return nil
}
