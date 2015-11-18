package blevext

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
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

func (q *allMatchQuery) SetBoost(b float64) bleve.Query {
	q.BoostVal = b
	return q
}

func (q *allMatchQuery) Field() string {
	return q.FieldVal
}

func (q *allMatchQuery) SetField(f string) bleve.Query {
	q.FieldVal = f
	return q
}

func (q *allMatchQuery) Searcher(i index.IndexReader, m *bleve.IndexMapping, explain bool) (search.Searcher, error) {

	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}
	analyzerName := m.FieldAnalyzer(field)
	tokens, err := m.AnalyzeText(analyzerName, []byte(q.Match))
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		noneQuery := bleve.NewMatchNoneQuery()
		return noneQuery.Searcher(i, m, explain)
	}

	tqs := make([]bleve.Query, len(tokens))
	for i, token := range tokens {
		tqs[i] = bleve.NewTermQuery(string(token.Term)).
			SetField(field).
			SetBoost(q.BoostVal)
	}
	allQuery := bleve.NewConjunctionQuery(tqs).SetBoost(q.BoostVal)
	return allQuery.Searcher(i, m, explain)
}

func (q *allMatchQuery) Validate() error {
	return nil
}
