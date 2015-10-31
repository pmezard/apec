package blevext

import (
	"sort"

	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

// DocIDSearcher returns documents matching a predefined set of identifiers.
type DocIDSearcher struct {
	ids     []string
	current int
	scorer  *scorers.ConstantScorer
}

func NewDocIDSearcher(ids []string, boost float64, explain bool) (*DocIDSearcher, error) {
	scorer := scorers.NewConstantScorer(1.0, boost, explain)
	return &DocIDSearcher{
		ids:    ids,
		scorer: scorer,
	}, nil
}

func (s *DocIDSearcher) Count() uint64 {
	return uint64(len(s.ids))
}

func (s *DocIDSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *DocIDSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *DocIDSearcher) Next() (*search.DocumentMatch, error) {
	if s.current >= len(s.ids) {
		return nil, nil
	}
	id := s.ids[s.current]
	s.current++
	docMatch := s.scorer.Score(id)
	return docMatch, nil

}

func (s *DocIDSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	s.current = sort.SearchStrings(s.ids, ID)
	return s.Next()
}

func (s *DocIDSearcher) Close() error {
	return nil
}

func (s *DocIDSearcher) Min() int {
	return 0
}
