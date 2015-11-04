package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/index/upside_down"
)

type TermCount struct {
	Term  string
	Count uint64
}

type sortedTermCounts []TermCount

func (s sortedTermCounts) Len() int {
	return len(s)
}

func (s sortedTermCounts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedTermCounts) Less(i, j int) bool {
	//return s[i].Term < s[j].Term
	return s[i].Count < s[j].Count
}

func decodeTermFrequencyRow(tfr *upside_down.TermFrequencyRow) ([]byte, uint64, error) {
	key := tfr.Key()
	// Strip field
	if len(key) < 3 {
		return nil, 0, fmt.Errorf("key is unexpectedly short")
	}
	key = key[3:]
	pos := bytes.IndexByte(key, upside_down.ByteSeparator)
	if pos < 0 {
		return nil, 0, fmt.Errorf("cannot extract term")
	}
	term := key[:pos]

	value := tfr.Value()
	n, read := binary.Uvarint(value)
	if read <= 0 {
		return nil, 0, fmt.Errorf("could not decode term frequency")
	}
	return term, n, nil
}

var (
	histogramCmd = app.Command("histogram", "generate indexed terms histogram")
)

func histogramFn(cfg *Config) error {
	index, err := OpenOfferIndex(cfg.Index())
	if err != nil {
		return err
	}
	terms := map[string]uint64{}
	rows := index.DumpAll()
	var failed error
	for item := range rows {
		if failed != nil {
			continue
		}
		switch item.(type) {
		case error:
			failed = item.(error)
		case *upside_down.TermFrequencyRow:
			row := item.(*upside_down.TermFrequencyRow)
			term, freq, err := decodeTermFrequencyRow(row)
			if err != nil {
				failed = err
				continue
			}
			terms[string(term)] += freq
		}
	}
	if failed != nil {
		return failed
	}
	counts := make([]TermCount, 0, len(terms))
	for k, v := range terms {
		counts = append(counts, TermCount{
			Term:  k,
			Count: v,
		})
	}
	sort.Sort(sortedTermCounts(counts))
	for _, t := range counts {
		fmt.Println(t.Term, t.Count)
	}
	return nil
}

var (
	indexStatsCmd = app.Command("indexstats",
		"collect and display full text index statistics")
)

func indexStatsFn(cfg *Config) error {
	index, err := OpenOfferIndex(cfg.Index())
	if err != nil {
		return err
	}
	kinds := map[byte]struct {
		Count int
		Size  int
	}{}
	unknown := 0
	rows := index.DumpAll()
	var failed error
	for item := range rows {
		if failed != nil {
			continue
		}
		switch item.(type) {
		case error:
			failed = item.(error)
		case upside_down.UpsideDownCouchRow:
			row := item.(upside_down.UpsideDownCouchRow)
			key := row.Key()
			st := kinds[key[0]]
			st.Count++
			st.Size += row.KeySize() + row.ValueSize()
			kinds[key[0]] = st
		default:
			unknown++
		}
	}
	totalCount := 0
	totalSize := 0
	for i := 0; i < 256; i++ {
		st, ok := kinds[byte(i)]
		if !ok {
			continue
		}
		fmt.Printf("%s: count: %d, size: %.1fkB\n", string([]byte{byte(i)}),
			st.Count, float64(st.Size)/1024.)
		totalCount += st.Count
		totalSize += st.Size
	}
	fmt.Printf("total: count: %d, size: %.1fkB\n", totalCount, float64(totalSize)/1024.)
	if unknown > 0 {
		fmt.Printf("unknown rows: %d\n", unknown)
	}
	return nil
}
