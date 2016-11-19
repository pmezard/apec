package main

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
	return s[i].Count < s[j].Count
}

var (
	histogramCmd = app.Command("histogram", "generate indexed terms histogram")
)

func histogramFn(cfg *Config) error {
	/*
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
				case *upsidedown.TermFrequencyRow:
					row := item.(*upsidedown.TermFrequencyRow)
					terms[string(row.Term())] += row.Freq()
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
	*/
	return nil
}

var (
	indexStatsCmd = app.Command("indexstats",
		`collect and display full text index statistics

Each output line contains statistics about a bleve index row type. See:

  http://www.blevesearch.com/docs/Index-Structure/

For more details about bleve index structure.
`)
	indexPathArg = indexStatsCmd.Arg("path", "index path").String()
)

func indexStatsFn(cfg *Config) error {
	return nil
	/*
		path := *indexPathArg
		if path == "" {
			path = cfg.Index()
		}
		index, err := OpenOfferIndex(path)
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
			case upsidedown.UpsideDownCouchRow:
				row := item.(upsidedown.UpsideDownCouchRow)
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
	*/
}
