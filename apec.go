package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kingpin"
)

var (
	app = kingpin.New("apec", "APEC crawler, indexer and query tool")
)

func dispatch() error {
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case crawlCmd.FullCommand():
		return crawlOffers()
	case indexCmd.FullCommand():
		return indexOffers()
	case searchCmd.FullCommand():
		return search()
	case webCmd.FullCommand():
		return web()
	case geocodeCmd.FullCommand():
		return geocode()
	}
	return nil
}

func main() {
	err := dispatch()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}
