package main

import (
	"fmt"
	"github.com/alecthomas/kingpin"
	"os"
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
