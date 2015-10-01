package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/alecthomas/kingpin"
)

var (
	app     = kingpin.New("apec", "APEC crawler, indexer and query tool")
	dataDir = app.Flag("data", "data directory").Default("offers").String()
)

type Config struct {
	RootDir string
}

func NewConfig(rootDir string) *Config {
	return &Config{
		RootDir: rootDir,
	}
}

func (d *Config) Store() string {
	return filepath.Join(d.RootDir, "offers")
}

func (d *Config) Index() string {
	return filepath.Join(d.RootDir, "index")
}

func (d *Config) Geocoder() string {
	return filepath.Join(d.RootDir, "geocoder")
}

func (d *Config) GeocodingKey() string {
	return os.Getenv("APEC_GEOCODING_KEY")
}

func dispatch() error {
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))
	cfg := NewConfig(*dataDir)
	switch cmd {
	case crawlCmd.FullCommand():
		return crawlOffers(cfg)
	case indexCmd.FullCommand():
		return indexOffers(cfg)
	case searchCmd.FullCommand():
		return search(cfg)
	case webCmd.FullCommand():
		return web(cfg)
	case geocodeCmd.FullCommand():
		return geocode(cfg)
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
