package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/alecthomas/kingpin"
	"github.com/davecheney/profile"
)

var (
	app     = kingpin.New("apec", "APEC crawler, indexer and query tool")
	dataDir = app.Flag("data", "data directory").Default("offers").String()
	prof    = app.Flag("profile", "enable profiling").Bool()
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
	if *prof {
		defer profile.Start(profile.CPUProfile).Stop()
	}
	cfg := NewConfig(*dataDir)
	switch cmd {
	case crawlCmd.FullCommand():
		return crawlFn(cfg)
	case indexCmd.FullCommand():
		return indexOffers(cfg)
	case searchCmd.FullCommand():
		return search(cfg)
	case webCmd.FullCommand():
		return web(cfg)
	case geocodeCmd.FullCommand():
		return geocode(cfg)
	case upgradeCmd.FullCommand():
		return upgrade(cfg)
	case dumpDeletedCmd.FullCommand():
		return dumpDeletedOffersFn(cfg)
	case changesCmd.FullCommand():
		return changesFn(cfg)
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
