package main

import (
	"log"
	"strings"
	"time"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

func nfdString(s string) string {
	result, _, _ := transform.String(norm.NFD, s)
	return result
}

func consumeNumSep(s string) (string, int) {
	consumed := 0
	for len(s) > 0 {
		c := s[0]
		if c == ' ' || c == '-' || c == '/' || c == ',' {
			consumed += 1
			s = s[1:]
			continue
		}
		if strings.HasPrefix(s, "ou") || strings.HasPrefix(s, "et") {
			consumed += 2
			s = s[2:]
			continue
		}
		break
	}
	return s, consumed
}

func isNum(c byte) bool {
	return c >= '0' && c <= '9'
}

// fixCountryNums split inputs like "23/45 - 52 ou 92" into country numbers.
func fixCountryNums(s string) []string {
	found := []string{}
	input, _ := consumeNumSep(s)
	var consumed int
	for {
		if len(input) >= 2 && isNum(input[0]) && isNum(input[1]) {
			found = append(found, input[:2])
			input = input[2:]
		} else if len(input) >= 1 && isNum(input[0]) {
			found = append(found, input[:1])
			input = input[1:]
		} else {
			break
		}
		input, consumed = consumeNumSep(input)
		if consumed <= 0 && input != "" {
			break
		}
		if input == "" {
			return found
		}
	}
	return []string{s}
}

var (
	locPrefixes = []string{
		nfdString("proche de"),
		nfdString("proche"),
		nfdString("dpts"),
		nfdString("dpt"),
		nfdString("départem."),
		nfdString("départements"),
		nfdString("agglo."),
		nfdString("agglo"),
		nfdString("agence de"),
		nfdString("agence"),
		nfdString("basé"),
	}
)

func stripPrefixes(s string) []string {
	orig := nfdString(s)
	stripped := orig
	for _, p := range locPrefixes {
		if strings.HasPrefix(stripped, p) {
			stripped = strings.TrimSpace(stripped[len(p):])
		}
	}
	if stripped != orig {
		s = stripped
	}
	return []string{s}
}

func splitAlternatives(s string) []string {
	return strings.Split(s, " ou ")
}

func fixWellKnown(s string) []string {
	if s == "idf" {
		return []string{"ile-de-france"}
	}
	if strings.Contains(s, "boulogne b") {
		return []string{"boulogne billancourt"}
	}
	if strings.Contains(s, "velizy") {
		return []string{"velizy"}
	}
	return []string{s}
}

func apply(input []string, fn func(string) []string) []string {
	output := []string{}
	for _, s := range input {
		res := fn(s)
		for _, r := range res {
			r = strings.TrimSpace(r)
			if r != "" {
				output = append(output, r)
			}
		}
	}
	return output
}

func nfcString(s string) []string {
	result, _, _ := transform.String(norm.NFC, s)
	return []string{result}
}

func fixLocation(s string) []string {
	result := []string{strings.TrimSpace(strings.ToLower(s))}
	result = apply(result, splitAlternatives)
	result = apply(result, stripPrefixes)
	result = apply(result, fixWellKnown)
	result = apply(result, fixCountryNums)
	result = apply(result, nfcString)
	return result
}

// getOfferLocation returns a cached or live geocoded location, an updated
// "offline" boolean signaling whether live calls could proceed or not, and an
// error on failure.
func geocodeOffer(geocoder *Geocoder, location string, offline bool,
	minQuota int) (*Location, bool, bool, error) {

	candidates := fixLocation(location)
	for _, c := range candidates {
		// Resolve from cache
		pos, ok, err := geocoder.GetCachedLocation(c, "fr")
		if err != nil {
			return nil, false, offline, err
		}
		if pos != nil || ok {
			return pos, false, offline, nil
		}
		if offline {
			// Tolerate a lower quality geocoding for now
			continue
		}
		loc, err := geocoder.Geocode(c, "fr", false)
		if err != nil {
			if err != QuotaError {
				return nil, false, offline, err
			}
			offline = true
			continue
		}
		if loc.Rate.Remaining <= minQuota {
			// Try to preserve quota for test purpose. This is not
			// perfect as it consumes one geocoding token per function
			// call. I do not know how to query quota directly yet.
			offline = true
		}
		p := buildLocation(loc)
		result := "no result"
		if p != nil {
			result = loc.Results[0].Component.String()
		}
		log.Printf("geocoding %s => %s => %s (quota: %d/%d)\n",
			location, c, result, loc.Rate.Remaining, loc.Rate.Limit)
		time.Sleep(1 * time.Second)
		if p != nil {
			return p, true, offline, nil
		}
	}
	return nil, false, offline, nil
}

func geocodeOffers(geocoder *Geocoder, offers []*Offer, minQuota int) (int, error) {
	rejected := 0
	offline := false
	for _, offer := range offers {
		pos, _, off, err := geocodeOffer(geocoder, offer.Location,
			offline, minQuota)
		if err != nil {
			return rejected, err
		}
		offline = off
		if pos != nil {
			offer.Geo = pos
		} else {
			rejected++
		}
	}
	return rejected, nil
}
