package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/pmezard/apec/jstruct"
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

func geocodeOffer(geocoder *Geocoder, offer *Offer, offline bool) (
	string, *jstruct.Location, error) {

	var loc *jstruct.Location
	var err error
	candidates := fixLocation(offer.Location)
	for _, candidate := range candidates {
		loc, err = geocoder.Geocode(candidate, "fr", offline)
		if err != nil {
			return candidate, nil, err
		}
		if loc == nil || len(loc.Results) == 0 {
			continue
		}
		res := loc.Results[0].Component
		offer.City = res.City
		offer.County = res.County
		offer.State = res.State
		offer.Country = res.Country
		return candidate, loc, nil
	}
	return offer.Location, loc, nil
}

func geocodeOffers(geocoder *Geocoder, offers []*Offer, minQuota int) (int, error) {
	rejected := 0
	quota := true
	for i, offer := range offers {
		candidates := fixLocation(offer.Location)
		found := false
		for _, c := range candidates {
			// Resolve from cache
			pos, ok, err := geocoder.GetCachedLocation(c, "fr")
			if err != nil {
				return rejected, err
			}
			if pos != nil {
				found = true
				offer.City = pos.City
				offer.County = pos.County
				offer.State = pos.State
				offer.Country = pos.Country
				break
			}
			if ok {
				// It cannot be geocoded
				break
			}
			if !quota {
				// Tolerate a lower quality geocoding for now
				continue
			}
			loc, err := geocoder.Geocode(c, "fr", false)
			if err != nil {
				fmt.Printf("error: geocoding %s: %s\n", c, err)
				if err != QuotaError {
					return rejected, err
				}
				quota = false
				continue
			}
			if loc.Rate.Remaining <= minQuota {
				// Try to preserve quota for test purpose. This is not
				// perfect as it consumes one geocoding token per function
				// call. I do not know how to query quota directly yet.
				quota = false
			}
			p := buildLocation(loc)
			result := "no result"
			if p != nil {
				result = loc.Results[0].Component.String()
			}
			fmt.Printf("geocoding %d/%d %s => %s => %s (quota: %d/%d)\n",
				i+1, len(offers), offer.Location, c, result, loc.Rate.Remaining,
				loc.Rate.Limit)
			if p != nil {
				offer.City = p.City
				offer.County = p.County
				offer.State = p.State
				offer.Country = p.Country
				found = true
			}
			time.Sleep(1 * time.Second)
		}
		if !found {
			rejected++
		}
	}
	return rejected, nil
}
