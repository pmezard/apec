package main

import (
	"fmt"
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
		nfdString("Agglo."),
		nfdString("Agglo"),
		nfdString("Agence de"),
		nfdString("Agence"),
		nfdString("Basé"),
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
	l := strings.ToLower(s)
	if l == "idf" {
		return []string{"Ile-de-France"}
	}
	if strings.Contains(l, "boulogne b") {
		return []string{"Boulogne Billancourt"}
	}
	if strings.Contains(l, "velizy") {
		return []string{"Velizy"}
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
	result := []string{strings.TrimSpace(s)}
	result = apply(result, splitAlternatives)
	result = apply(result, stripPrefixes)
	result = apply(result, fixWellKnown)
	result = apply(result, fixCountryNums)
	result = apply(result, nfcString)
	return result
}

func geocodeOffer(geocoder *Geocoder, offer *Offer, offline bool) (
	string, *Location, error) {

	var loc *Location
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

func geocodeOffers(geocoder *Geocoder, offers []*Offer, verbose bool) (int, error) {
	rejected := 0
	for _, offer := range offers {
		q, loc, err := geocodeOffer(geocoder, offer, rejected > 0)
		if err != nil {
			fmt.Printf("error: geocoding %s: %s\n", q, err)
			if err != QuotaError {
				return rejected, err
			}
			rejected += 1
		} else if loc == nil {
			rejected += 1
		} else if !loc.Cached || verbose {
			result := "no result"
			if len(loc.Results) > 0 {
				result = loc.Results[0].Component.String()
			}
			if !loc.Cached {
				fmt.Printf("geocoding %s => %s => %s (quota: %d/%d)\n",
					offer.Location, q, result, loc.Rate.Remaining, loc.Rate.Limit)
				time.Sleep(1 * time.Second)
			} else {
				fmt.Printf("geocoding %s => %s => %s\n", offer.Location, q, result)
			}
		}
	}
	return rejected, nil
}
