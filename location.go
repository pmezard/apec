package main

import (
	"strings"

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
func fixCountryNums(s string, result []string) (string, []string) {
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
			return s, result
		}
		input, consumed = consumeNumSep(input)
		if consumed <= 0 && input != "" {
			return s, result
		}
		if input == "" {
			break
		}
	}
	result = append(result, found...)
	return "", result
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
	}
)

func stripPrefixes(s string, result []string) (string, []string) {
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
	return s, result
}

func fixWellKnown(s string, result []string) (string, []string) {
	l := strings.ToLower(s)
	if l == "idf" {
		result = append(result, "Ile-de-France")
		s = ""
	}
	if strings.Contains(l, "boulogne b") {
		result = append(result, "Boulogne Billancourt")
		s = ""
	}
	if strings.Contains(l, "velizy") {
		result = append(result, "Velizy")
		s = ""
	}
	return s, result
}

func fixLocation(s string) []string {
	result := []string{}
	s = strings.TrimSpace(s)
	s, result = stripPrefixes(s, result)
	s, result = fixWellKnown(s, result)
	s, result = fixCountryNums(s, result)
	if s != "" {
		result = append(result, s)
	}
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
