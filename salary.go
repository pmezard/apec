package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var (
	reSalaryNum   = regexp.MustCompile(`(\d+(?:\.\d+)?)`)
	reSalaryUndef = regexp.MustCompile(`^(?:.*(definir|negoc|profil|experience|a voir|determiner|attract|precise|selon|competitif).*|nc|-)$`)
)

func isMn(r rune) bool {
	return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
}

var (
	cleaner = transform.Chain(norm.NFD,
		transform.RemoveFunc(isMn),
		norm.NFC)
)

func normString(s string) string {
	result, _, _ := transform.String(cleaner, s)
	return result
}

func parseSalary(s string) (int, int, error) {
	s = strings.ToLower(normString(s))
	m := reSalaryNum.FindAllStringSubmatch(s, -1)
	if m != nil {
		values := []int{}
		for _, n := range m {
			v, err := strconv.ParseFloat(n[0], 32)
			if err != nil {
				return -1, -1, err
			}
			if v >= 1000 {
				v = v / 1000.
			}
			values = append(values, int(v))
		}
		switch len(values) {
		case 1:
			return values[0], values[0], nil
		case 2:
			return values[0], values[1], nil
		}
		return 0, 0, fmt.Errorf("too many numbers")
	}
	return 0, 0, nil
}
