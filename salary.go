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
	reSalarySep   = regexp.MustCompile(`\d\s+0\s+\d`)
	reSalarySplit = regexp.MustCompile(`(?:^|[^\d])(\d+)\s+(\d{3})(?:[^\d]|$)`)
	cleaner       = transform.Chain(norm.NFD,
		transform.RemoveFunc(func(r rune) bool {
			return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
		}),
		norm.NFC)
)

func cleanSalary(input string) string {
	output, _, _ := transform.String(cleaner, input)
	output = strings.ToLower(output)
	m := reSalarySep.FindStringSubmatchIndex(output)
	if m != nil {
		output = output[:m[0]+1] + " - " + output[m[1]-1:]
	}
	m2 := reSalarySplit.FindAllStringSubmatchIndex(output, -1)
	if m2 != nil {
		res := ""
		start := 0
		for _, m := range m2 {
			s0, e0 := m[0], m[1]
			_, e1 := m[2], m[3]
			s2, _ := m[4], m[5]
			res += output[start:s0]
			start = e0
			res += output[s0:e1] + output[s2:e0]
		}
		res += output[start:]
		output = res
	}
	return output
}

func parseSalary(s string) (int, int, error) {
	s = cleanSalary(s)
	m := reSalaryNum.FindAllStringSubmatch(s, -1)
	if m == nil {
		return 0, 0, nil
	}
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
