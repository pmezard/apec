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
	reSalarySplit = regexp.MustCompile(`(?:^|\D)(\d+)\s+(\d{3})(?:\D|$)`)
)

func cleanSalary(input string) string {
	cleaner := transform.Chain(norm.NFD,
		transform.RemoveFunc(func(r rune) bool {
			return unicode.Is(unicode.Mn, r) // Mn: nonspacing marks
		}),
		norm.NFC)
	output, _, _ := transform.String(cleaner, input)
	output = strings.ToLower(output)
	m := reSalarySep.FindStringSubmatchIndex(output)
	if m != nil {
		output = output[:m[0]+1] + " - " + output[m[1]-1:]
	}
	for {
		m := reSalarySplit.FindStringSubmatchIndex(output)
		if m == nil {
			break
		}
		_, e1 := m[2], m[3]
		s2, _ := m[4], m[5]
		output = output[:e1] + output[s2:]
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
	l := len(values)
	switch l {
	case 0:
		return 0, 0, fmt.Errorf("not enough numbers")
	case 1:
		return values[0], values[0], nil
	default:
		return values[0], values[1], nil
	}
}
