package main

import (
	"testing"
)

func TestParseSalary(t *testing.T) {
	tests := []struct {
		Input string
		Min   int
		Max   int
	}{
		{
			Input: "20 à 30 kEUR",
			Min:   20,
			Max:   30,
		},
		{
			Input: "20 kEUR",
			Min:   20,
			Max:   20,
		},
		{
			Input: "45 0 60 K€ brut/an",
			Min:   45,
			Max:   60,
		},
	}

	for _, test := range tests {
		min, max, err := parseSalary(test.Input)
		if err != nil {
			t.Fatalf("failed to parse %s: %s", test.Input, err)
		}
		if min != test.Min || max != test.Max {
			t.Fatalf("unexpected output: (%d, %d) != (%d, %d)", min, max,
				test.Min, test.Max)
		}
	}
}
