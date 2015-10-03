package main

import (
	"testing"
)

func TestFixLocation(t *testing.T) {
	tests := []struct {
		Input  string
		Output []string
	}{
		{
			Input:  "Paris",
			Output: []string{"Paris"},
		},
		{
			Input:  "Idf",
			Output: []string{"Ile-de-France"},
		},
		{
			Input:  "29 - 56/75 ou 92, 93 ",
			Output: []string{"29", "56", "75", "92", "93"},
		},
		{
			Input:  "proche velizy",
			Output: []string{"Velizy"},
		},
		{
			Input:  "départements 22 et 1",
			Output: []string{"22", "1"},
		},
		{
			Input:  "BOULOGNE BILL",
			Output: []string{"Boulogne Billancourt"},
		},
		{
			Input:  "Nantes ou paris",
			Output: []string{"Nantes", "paris"},
		},
	}

	for _, test := range tests {
		res := fixLocation(test.Input)
		if len(res) != len(test.Output) {
			t.Fatalf("candidate lengths do not match for '%s': %+v != %+v",
				test.Input, res, test.Output)
		}
		for i, v := range test.Output {
			if res[i] != v {
				t.Fatalf("candidates do not match for '%s': %s != %s",
					test.Input, v, res[i])
			}
		}
	}
}
