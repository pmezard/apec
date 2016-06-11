//go:generate ffjson $GOFILE

package jstruct

import "fmt"

type JsonOffer struct {
	Id          string `json:"numeroOffre"`
	Title       string `json:"intitule"`
	Date        string `json:"datePublication"`
	Salary      string `json:"salaireTexte"`
	PartialTime bool   `json:"tempsPartiel"`
	Location    string `json:"lieuTexte"`
	Locations   []struct {
		Name string `json:"libelleLieu"`
	} `json:"lieux"`
	HTML    string `json:"texteHtml"`
	Account string `json:"nomCompteEtablissement"`
}

func (offer *JsonOffer) Type() string {
	return "offer"
}

type LocRate struct {
	Limit     int `json:"limit"`
	Remaining int `json:"remaining"`
}

type LocComponent struct {
	City        string `json:"city"`
	PostCode    string `json:"postcode"`
	County      string `json:"county"`
	State       string `json:"state"`
	Country     string `json:"country"`
	CountryCode string `json:"country_code"`
}

func (c *LocComponent) String() string {
	values := []struct {
		Field string
		Value string
	}{
		{"city", c.City},
		{"postcode", c.PostCode},
		{"county", c.County},
		{"state", c.State},
		{"country", c.Country},
	}
	s := ""
	written := false
	for _, v := range values {
		if v.Value == "" {
			continue
		}
		if written {
			s += ", "
		}
		s += fmt.Sprintf("%s: %s", v.Field, v.Value)
		written = true
	}
	return s
}

type LocGeom struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lng"`
}

type LocResult struct {
	Component LocComponent `json:"components"`
	Geometry  *LocGeom     `json:"geometry"`
}

type Location struct {
	Cached  bool
	Rate    LocRate     `json:"rate"`
	Results []LocResult `json:"results"`
}
