//go:generate ffjson $GOFILE

package jstruct

type JsonOffer struct {
	Id          string `json:"numeroOffre"`
	Title       string `json:"intitule"`
	Date        string `json:"datePublication"`
	Salary      string `json:"salaireTexte"`
	PartialTime bool   `json:"tempsPartiel"`
	Location    string `json:"lieuTexte"`
	HTML        string `json:"texteHtml"`
	Account     string `json:"nomCompteEtablissement"`
}

func (offer *JsonOffer) Type() string {
	return "offer"
}
