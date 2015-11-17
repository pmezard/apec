//go:generate go tool yacc -o query.y.go query.y

package blevext

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

var (
	traceParser = false
)

// queryLexer implements yacc yyLexer interface as a small wrapper around
// []*yySimType.
type queryLexer struct {
	tokens []*yySymType
	err    string
	result *Node
}

func (lex *queryLexer) Lex(lval *yySymType) int {
	if len(lex.tokens) == 0 {
		return 0
	}
	token := lex.tokens[0]
	lex.tokens = lex.tokens[1:]
	lval.s = token.s
	return token.yys
}

func (lex *queryLexer) Error(s string) {
	lex.err = s
}

// Lex extracts tokens from input string.
func Lex(input string) ([]*yySymType, error) {
	tokens := []*yySymType{}
	for {
		input = strings.TrimLeftFunc(input, unicode.IsSpace)
		if input == "" {
			break
		}
		if input[0] == '(' || input[0] == ')' {
			typ := tLPARENS
			if input[0] == ')' {
				typ = tRPARENS
			}
			tokens = append(tokens, &yySymType{
				yys: typ,
				s:   input[:1],
			})
			input = input[1:]
		} else if input[0] == '"' {
			pos := strings.IndexByte(input[1:], '"')
			if pos < 0 {
				return nil, fmt.Errorf("unclosed literal string: %q", input)
			}
			pos += 1
			tokens = append(tokens, &yySymType{
				yys: tPHRASE,
				s:   input[1:pos],
			})
			input = input[pos+1:]
		} else {
			pos := strings.IndexFunc(input, func(r rune) bool {
				return unicode.IsSpace(r) || r == ')' || r == '('
			})
			if pos < 0 {
				pos = len(input)
			}
			s := input[:pos]
			input = input[pos:]
			typ := tSTRING
			switch s {
			case "and":
				typ = tAND
			case "or":
				typ = tOR
			}
			tokens = append(tokens, &yySymType{
				yys: typ,
				s:   s,
			})
		}
	}
	return tokens, nil
}

type NodeKind int

const (
	NodeAnd NodeKind = iota + 1
	NodeOr
	NodeString
	NodePhrase
)

type Node struct {
	Kind     NodeKind
	Children []*Node
	Value    string
}

// Parse takes an input query expression and return the parsed node tree, or
// nil if the expression is empty, or an error. A query expression supports the
// following constructs (without the single quotes):
// - query strings: 'symbols_without_spaces_or_parenthesis'
// - query phrases: '"words withing double quotes"
// - 'a and b' or 'a or b'
// - '(a or b) and c'
func Parse(input string) (*Node, error) {
	tokens, err := Lex(input)
	if err != nil {
		return nil, err
	}
	lexer := &queryLexer{
		tokens: tokens,
	}
	if traceParser {
		traceRule("<START\n")
	}
	res := yyParse(lexer)
	if traceParser {
		traceRule("END>\n")
	}
	if res != 0 {
		return nil, errors.New(lexer.err)
	}
	return lexer.result, nil
}
