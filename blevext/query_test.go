package blevext

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func nodeToString(n *Node) (string, error) {
	if n == nil {
		return "", nil
	}
	var werr error
	write := func(w io.Writer, format string, args ...interface{}) {
		_, err := fmt.Fprintf(w, format, args...)
		if werr == nil {
			werr = err
		}
	}

	var toString func(w io.Writer, n *Node, prefix string) error
	toString = func(w io.Writer, n *Node, prefix string) error {
		switch n.Kind {
		case NodeString:
			write(w, prefix+n.Value+"\n")
		case NodePhrase:
			write(w, prefix+"'"+n.Value+"'\n")
		case NodeAnd:
			write(w, prefix+"AND\n")
			toString(w, n.Children[0], prefix+"  ")
			toString(w, n.Children[1], prefix+"  ")
		case NodeOr:
			write(w, prefix+"OR\n")
			toString(w, n.Children[0], prefix+"  ")
			toString(w, n.Children[1], prefix+"  ")
		default:
			return fmt.Errorf("unsuported node type: %d", n.Kind)
		}
		return nil
	}
	w := &bytes.Buffer{}
	err := toString(w, n, "")
	if err == nil {
		err = werr
	}
	return w.String(), err
}

func testLexer(t *testing.T, input, expected string) {
	n, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	got, err := nodeToString(n)
	if err != nil {
		t.Fatal(err)
	}
	if got != expected {
		t.Fatalf("\n%s\n!=\n%s\n", got, expected)
	}
}

func TestLexerStrings(t *testing.T) {
	testLexer(t, "", "")
	testLexer(t, "  ", "")
	testLexer(t, "  some_string", "some_string\n")
}

func TestLexerPhrases(t *testing.T) {
	testLexer(t, ` " several words here"`, "' several words here'\n")
	testLexer(t, `" "`, "' '\n")
}

func TestLexerAnd(t *testing.T) {
	testLexer(t, `foo and "several words"`, `AND
  foo
  'several words'
`)
}

func TestLexerOr(t *testing.T) {
	testLexer(t, `foo or "several words"`, `OR
  foo
  'several words'
`)
}

func TestLexerParens(t *testing.T) {
	testLexer(t, `a and (b or "c")`, `AND
  a
  OR
    b
    'c'
`)
	testLexer(t, `(a or b) and "c"`, `AND
  OR
    a
    b
  'c'
`)
	testLexer(t, `a or b and "c"`, `OR
  a
  AND
    b
    'c'
`)
	testLexer(t, `a and b or "c"`, `OR
  AND
    a
    b
  'c'
`)
}
