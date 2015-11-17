%{
package blevext

import "fmt"

func traceRule(format string, args ...interface{}) {
	if traceParser {
		fmt.Printf(format, args...)
	}
}
%}

%union {
    s string
	n *Node
}

%token tAND
%token tOR
%token tLPARENS
%token tRPARENS
%token <s> tSTRING
%token <s> tPHRASE

%left tOR
%left tAND

%type <n> queryPart
%type <n> query

%%

query:
queryPart {
	traceRule("queryPart -> query")
	yylex.(*queryLexer).result = $1
}
|
/* empty */ {

};

queryPart:
tLPARENS queryPart tRPARENS {
	traceRule("tLPARENS queryPart tRPARENS -> queryPart\n")
	$$ = $2
}
|
queryPart tAND queryPart {
	traceRule("tAND -> queryPart\n")
	$$ = &Node{
		Kind: NodeAnd,
		Children: []*Node{
			$1,
			$3,
		},
	}
}
|
queryPart tOR queryPart {
	traceRule("tOR -> queryPart\n")
	$$ = &Node{
		Kind: NodeOr,
		Children: []*Node{
			$1,
			$3,
		},
	}
}
|
tPHRASE {
	traceRule("tPHRASE[%s] -> queryPart\n", $1)
    $$ = &Node{
        Kind: NodePhrase,
        Value: $1,
	}
}
|
tSTRING {
	traceRule("tSTRING[%s] -> queryPart\n", $1)
	$$ = &Node{
		Kind: NodeString,
		Value: $1,
    }
};

