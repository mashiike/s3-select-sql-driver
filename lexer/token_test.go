package lexer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLexer__Simple(t *testing.T) {
	query := "SELECT * FROM S3Object as s"
	lexer := NewLexer(query)
	tokens, err := lexer.Lex()
	require.NoError(t, err)
	expected := Tokens{
		{Kind: KindIdentifier, Value: "SELECT"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: "*"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "FROM"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "S3Object"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "as"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s"},
		{Kind: KindEOF, Value: ""},
	}
	require.EqualValues(t, expected, tokens)
	require.Equal(t, query, tokens.String())
}

func TestLexer__WithPlaceholder(t *testing.T) {
	query := "SELECT * FROM S3Object as s WHERE s._1 > ? AND s._2 = 'hoge?'"
	lexer := NewLexer(query)
	tokens, err := lexer.Lex()
	require.NoError(t, err)
	expected := Tokens{
		{Kind: KindIdentifier, Value: "SELECT"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: "*"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "FROM"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "S3Object"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "as"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "WHERE"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s._1"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: ">"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindPlaceholder, Value: "?"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "AND"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s._2"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: "="},
		{Kind: KindSpace, Value: " "},
		{Kind: KindString, Value: "'hoge?'"},
		{Kind: KindEOF, Value: ""},
	}
	require.EqualValues(t, expected, tokens)
	require.Equal(t, query, tokens.String())
}

func TestLexer__WithNamedPlaceholder(t *testing.T) {
	query := "SELECT s._2 as hoge,_3 FROM S3Object as s WHERE s._1 > :time AND s._2 = 'hoge?'"
	lexer := NewLexer(query)
	tokens, err := lexer.Lex()
	require.NoError(t, err)
	expected := []Token{
		{Kind: KindIdentifier, Value: "SELECT"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s._2"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "as"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "hoge"},
		{Kind: KindSymbol, Value: ","},
		{Kind: KindIdentifier, Value: "_3"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "FROM"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "S3Object"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "as"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "WHERE"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s._1"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: ">"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindNamedPlaceholder, Value: ":time"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "AND"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s._2"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: "="},
		{Kind: KindSpace, Value: " "},
		{Kind: KindString, Value: "'hoge?'"},
		{Kind: KindEOF, Value: ""},
	}
	require.EqualValues(t, expected, tokens)
	require.Equal(t, query, tokens.String())
}
