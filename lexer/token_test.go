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
	expected := Tokens{
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

func TestLexer__TableNameWithPath(t *testing.T) {
	query := "SELECT * FROM S3Object[*].hoge as s"
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
		{Kind: KindSymbol, Value: "["},
		{Kind: KindSymbol, Value: "*"},
		{Kind: KindSymbol, Value: "]"},
		{Kind: KindIdentifier, Value: ".hoge"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "as"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s"},
		{Kind: KindEOF, Value: ""},
	}
	require.EqualValues(t, expected, tokens)
	require.Equal(t, query, tokens.String())
}

func TestLexer__WithComment(t *testing.T) {
	query := `-- line comment
/* block comment */
SELECT * FROM S3Object as s`
	lexer := NewLexer(query)
	tokens, err := lexer.Lex()
	require.NoError(t, err)
	expected := Tokens{
		{Kind: KindComment, Value: "-- line comment"},
		{Kind: KindNewline, Value: "\n"},
		{Kind: KindComment, Value: "/* block comment */"},
		{Kind: KindNewline, Value: "\n"},
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

func TestLexer__WithFunction(t *testing.T) {
	query := `SELECT * FROM s3object s WHERE CAST(s._N as FLOAT) > 12.34`
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
		{Kind: KindIdentifier, Value: "s3object"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "WHERE"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "CAST"},
		{Kind: KindSymbol, Value: "("},
		{Kind: KindIdentifier, Value: "s._N"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "as"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "FLOAT"},
		{Kind: KindSymbol, Value: ")"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindSymbol, Value: ">"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindNumber, Value: "12.34"},
		{Kind: KindEOF, Value: ""},
	}
	require.EqualValues(t, expected, tokens)
	require.Equal(t, query, tokens.String())
}

func TestLexer__Like(t *testing.T) {
	query := `SELECT * FROM s3object s WHERE s._N like '%xyz%'`
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
		{Kind: KindIdentifier, Value: "s3object"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "WHERE"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "s._N"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindIdentifier, Value: "like"},
		{Kind: KindSpace, Value: " "},
		{Kind: KindString, Value: "'%xyz%'"},
		{Kind: KindEOF, Value: ""},
	}
	require.EqualValues(t, expected, tokens)
	require.Equal(t, query, tokens.String())
}
