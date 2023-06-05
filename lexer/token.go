package lexer

import (
	"fmt"
	"strings"
)

type Token struct {
	Kind  Kind
	Value string
}

type Tokens []Token

func (ts Tokens) String() string {
	var builder strings.Builder
	for _, t := range ts {
		builder.WriteString(t.Value)
	}
	return builder.String()
}

type Kind int

const (
	KindUnknown Kind = iota
	KindEOF
	KindSpace
	KindNewline
	KindIdentifier
	KindPlaceholder
	KindNamedPlaceholder
	KindString
	KindNumber
	KindSymbol
	KindComment
)

func (k Kind) String() string {
	switch k {
	case KindUnknown:
		return "unknown"
	case KindEOF:
		return "eof"
	case KindSpace:
		return "space"
	case KindNewline:
		return "newline"
	case KindIdentifier:
		return "identifier"
	case KindPlaceholder:
		return "placeholder"
	case KindNamedPlaceholder:
		return "named placeholder"
	case KindString:
		return "string"
	case KindNumber:
		return "number"
	case KindSymbol:
		return "symbol"
	case KindComment:
		return "comment"
	default:
		return "undefined"
	}
}

type Lexer struct {
	input  string
	tokens Tokens
	pos    int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
	}
}

func (l *Lexer) Lex() (Tokens, error) {
	for {
		if l.pos >= len(l.input) {
			break
		}
		if err := l.lex(); err != nil {
			return nil, err
		}
	}
	l.tokens = append(l.tokens, Token{Kind: KindEOF})
	return l.tokens, nil
}

func (l *Lexer) lex() error {
	switch l.input[l.pos] {
	case ' ', '\t':
		return l.lexSpace()
	case '\n', '\r':
		return l.lexNewline()
	case '`', '"', '\'':
		return l.lexString()
	case '?':
		return l.lexPlaceholder()
	case ':':
		return l.lexNamedPlaceholder()
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return l.lexNumber()
	case '-':
		// if -- is comment then lexComment
		// else lexSymbol
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			return l.lexComment()
		}
		return l.lexSymbol()
	case '/':
		// if /* is comment then lexComment
		// else lexSymbol
		if l.pos+1 < len(l.input) && l.input[l.pos+1] == '*' {
			return l.lexComment()
		}
		return l.lexSymbol()
	case '(', ')', ',', ';', '[', ']', '{', '}', '+', '*', '%', '^', '=', '<', '>', '&', '|', '~', '!':
		return l.lexSymbol()
	default:
		return l.lexIdentifier()
	}
}

func (l *Lexer) lexComment() error {
	// check comment type /* */ or --
	start := l.pos
	l.pos++
	if l.pos >= len(l.input) {
		return &LexError{
			Pos: l.pos,
			Err: fmt.Errorf("unexpected eof"),
		}
	}
	var isLineComment bool
	var isBlockComment bool
	if l.input[l.pos] == '-' {
		isLineComment = true
	}
	if l.input[l.pos] == '*' {
		isBlockComment = true
	}
	if !isLineComment && !isBlockComment {
		return &LexError{
			Pos: l.pos,
			Err: fmt.Errorf("unexpected comment"),
		}
	}
Loop:
	for {
		l.pos++
		if l.pos >= len(l.input) {
			break
		}
		if isLineComment && (l.input[l.pos] == '\n' || l.input[l.pos] == '\r') {
			break Loop
		}
		if isBlockComment && l.input[l.pos] == '*' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '/' {
			l.pos += 2
			break Loop
		}
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindComment,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) lexSpace() error {
	start := l.pos
Loop:
	for {
		l.pos++
		if l.pos >= len(l.input) {
			break
		}
		switch l.input[l.pos] {
		case ' ', '\t':
			continue
		default:
			break Loop
		}
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindSpace,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) lexNewline() error {
	start := l.pos
Loop:
	for {
		l.pos++
		if l.pos >= len(l.input) {
			break
		}
		switch l.input[l.pos] {
		case '\n', '\r':
			continue
		default:
			break Loop
		}
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindNewline,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) lexString() error {
	start := l.pos
	quote := l.input[l.pos]
	l.pos++
Loop:
	for {
		if l.pos >= len(l.input) {
			break
		}
		switch l.input[l.pos] {
		case '\\':
			l.pos++
			continue
		case quote:
			l.pos++
			break Loop
		case '\n', '\r':
			return &LexError{
				Pos: l.pos,
				Err: fmt.Errorf("unexpected newline in string"),
			}
		default:
			l.pos++
			continue
		}
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindString,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) lexPlaceholder() error {
	start := l.pos
	l.pos++
	l.tokens = append(l.tokens, Token{
		Kind:  KindPlaceholder,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) lexNamedPlaceholder() error {
	start := l.pos
	if l.pos+1 >= len(l.input) {
		return &LexError{
			Pos: l.pos,
			Err: fmt.Errorf("unexpected end of input"),
		}
	}
	l.pos++
	value, err := l.extructIdentifier()
	if err != nil {
		return err
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindNamedPlaceholder,
		Value: l.input[start:start+1] + value,
	})
	return nil
}

func (l *Lexer) lexNumber() error {
	start := l.pos
Loop:
	for {
		l.pos++
		if l.pos >= len(l.input) {
			break
		}
		switch l.input[l.pos] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			continue
		default:
			break Loop
		}
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindNumber,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) lexSymbol() error {
	start := l.pos
	l.pos++
	l.tokens = append(l.tokens, Token{
		Kind:  KindSymbol,
		Value: l.input[start:l.pos],
	})
	return nil
}

func (l *Lexer) extructIdentifier() (string, error) {
	start := l.pos
Loop:
	for {
		l.pos++
		if l.pos >= len(l.input) {
			break
		}
		switch l.input[l.pos] {
		case ' ', '\t', '\n', '\r', '`', '"', '\'', '?', ':', '(', ')', ',', ';', '[', ']', '{', '}', '+', '-', '*', '/', '%', '^', '=', '<', '>', '&', '|', '~', '!':
			break Loop
		default:
			continue
		}
	}
	return l.input[start:l.pos], nil
}

func (l *Lexer) lexIdentifier() error {
	value, err := l.extructIdentifier()
	if err != nil {
		return err
	}
	l.tokens = append(l.tokens, Token{
		Kind:  KindIdentifier,
		Value: value,
	})
	return nil
}

type LexError struct {
	Pos int
	Err error
}

func (e *LexError) Error() string {
	return fmt.Sprintf("parse failed at %d: %s", e.Pos, e.Err)
}

func (e *LexError) Unwrap() error {
	return e.Err
}
