package main

// TokenType represents different types of tokens in formulas
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenEquals
	TokenNumber
	TokenString
	TokenBoolean
	TokenCell
	TokenRange
	TokenFunction
	TokenUnaryPrefixOp
	TokenUnaryPostfixOp
	TokenBinaryOp
	TokenComma
	TokenColon
	TokenLeftParen
	TokenRightParen
	TokenIdentifier
	TokenWhitespace
	TokenError
)

// BinaryOp represents binary operators in AST nodes
type BinaryOp int

const (
	BinOpAdd BinaryOp = iota
	BinOpSubtract
	BinOpMultiply
	BinOpDivide
	BinOpModulo
	BinOpPower
	BinOpConcat
	BinOpEqual
	BinOpNotEqual
	BinOpLess
	BinOpLessEqual
	BinOpGreater
	BinOpGreaterEqual
)

// UnaryOp represents unary operators in AST nodes
type UnaryOp int

const (
	UnaryOpPlus UnaryOp = iota
	UnaryOpMinus
	UnaryOpPercent
)

// character classification constants. slightly easier to read.
const (
	charNull       = 0
	charTab        = '\t'
	charNewline    = '\n'
	charReturn     = '\r'
	charSpace      = ' '
	charQuote      = '"'
	charApostrophe = '\''
	charPercent    = '%'
	charAmpersand  = '&'
	charLParen     = '('
	charRParen     = ')'
	charAsterisk   = '*'
	charPlus       = '+'
	charComma      = ','
	charMinus      = '-'
	charPeriod     = '.'
	charSlash      = '/'
	charColon      = ':'
	charLess       = '<'
	charEqual      = '='
	charGreater    = '>'
	charCaret      = '^'
	charUnderscore = '_'
	charExclaim    = '!'
)

// tokenTransitions maps the current state to valid next token types
var tokenTransitions = map[TokenState]map[TokenType]bool{
	StateStart: {
		TokenEquals:        true, // formula prefix
		TokenUnaryPrefixOp: true, // unary +/-
		TokenNumber:        true,
		TokenString:        true,
		TokenBoolean:       true,
		TokenCell:          true,
		TokenRange:         true, // allow ranges at start for standalone parsing
		TokenFunction:      true,
		TokenIdentifier:    true,
		TokenLeftParen:     true,
	},
	StateAfterValue: { // after number, string, cell, range
		TokenBinaryOp:       true,
		TokenUnaryPostfixOp: true, // for %
		TokenRightParen:     true,
		TokenComma:          true, // only if in function
		TokenEOF:            true,
		// whitespace is significant - no consecutive values
	},
	StateAfterOperator: {
		TokenNumber:        true,
		TokenString:        true,
		TokenBoolean:       true,
		TokenCell:          true,
		TokenFunction:      true,
		TokenIdentifier:    true,
		TokenLeftParen:     true,
		TokenUnaryPrefixOp: true, // only unary after binary
	},
	StateAfterLeftParen: {
		TokenNumber:        true,
		TokenString:        true,
		TokenBoolean:       true,
		TokenCell:          true,
		TokenRange:         true, // allow ranges in functions
		TokenFunction:      true,
		TokenIdentifier:    true,
		TokenLeftParen:     true, // nested
		TokenUnaryPrefixOp: true, // unary
		TokenRightParen:    true, // empty parens for arg-less functions like PI()
	},
	StateAfterRightParen: {
		TokenBinaryOp:       true,
		TokenUnaryPostfixOp: true, // for %
		TokenRightParen:     true, // if nested
		TokenComma:          true, // if in function
		TokenEOF:            true,
	},
	StateAfterComma: { // only valid in function context
		TokenNumber:        true,
		TokenString:        true,
		TokenBoolean:       true,
		TokenCell:          true,
		TokenRange:         true, // allow ranges in function arguments
		TokenFunction:      true,
		TokenIdentifier:    true,
		TokenLeftParen:     true,
		TokenUnaryPrefixOp: true, // unary
	},
	StateAfterColon: { // only after cell, expecting another cell
		TokenCell: true,
		// nothing else is valid
	},
	StateAfterIdentifier: {
		TokenLeftParen:      true, // function call
		TokenBinaryOp:       true, // named range used as value
		TokenUnaryPostfixOp: true, // for %
		TokenRightParen:     true, // if in parens
		TokenComma:          true, // if in function args
		TokenEOF:            true,
	},
	StateAfterEquals: {
		TokenNumber:        true,
		TokenString:        true,
		TokenBoolean:       true,
		TokenCell:          true,
		TokenRange:         true,
		TokenFunction:      true,
		TokenIdentifier:    true,
		TokenLeftParen:     true,
		TokenUnaryPrefixOp: true, // unary +/-
	},
}

// Token represents a lexical token with position information
type Token struct {
	Type  TokenType
	Value string
	Pos   int // byte position in input
}

// TokenState represents the lexer state for validation
type TokenState int

const (
	StateStart TokenState = iota
	StateAfterEquals
	StateAfterValue
	StateAfterOperator
	StateAfterLeftParen
	StateAfterRightParen
	StateAfterComma
	StateAfterColon
	StateAfterIdentifier
)

// Lexer tokenizes spreadsheet formula expressions
type Lexer struct {
	input      string
	runes      []rune // UTF-8 aware representation
	pos        int
	state      TokenState
	parenDepth int
	inString   bool
	tokens     []Token
	error      string
	context    *LexerContext
}

// LexerContext defines the context for lexing
type LexerContext struct {
	InitialState   TokenState
	ExpectedTokens map[TokenType]bool
	AllowEOF       bool
}

// NewLexer creates a new lexer for the given formula input (legacy method)
func NewLexer(input string) *Lexer {
	return NewLexerWithContext(input, &LexerContext{
		InitialState:   StateStart,
		ExpectedTokens: nil, // allow all tokens
		AllowEOF:       false,
	})
}

// NewLexerWithContext creates a new lexer with specific context
func NewLexerWithContext(input string, context *LexerContext) *Lexer {
	return &Lexer{
		input:   input,
		runes:   []rune(input), // runes for UTF-8 support. could do without but a real pain
		pos:     0,
		state:   context.InitialState,
		tokens:  []Token{},
		error:   "",
		context: context,
	}
}

// NewLexerForReference creates a lexer specifically for parsing cell
// references or ranges
func NewLexerForReference(input string) *Lexer {
	return NewLexerWithContext(input, &LexerContext{
		InitialState: StateStart,
		ExpectedTokens: map[TokenType]bool{
			TokenCell:  true,
			TokenRange: true,
		},
		AllowEOF: true,
	})
}

// NewLexerForNumber creates a lexer specifically for parsing numbers
func NewLexerForNumber(input string) *Lexer {
	return NewLexerWithContext(input, &LexerContext{
		InitialState: StateStart,
		ExpectedTokens: map[TokenType]bool{
			TokenUnaryPrefixOp: true, // for unary +/-
			TokenNumber:        true,
		},
		AllowEOF: true,
	})
}

// NewLexerForBoolean creates a lexer specifically for parsing booleans
func NewLexerForBoolean(input string) *Lexer {
	return NewLexerWithContext(input, &LexerContext{
		InitialState: StateStart,
		ExpectedTokens: map[TokenType]bool{
			TokenBoolean: true,
		},
		AllowEOF: true,
	})
}

// NewLexerForString creates a lexer specifically for parsing strings
func NewLexerForString(input string) *Lexer {
	return NewLexerWithContext(input, &LexerContext{
		InitialState: StateStart,
		ExpectedTokens: map[TokenType]bool{
			TokenString: true,
		},
		AllowEOF: true,
	})
}

// Tokenize tokenizes the entire input and returns tokens and any error
func (l *Lexer) Tokenize() ([]Token, []string) {
	// check if this is a specialized lexer (for individual values) or
	// full formula lexer
	if l.context != nil && l.context.ExpectedTokens != nil {
		// specialized lexer for individual values - don't expect = prefix
		l.pos = 0
	} else {
		// full formula lexer - must start with = and we tokenize it
		if len(l.runes) == 0 || l.runes[0] != '=' {
			l.error = "formula must start with '='"
			return nil, []string{l.error}
		}
		// start from beginning to tokenize the = as well
		l.pos = 0
	}

	// tokenize the rest
	for l.pos < len(l.runes) {
		tok := l.nextToken()
		if tok.Type == TokenError {
			l.error = tok.Value
			return nil, []string{l.error}
		}
		if tok.Type != TokenWhitespace {
			// validate state transition
			if !l.validateTransition(tok.Type) {
				l.error = "unexpected token: " + tok.Value
				return nil, []string{l.error}
			}
			l.tokens = append(l.tokens, tok)
			l.updateState(tok.Type)
		}
	}

	// check for unbalanced parentheses (only if no error already)
	if l.error == "" && l.parenDepth > 0 {
		l.error = "unbalanced parentheses: missing closing parenthesis"
		return nil, []string{l.error}
	} else if l.error == "" && l.parenDepth < 0 {
		l.error = "unbalanced parentheses: too many closing parentheses"
		return nil, []string{l.error}
	}

	// check for unclosed string (only if no error already)
	if l.error == "" && l.inString {
		l.error = "unclosed string literal"
		return nil, []string{l.error}
	}

	// add EOF token
	l.tokens = append(l.tokens, Token{Type: TokenEOF, Pos: l.pos})

	// Return empty error slice if successful
	if l.error == "" {
		return l.tokens, nil
	}
	return l.tokens, []string{l.error}
}

// validateTransition checks if the token type is valid in current state
func (l *Lexer) validateTransition(tokenType TokenType) bool {
	// check context-specific expected tokens first
	if l.context != nil && l.context.ExpectedTokens != nil && len(l.context.ExpectedTokens) > 0 {
		if !l.context.ExpectedTokens[tokenType] {
			return false
		}
		// for specialized lexers, if the token is in ExpectedTokens, it's valid
		// regardless of state transitions
		return true
	}

	// check state-based transitions for full formula lexers
	validTokens, exists := tokenTransitions[l.state]
	if !exists {
		return false
	}
	return validTokens[tokenType]
}

// updateState updates the lexer state based on the token type
func (l *Lexer) updateState(tokenType TokenType) {
	switch tokenType {
	case TokenEquals:
		l.state = StateAfterEquals
	case TokenNumber, TokenString, TokenBoolean, TokenCell:
		l.state = StateAfterValue
	case TokenRange:
		l.state = StateAfterValue
	case TokenUnaryPrefixOp, TokenBinaryOp:
		l.state = StateAfterOperator
	case TokenUnaryPostfixOp:
		// Postfix operators don't change state - they stay in current state
	case TokenLeftParen:
		l.state = StateAfterLeftParen
	case TokenRightParen:
		l.state = StateAfterRightParen
	case TokenComma:
		l.state = StateAfterComma
	case TokenColon:
		l.state = StateAfterColon
	case TokenIdentifier:
		l.state = StateAfterIdentifier
	case TokenFunction:
		l.state = StateAfterIdentifier
	}
}

// nextToken returns the next token from the input
func (l *Lexer) nextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.runes) {
		return Token{Type: TokenEOF, Pos: l.pos}
	}

	startPos := l.pos
	ch := l.current()

	// check for string literals
	if ch == charQuote {
		return l.scanString()
	}

	// check for single-quoted worksheet references
	if ch == charApostrophe {
		if tok := l.scanWorksheetRef(); tok.Type != TokenError {
			return tok
		}
	}

	// check for numbers
	if l.isDigit(ch) || (ch == charPeriod && l.pos+1 < len(l.input) && l.isDigit(rune(l.runes[l.pos+1]))) {
		return l.scanNumber()
	}

	// check for operators and special characters
	switch ch {
	case charLParen:
		l.pos++
		l.parenDepth++
		return Token{Type: TokenLeftParen, Value: "(", Pos: startPos}
	case charRParen:
		l.pos++
		l.parenDepth--
		if l.parenDepth < 0 {
			return Token{Type: TokenError, Value: "unexpected closing parenthesis", Pos: startPos}
		}
		return Token{Type: TokenRightParen, Value: ")", Pos: startPos}
	case charComma:
		l.pos++
		return Token{Type: TokenComma, Value: ",", Pos: startPos}
	case charColon:
		l.pos++
		return Token{Type: TokenColon, Value: ":", Pos: startPos}
	case charPlus, charMinus:
		return l.scanUnaryPrefixOrBinaryOp()
	case charAsterisk, charSlash, charCaret, charAmpersand:
		return l.scanBinaryOp()
	case charPercent:
		return l.scanUnaryPostfixOp()
	case charEqual:
		// distinguish between formula prefix = and comparison operator =
		if l.pos == 0 {
			// first character is the formula prefix
			l.pos++
			return Token{Type: TokenEquals, Value: "=", Pos: startPos}
		} else {
			// comparison operator
			l.pos++
			return Token{Type: TokenBinaryOp, Value: "=", Pos: startPos}
		}
	case charLess, charGreater:
		return l.scanBinaryOp()
	case charExclaim:
		// could be part of a worksheet reference
		if l.pos > 0 {
			return Token{Type: TokenUnaryPrefixOp, Value: "!", Pos: startPos}
		}
		return l.scanBinaryOp()
	}

	// check for identifiers, functions, cells, booleans
	if l.isAlpha(ch) || ch == charUnderscore {
		return l.scanIdentifierOrCell()
	}

	// unknown character
	l.pos++
	return Token{Type: TokenError, Value: "unexpected character: " + string(ch), Pos: startPos}
}

// helper methods for character navigation and classification

// substring returns a substring of the original input based on rune positions
func (l *Lexer) substring(start, end int) string {
	if start < 0 || end > len(l.runes) || start > end {
		return ""
	}
	return string(l.runes[start:end])
}

func (l *Lexer) current() rune {
	if l.pos >= len(l.runes) {
		return charNull
	}
	return l.runes[l.pos]
}

func (l *Lexer) peek(offset int) rune {
	pos := l.pos + offset
	if pos >= len(l.runes) || pos < 0 {
		return charNull
	}
	return l.runes[pos]
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.runes) {
		ch := l.current()
		if ch == charSpace || ch == charTab || ch == charNewline || ch == charReturn {
			l.pos++
		} else {
			break
		}
	}
}

func (l *Lexer) isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func (l *Lexer) isAlpha(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func (l *Lexer) isAlphaNumeric(ch rune) bool {
	return l.isAlpha(ch) || l.isDigit(ch)
}

// scanNumber scans a number token including decimals and scientific notation
func (l *Lexer) scanNumber() Token {
	startPos := l.pos

	// scan integer part
	for l.pos < len(l.runes) && l.isDigit(l.current()) {
		l.pos++
	}

	// check for decimal part
	if l.current() == charPeriod && l.pos+1 < len(l.input) && l.isDigit(l.peek(1)) {
		l.pos++ // consume '.'
		for l.pos < len(l.runes) && l.isDigit(l.current()) {
			l.pos++
		}
	}

	// check for scientific notation (e or E)
	if l.current() == 'e' || l.current() == 'E' {
		savedPos := l.pos
		l.pos++ // consume 'e' or 'E'

		// optional + or - sign
		if l.current() == charPlus || l.current() == charMinus {
			l.pos++
		}

		// must have at least one digit after e/E
		if !l.isDigit(l.current()) {
			// not scientific notation, restore position
			l.pos = savedPos
		} else {
			// scan exponent digits
			for l.pos < len(l.runes) && l.isDigit(l.current()) {
				l.pos++
			}
		}
	}

	value := l.substring(startPos, l.pos)
	return Token{Type: TokenNumber, Value: value, Pos: startPos}
}

// scanString scans a string literal with support for double-quote escapes
func (l *Lexer) scanString() Token {
	startPos := l.pos
	l.pos++ // consume opening quote
	l.inString = true

	var result []rune

	for l.pos < len(l.runes) {
		ch := l.current()

		if ch == charQuote {
			// check if it's an escape sequence (double quote)
			if l.peek(1) == charQuote {
				result = append(result, charQuote)
				l.pos += 2 // consume both quotes
			} else {
				// EOS
				l.pos++ // consume closing quote
				l.inString = false
				return Token{Type: TokenString, Value: string(result), Pos: startPos}
			}
		} else {
			result = append(result, ch)
			l.pos++
		}
	}

	// enclosed string
	l.inString = false
	return Token{Type: TokenError, Value: "unclosed string literal", Pos: startPos}
}

// scanIdentifierOrCell scans identifiers, functions, cells, ranges, and booleans
func (l *Lexer) scanIdentifierOrCell() Token {
	startPos := l.pos

	// first, collect the identifier part
	for l.pos < len(l.runes) && (l.isAlphaNumeric(l.current()) || l.current() == charUnderscore) {
		l.pos++
	}

	value := l.substring(startPos, l.pos)
	upperValue := l.toUpper(value)

	// check for boolean literals
	if upperValue == "TRUE" || upperValue == "FALSE" {
		return Token{Type: TokenBoolean, Value: upperValue, Pos: startPos}
	}

	// check if it's a worksheet reference (identifier followed by !)
	if l.current() == charExclaim {
		// this is a worksheet name, scan the rest as worksheet reference
		return l.scanWorksheetRefWithName(startPos)
	}

	// check if it's a cell reference
	if l.isCell(value) {
		// check for range (A1:B2)
		if l.current() == charColon {
			savedPos := l.pos
			l.pos++ // consume ':'

			// try to scan another cell
			cellStart := l.pos
			for l.pos < len(l.runes) && (l.isAlphaNumeric(l.current())) {
				l.pos++
			}

			secondCell := l.substring(cellStart, l.pos)
			if l.isCell(secondCell) {
				// is range
				rangeValue := l.substring(startPos, l.pos)
				return Token{Type: TokenRange, Value: rangeValue, Pos: startPos}
			} else {
				// not  valid range, restore position and return just the cell
				l.pos = savedPos
				return Token{Type: TokenCell, Value: value, Pos: startPos}
			}
		}
		return Token{Type: TokenCell, Value: value, Pos: startPos}
	}

	// check if it's a function (followed by open paren)
	if l.current() == charLParen {
		return Token{Type: TokenFunction, Value: upperValue, Pos: startPos}
	}

	// it's an identifier (possibly a named range)
	return Token{Type: TokenIdentifier, Value: value, Pos: startPos}
}

// isCell checks if a string is a valid cell reference (e.g., A1, B12)
func (l *Lexer) isCell(s string) bool {
	if len(s) < 2 {
		return false
	}

	// find where letters end and numbers begin
	letterEnd := 0
	for i, ch := range s {
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' {
			letterEnd = i + 1
		} else {
			break
		}
	}

	// must have at least one letter and one digit
	if letterEnd == 0 || letterEnd == len(s) {
		return false
	}

	// check remaining characters are all digits
	for i := letterEnd; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// toUpper converts a string to uppercase
func (l *Lexer) toUpper(s string) string {
	result := make([]rune, len(s))
	for i, ch := range s {
		if ch >= 'a' && ch <= 'z' {
			result[i] = ch - 32
		} else {
			result[i] = ch
		}
	}
	return string(result)
}

// scanWorksheetRef scans a worksheet reference starting with single quote
func (l *Lexer) scanWorksheetRef() Token {
	startPos := l.pos

	if l.current() != charApostrophe {
		return Token{Type: TokenError, Value: "expected single quote", Pos: startPos}
	}

	l.pos++ // consume opening single quote

	// scan until we find closing single quote
	for l.pos < len(l.runes) && l.current() != charApostrophe {
		l.pos++
	}

	if l.pos >= len(l.runes) {
		return Token{Type: TokenError, Value: "unclosed worksheet name", Pos: startPos}
	}

	l.pos++ // consume closing single quote

	// must be followed by !
	if l.current() != charExclaim {
		// not worksheet reference, could be just a string
		l.pos = startPos
		return Token{Type: TokenError, Value: "not a worksheet reference", Pos: startPos}
	}

	l.pos++ // consume !

	// now scan the cell or range reference
	cellStart := l.pos
	for l.pos < len(l.runes) && (l.isAlphaNumeric(l.current())) {
		l.pos++
	}

	cellRef := l.substring(cellStart, l.pos)
	if !l.isCell(cellRef) {
		return Token{Type: TokenError, Value: "invalid cell reference after worksheet", Pos: startPos}
	}

	// check for range
	if l.current() == charColon {
		l.pos++ // consume ':'
		rangeStart := l.pos
		for l.pos < len(l.runes) && (l.isAlphaNumeric(l.current())) {
			l.pos++
		}

		secondCell := l.substring(rangeStart, l.pos)
		if l.isCell(secondCell) {
			// worksheet _range_ reference
			fullRef := l.substring(startPos, l.pos)
			return Token{Type: TokenRange, Value: fullRef, Pos: startPos}
		} else {
			return Token{Type: TokenError, Value: "invalid range reference", Pos: startPos}
		}
	}

	// worksheet _cell_ reference
	fullRef := l.substring(startPos, l.pos)
	return Token{Type: TokenCell, Value: fullRef, Pos: startPos}
}

// scanWorksheetRefWithName scans worksheet reference when we already have
// the sheet name
func (l *Lexer) scanWorksheetRefWithName(startPos int) Token {
	if l.current() != charExclaim {
		return Token{Type: TokenError, Value: "expected ! after worksheet name", Pos: startPos}
	}

	l.pos++ // consume !

	// scan the cell or range reference
	cellStart := l.pos
	for l.pos < len(l.runes) && (l.isAlphaNumeric(l.current())) {
		l.pos++
	}

	cellRef := l.substring(cellStart, l.pos)
	if !l.isCell(cellRef) {
		return Token{Type: TokenError, Value: "invalid cell reference after worksheet", Pos: startPos}
	}

	// check for range
	if l.current() == charColon {
		l.pos++ // consume ':'
		rangeStart := l.pos
		for l.pos < len(l.runes) && (l.isAlphaNumeric(l.current())) {
			l.pos++
		}

		secondCell := l.substring(rangeStart, l.pos)
		if l.isCell(secondCell) {
			// worksheet range reference
			fullRef := l.substring(startPos, l.pos)
			return Token{Type: TokenRange, Value: fullRef, Pos: startPos}
		} else {
			return Token{Type: TokenError, Value: "invalid range reference", Pos: startPos}
		}
	}

	// worksheet cell reference
	fullRef := l.substring(startPos, l.pos)
	return Token{Type: TokenCell, Value: fullRef, Pos: startPos}
}

// scanUnaryPrefixOrBinaryOp scans + and - which can be either unary
// prefix or binary
func (l *Lexer) scanUnaryPrefixOrBinaryOp() Token {
	startPos := l.pos
	ch := l.current()
	l.pos++

	if l.isUnaryContext() {
		return Token{Type: TokenUnaryPrefixOp, Value: string(ch), Pos: startPos}
	}
	return Token{Type: TokenBinaryOp, Value: string(ch), Pos: startPos}
}

// scanBinaryOp scans binary operators
func (l *Lexer) scanBinaryOp() Token {
	startPos := l.pos
	ch := l.current()

	// check for two-character operators first
	if ch == charLess {
		l.pos++
		if l.current() == charEqual {
			l.pos++
			return Token{Type: TokenBinaryOp, Value: "<=", Pos: startPos}
		} else if l.current() == charGreater {
			l.pos++
			return Token{Type: TokenBinaryOp, Value: "<>", Pos: startPos}
		}
		return Token{Type: TokenBinaryOp, Value: "<", Pos: startPos}
	}

	if ch == charGreater {
		l.pos++
		if l.current() == charEqual {
			l.pos++
			return Token{Type: TokenBinaryOp, Value: ">=", Pos: startPos}
		}
		return Token{Type: TokenBinaryOp, Value: ">", Pos: startPos}
	}

	// handle != as not equal
	if ch == charExclaim {
		l.pos++
		if l.current() == charEqual {
			l.pos++
			return Token{Type: TokenBinaryOp, Value: "!=", Pos: startPos}
		}
		// single ! is not a valid operator in our context (except for worksheet refs)
		l.pos = startPos
		return Token{Type: TokenError, Value: "unexpected '!'", Pos: startPos}
	}

	// single character binary operators
	switch ch {
	case charAsterisk:
		l.pos++
		return Token{Type: TokenBinaryOp, Value: "*", Pos: startPos}
	case charSlash:
		l.pos++
		return Token{Type: TokenBinaryOp, Value: "/", Pos: startPos}
	case charCaret:
		l.pos++
		return Token{Type: TokenBinaryOp, Value: "^", Pos: startPos}
	case charAmpersand:
		l.pos++
		return Token{Type: TokenBinaryOp, Value: "&", Pos: startPos}
	}

	return Token{Type: TokenError, Value: "unknown operator", Pos: startPos}
}

// scanUnaryPostfixOp scans postfix operators like %
func (l *Lexer) scanUnaryPostfixOp() Token {
	startPos := l.pos
	ch := l.current()
	l.pos++
	return Token{Type: TokenUnaryPostfixOp, Value: string(ch), Pos: startPos}
}

// isUnaryContext checks if the current context allows for unary operators
func (l *Lexer) isUnaryContext() bool {
	// unary operators are allowed after:
	// - start of expression
	// - after equals (=)
	// - after another operator
	// - after left paren
	// - after comma
	switch l.state {
	case StateStart, StateAfterEquals, StateAfterOperator, StateAfterLeftParen, StateAfterComma:
		return true
	default:
		return false
	}
}
