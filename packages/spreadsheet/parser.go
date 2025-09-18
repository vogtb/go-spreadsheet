package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type NodePosition struct {
	Start int
	End   int
}

// AST enables dependency extraction, formula transformation, and
// volatile function detection through tree traversal rather than
// regex/string manipulation.
type ASTNode interface {
	Eval(s *Spreadsheet) (Primitive, error)
	GetPosition() NodePosition
	ToString() string
}

// ParserContext provides context for parsing relative references
type ParserContext struct {
	CurrentWorksheetID uint32
	CurrentRow         int32
	CurrentColumn      int32
	ResolveWorksheet   func(name string) uint32
}

// Parser parses tokens into an AST
type Parser struct {
	tokens  []Token
	pos     int
	context *ParserContext
	lexer   *Lexer
}

// StringNode represents a string literal
type StringNode struct {
	Value    string
	Position NodePosition
}

func (n *StringNode) Eval(s *Spreadsheet) (Primitive, error) {
	return n.Value, nil
}

func (n *StringNode) GetPosition() NodePosition {
	return n.Position
}

func (n *StringNode) ToString() string {
	// Escape quotes in string
	escaped := strings.ReplaceAll(n.Value, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", escaped)
}

// NumberNode represents a numeric literal
type NumberNode struct {
	Value    float64
	Position NodePosition
}

func (n *NumberNode) Eval(s *Spreadsheet) (Primitive, error) {
	return n.Value, nil
}

func (n *NumberNode) GetPosition() NodePosition {
	return n.Position
}

func (n *NumberNode) ToString() string {
	// Format number without unnecessary decimals
	if n.Value == float64(int64(n.Value)) {
		return fmt.Sprintf("%d", int64(n.Value))
	}
	return fmt.Sprintf("%g", n.Value)
}

// BooleanNode represents a boolean literal
type BooleanNode struct {
	Value    bool
	Position NodePosition
}

func (n *BooleanNode) Eval(s *Spreadsheet) (Primitive, error) {
	return n.Value, nil
}

func (n *BooleanNode) GetPosition() NodePosition {
	return n.Position
}

func (n *BooleanNode) ToString() string {
	if n.Value {
		return "TRUE"
	}
	return "FALSE"
}

// CellRefNode represents a cell reference (relative)
type CellRefNode struct {
	WorksheetID uint32
	RowOffset   int32
	ColOffset   int32
	Position    NodePosition
}

func (n *CellRefNode) Eval(s *Spreadsheet) (Primitive, error) {
	// Calculate absolute address from relative offset
	currentAddr := s.GetCurrentAddress()
	targetRow := int32(currentAddr.Row) + n.RowOffset
	targetCol := int32(currentAddr.Column) + n.ColOffset

	if targetRow < 0 || targetCol < 0 {
		return nil, NewSpreadsheetError(ErrorCodeRef, "Invalid cell reference")
	}

	// Determine worksheet
	worksheetID := n.WorksheetID
	if worksheetID == 0 {
		worksheetID = currentAddr.WorksheetID
	}

	// Note: We don't calculate dependencies here anymore - they should already
	// be calculated by the time we evaluate them, thanks to deterministic ordering
	// in Calculate()

	// Get worksheet
	worksheet, exists := s.storage.worksheets.GetWorksheet(worksheetID)
	if !exists {
		return nil, NewSpreadsheetError(ErrorCodeRef, "Worksheet not found")
	}

	// Get cell value
	cell := worksheet.GetCell(uint32(targetRow), uint32(targetCol))
	if cell == nil {
		return nil, nil // Empty cell
	}

	// If it's a formula cell with a result, return the result
	if cell.FormulaID != 0 && cell.FormulaResultType != CellValueTypeEmpty {
		return cell.Value, nil
	}

	return cell.Value, nil
}

func (n *CellRefNode) GetPosition() NodePosition {
	return n.Position
}

func (n *CellRefNode) ToString() string {
	if n.WorksheetID != 0 {
		return fmt.Sprintf("WS_REF(%d,%d,%d)", n.WorksheetID, n.RowOffset, n.ColOffset)
	}
	return fmt.Sprintf("REF(%d,%d)", n.RowOffset, n.ColOffset)
}

// RangeNode represents a range of cells
type RangeNode struct {
	WorksheetID    uint32
	StartRowOffset int32
	StartColOffset int32
	EndRowOffset   int32
	EndColOffset   int32
	Position       NodePosition
}

func (n *RangeNode) Eval(s *Spreadsheet) (Primitive, error) {
	// calculate absolute range from relative offsets
	currentAddr := s.GetCurrentAddress()
	startRow := int32(currentAddr.Row) + n.StartRowOffset
	startCol := int32(currentAddr.Column) + n.StartColOffset
	endRow := int32(currentAddr.Row) + n.EndRowOffset
	endCol := int32(currentAddr.Column) + n.EndColOffset

	if startRow < 0 || startCol < 0 || endRow < 0 || endCol < 0 {
		return nil, NewSpreadsheetError(ErrorCodeRef, "Invalid range reference")
	}

	worksheetID := n.WorksheetID
	if worksheetID == 0 {
		worksheetID = currentAddr.WorksheetID
	}

	worksheet, exists := s.storage.worksheets.GetWorksheet(worksheetID)
	if !exists {
		return nil, NewSpreadsheetError(ErrorCodeRef, "Worksheet not found")
	}

	// normalize the range so start is always less than or equal to end
	normalizedStartRow := min(startRow, endRow)
	normalizedEndRow := max(startRow, endRow)
	normalizedStartCol := min(startCol, endCol)
	normalizedEndCol := max(startCol, endCol)

	// create and return a CellRange
	return &CellRange{
		worksheetID: worksheetID,
		startRow:    uint32(normalizedStartRow),
		startCol:    uint32(normalizedStartCol),
		endRow:      uint32(normalizedEndRow),
		endCol:      uint32(normalizedEndCol),
		worksheet:   worksheet,
		storage:     s.storage,
	}, nil
}

func (n *RangeNode) GetPosition() NodePosition {
	return n.Position
}

func (n *RangeNode) ToString() string {
	if n.WorksheetID != 0 {
		return fmt.Sprintf("WS_RANGE(%d,%d,%d,%d,%d)", n.WorksheetID,
			n.StartRowOffset, n.StartColOffset, n.EndRowOffset, n.EndColOffset)
	}
	return fmt.Sprintf("N_WS_RANGE(%d,%d,%d,%d)",
		n.StartRowOffset, n.StartColOffset, n.EndRowOffset, n.EndColOffset)
}

// NamedRangeNode represents a named range reference
type NamedRangeNode struct {
	Name     string
	Position NodePosition
}

func (n *NamedRangeNode) Eval(s *Spreadsheet) (Primitive, error) {
	// Look up named range
	nameID, exists := s.storage.namedRanges.GetNamedRangeID(n.Name)
	if !exists {
		return nil, NewSpreadsheetError(ErrorCodeName, fmt.Sprintf("Named range '%s' not found", n.Name))
	}

	// Get range address
	rangeAddr, exists := s.storage.namedRanges.GetRangeAddress(nameID)
	if !exists {
		return nil, NewSpreadsheetError(ErrorCodeName, fmt.Sprintf("Named range '%s' is not defined", n.Name))
	}

	// Get worksheet
	worksheet, exists := s.storage.worksheets.GetWorksheet(rangeAddr.WorksheetID)
	if !exists {
		return nil, NewSpreadsheetError(ErrorCodeRef, "Worksheet not found for named range")
	}

	// Return a CellRange for the named range
	return &CellRange{
		worksheetID: rangeAddr.WorksheetID,
		startRow:    rangeAddr.StartRow,
		startCol:    rangeAddr.StartColumn,
		endRow:      rangeAddr.EndRow,
		endCol:      rangeAddr.EndColumn,
		worksheet:   worksheet,
		storage:     s.storage,
	}, nil
}

func (n *NamedRangeNode) GetPosition() NodePosition {
	return n.Position
}

func (n *NamedRangeNode) ToString() string {
	return n.Name
}

// BinaryOpNode represents a binary operation
type BinaryOpNode struct {
	Op       BinaryOp
	Left     ASTNode
	Right    ASTNode
	Position NodePosition
}

func (n *BinaryOpNode) Eval(s *Spreadsheet) (Primitive, error) {
	// evaluate left and right operands
	// errors from evaluation are converted to error values
	leftVal, err := n.Left.Eval(s)
	if err != nil {
		// convert evaluation errors to error values
		if spreadsheetErr, ok := err.(*SpreadsheetError); ok {
			leftVal = spreadsheetErr
		} else {
			leftVal = NewSpreadsheetError(ErrorCodeValue, err.Error())
		}
	}

	rightVal, err := n.Right.Eval(s)
	if err != nil {
		// convert evaluation errors to error values
		if spreadsheetErr, ok := err.(*SpreadsheetError); ok {
			rightVal = spreadsheetErr
		} else {
			rightVal = NewSpreadsheetError(ErrorCodeValue, err.Error())
		}
	}

	// propagate errors
	if err, ok := leftVal.(*SpreadsheetError); ok {
		return err, nil
	}
	if err, ok := rightVal.(*SpreadsheetError); ok {
		return err, nil
	}

	switch n.Op {
	case BinOpAdd:
		// try numeric addition first
		if leftNum, leftOk := toNumber(leftVal); leftOk {
			if rightNum, rightOk := toNumber(rightVal); rightOk {
				return leftNum + rightNum, nil
			}
		}
		return nil, NewSpreadsheetError(ErrorCodeValue, "Addition requires numeric values")

	case BinOpSubtract:
		leftNum, leftOk := toNumber(leftVal)
		rightNum, rightOk := toNumber(rightVal)
		if !leftOk || !rightOk {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Subtraction requires numeric values")
		}
		return leftNum - rightNum, nil

	case BinOpMultiply:
		leftNum, leftOk := toNumber(leftVal)
		rightNum, rightOk := toNumber(rightVal)
		if !leftOk || !rightOk {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Multiplication requires numeric values")
		}
		return leftNum * rightNum, nil

	case BinOpDivide:
		leftNum, leftOk := toNumber(leftVal)
		rightNum, rightOk := toNumber(rightVal)
		if !leftOk || !rightOk {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Division requires numeric values")
		}
		if rightNum == 0 {
			return nil, NewSpreadsheetError(ErrorCodeDiv0, "Division by zero")
		}
		return leftNum / rightNum, nil

	case BinOpPower:
		leftNum, leftOk := toNumber(leftVal)
		rightNum, rightOk := toNumber(rightVal)
		if !leftOk || !rightOk {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Power requires numeric values")
		}
		return math.Pow(leftNum, rightNum), nil

	case BinOpConcat:
		return toString(leftVal) + toString(rightVal), nil

	case BinOpEqual:
		return comparePrimitives(leftVal, rightVal) == 0, nil

	case BinOpNotEqual:
		return comparePrimitives(leftVal, rightVal) != 0, nil

	case BinOpLess:
		cmp := comparePrimitives(leftVal, rightVal)
		if cmp == -2 {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Cannot compare these values")
		}
		return cmp < 0, nil

	case BinOpLessEqual:
		cmp := comparePrimitives(leftVal, rightVal)
		if cmp == -2 {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Cannot compare these values")
		}
		return cmp <= 0, nil

	case BinOpGreater:
		cmp := comparePrimitives(leftVal, rightVal)
		if cmp == -2 {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Cannot compare these values")
		}
		return cmp > 0, nil

	case BinOpGreaterEqual:
		cmp := comparePrimitives(leftVal, rightVal)
		if cmp == -2 {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Cannot compare these values")
		}
		return cmp >= 0, nil

	default:
		return nil, NewSpreadsheetError(ErrorCodeValue, "Unknown operator")
	}
}

func (n *BinaryOpNode) GetPosition() NodePosition {
	return n.Position
}

func (n *BinaryOpNode) ToString() string {
	opStr := ""
	switch n.Op {
	case BinOpAdd:
		opStr = "+"
	case BinOpSubtract:
		opStr = "-"
	case BinOpMultiply:
		opStr = "*"
	case BinOpDivide:
		opStr = "/"
	case BinOpModulo:
		opStr = "%"
	case BinOpPower:
		opStr = "^"
	case BinOpConcat:
		opStr = "&"
	case BinOpEqual:
		opStr = "="
	case BinOpNotEqual:
		opStr = "<>"
	case BinOpLess:
		opStr = "<"
	case BinOpLessEqual:
		opStr = "<="
	case BinOpGreater:
		opStr = ">"
	case BinOpGreaterEqual:
		opStr = ">="
	}
	return fmt.Sprintf("(%s%s%s)", n.Left.ToString(), opStr, n.Right.ToString())
}

// UnaryOpNode represents a unary operation
type UnaryOpNode struct {
	Op       UnaryOp
	Operand  ASTNode
	Position NodePosition
}

func (n *UnaryOpNode) Eval(s *Spreadsheet) (Primitive, error) {
	// Evaluate operand
	// Errors from evaluation are converted to error values
	val, err := n.Operand.Eval(s)
	if err != nil {
		// Convert evaluation errors to error values
		if spreadsheetErr, ok := err.(*SpreadsheetError); ok {
			val = spreadsheetErr
		} else {
			val = NewSpreadsheetError(ErrorCodeValue, err.Error())
		}
	}

	// Check for error in value and propagate it
	if err, ok := val.(*SpreadsheetError); ok {
		return err, nil
	}

	switch n.Op {
	case UnaryOpPlus:
		num, ok := toNumber(val)
		if !ok {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Unary plus requires a numeric value")
		}
		return num, nil

	case UnaryOpMinus:
		num, ok := toNumber(val)
		if !ok {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Negation requires a numeric value")
		}
		return -num, nil

	case UnaryOpPercent:
		num, ok := toNumber(val)
		if !ok {
			return nil, NewSpreadsheetError(ErrorCodeValue, "Percent requires a numeric value")
		}
		return num / 100.0, nil

	default:
		return nil, NewSpreadsheetError(ErrorCodeValue, "Unknown unary operator")
	}
}

func (n *UnaryOpNode) GetPosition() NodePosition {
	return n.Position
}

func (n *UnaryOpNode) ToString() string {
	opStr := ""
	switch n.Op {
	case UnaryOpPlus:
		opStr = "+"
	case UnaryOpMinus:
		opStr = "-"
	case UnaryOpPercent:
		return fmt.Sprintf("(%s%%)", n.Operand.ToString())
	}
	return fmt.Sprintf("%s%s", opStr, n.Operand.ToString())
}

// FunctionCallNode represents a function call
type FunctionCallNode struct {
	Name     string
	Args     []ASTNode
	Position NodePosition
}

func (n *FunctionCallNode) Eval(s *Spreadsheet) (Primitive, error) {
	// Evaluate arguments
	args := make([]any, len(n.Args))
	for i, argNode := range n.Args {
		argVal, err := argNode.Eval(s)
		if err != nil {
			// If the error is a SpreadsheetError, pass it as a value to the function
			// Functions will decide how to handle error values
			if spreadsheetErr, ok := err.(*SpreadsheetError); ok {
				args[i] = spreadsheetErr
			} else {
				// For non-SpreadsheetErrors (shouldn't happen), convert to SpreadsheetError
				args[i] = NewSpreadsheetError(ErrorCodeValue, err.Error())
			}
		} else {
			args[i] = argVal
		}
	}

	// Call built-in function
	result, err := s.functions.Call(n.Name, args...)
	if err != nil {
		// Convert regular error to SpreadsheetError if needed
		if spreadsheetErr, ok := err.(*SpreadsheetError); ok {
			return nil, spreadsheetErr
		}
		return nil, NewSpreadsheetError(ErrorCodeValue, err.Error())
	}

	return result, nil
}

func (n *FunctionCallNode) GetPosition() NodePosition {
	return n.Position
}

func (n *FunctionCallNode) ToString() string {
	args := make([]string, len(n.Args))
	for i, arg := range n.Args {
		args[i] = arg.ToString()
	}
	return fmt.Sprintf("%s(%s)", n.Name, strings.Join(args, ","))
}

// NewParser creates a new parser with the given tokens and context
func NewParser(tokens []Token, context *ParserContext) *Parser {
	return &Parser{
		tokens:  tokens,
		pos:     0,
		context: context,
		lexer:   nil,
	}
}

// NewParserWithContext creates a new parser with just context (for parsing
// individual components)
func NewParserWithContext(context *ParserContext) *Parser {
	return &Parser{
		tokens:  nil,
		pos:     0,
		context: context,
		lexer:   nil,
	}
}

// Parse parses the tokens into an AST
func (p *Parser) Parse() (ASTNode, error) {
	if len(p.tokens) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, "no tokens to parse")
	}

	// Expect and skip the equals prefix
	if p.tokens[p.pos].Type != TokenEquals {
		return nil, NewSpreadsheetError(ErrorCodeValue, "formula must start with '='")
	}
	p.pos++ // consume the equals token

	// Parse the expression
	node, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	// Ensure we've consumed all tokens except EOF
	if p.pos < len(p.tokens)-1 || (p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF) {
		// Check for cross-worksheet range syntax (Cell:Cell)
		if p.tokens[p.pos].Type == TokenColon {
			// Check if we just parsed a cell and the next token after colon is also a cell
			if _, isCellRefNode := node.(*CellRefNode); isCellRefNode && p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].Type == TokenCell {
				// This is an attempt to create a range with Cell:Cell syntax
				// which is not supported when cells have worksheet prefixes
				return nil, NewSpreadsheetError(ErrorCodeRef, "Cross-worksheet ranges are not supported")
			}
		}
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("unexpected token after expression: %s", p.tokens[p.pos].Value))
	}

	return node, nil
}

// parseComparison handles comparison operators (lowest precedence)
func (p *Parser) parseComparison() (ASTNode, error) {
	left, err := p.parseConcatenation()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) {
		tok := p.tokens[p.pos]
		if tok.Type != TokenBinaryOp {
			break
		}

		var op BinaryOp
		switch tok.Value {
		case "=":
			op = BinOpEqual
		case "<>", "!=":
			op = BinOpNotEqual
		case "<":
			op = BinOpLess
		case "<=":
			op = BinOpLessEqual
		case ">":
			op = BinOpGreater
		case ">=":
			op = BinOpGreaterEqual
		default:
			return left, nil
		}

		p.pos++
		right, err := p.parseConcatenation()
		if err != nil {
			return nil, err
		}

		left = &BinaryOpNode{
			Op:       op,
			Left:     left,
			Right:    right,
			Position: NodePosition{Start: left.GetPosition().Start, End: right.GetPosition().End},
		}
	}

	return left, nil
}

// parseConcatenation handles string concatenation operator
func (p *Parser) parseConcatenation() (ASTNode, error) {
	left, err := p.parseAddition()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) {
		tok := p.tokens[p.pos]
		if tok.Type != TokenBinaryOp || tok.Value != "&" {
			break
		}

		p.pos++
		right, err := p.parseAddition()
		if err != nil {
			return nil, err
		}

		left = &BinaryOpNode{
			Op:       BinOpConcat,
			Left:     left,
			Right:    right,
			Position: NodePosition{Start: left.GetPosition().Start, End: right.GetPosition().End},
		}
	}

	return left, nil
}

// parseAddition handles addition and subtraction
func (p *Parser) parseAddition() (ASTNode, error) {
	left, err := p.parseMultiplication()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) {
		tok := p.tokens[p.pos]
		if tok.Type != TokenBinaryOp {
			break
		}

		var op BinaryOp
		switch tok.Value {
		case "+":
			op = BinOpAdd
		case "-":
			op = BinOpSubtract
		default:
			return left, nil
		}

		p.pos++
		right, err := p.parseMultiplication()
		if err != nil {
			return nil, err
		}

		left = &BinaryOpNode{
			Op:       op,
			Left:     left,
			Right:    right,
			Position: NodePosition{Start: left.GetPosition().Start, End: right.GetPosition().End},
		}
	}

	return left, nil
}

// parseMultiplication handles multiplication, division, and modulo
func (p *Parser) parseMultiplication() (ASTNode, error) {
	left, err := p.parsePower()
	if err != nil {
		return nil, err
	}

	for p.pos < len(p.tokens) {
		tok := p.tokens[p.pos]
		if tok.Type != TokenBinaryOp {
			break
		}

		var op BinaryOp
		switch tok.Value {
		case "*":
			op = BinOpMultiply
		case "/":
			op = BinOpDivide
		case "%":
			// check if this is modulo or percent unary.
			// if the next token suggests it's postfix percent,
			// let parsePostfix handle it
			if p.pos+1 >= len(p.tokens) || !p.isValueToken(p.pos+1) {
				return left, nil
			}
			op = BinOpModulo
		default:
			return left, nil
		}

		p.pos++
		right, err := p.parsePower()
		if err != nil {
			return nil, err
		}

		left = &BinaryOpNode{
			Op:       op,
			Left:     left,
			Right:    right,
			Position: NodePosition{Start: left.GetPosition().Start, End: right.GetPosition().End},
		}
	}

	return left, nil
}

// parsePower handles exponentiation
func (p *Parser) parsePower() (ASTNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	// right-associative
	if p.pos < len(p.tokens) && p.tokens[p.pos].Type == TokenBinaryOp && p.tokens[p.pos].Value == "^" {
		p.pos++
		right, err := p.parsePower() // recursive for right-associativity
		if err != nil {
			return nil, err
		}

		return &BinaryOpNode{
			Op:       BinOpPower,
			Left:     left,
			Right:    right,
			Position: NodePosition{Start: left.GetPosition().Start, End: right.GetPosition().End},
		}, nil
	}

	return left, nil
}

// parseUnary handles unary operators
func (p *Parser) parseUnary() (ASTNode, error) {
	if p.pos >= len(p.tokens) {
		return nil, NewSpreadsheetError(ErrorCodeValue, "unexpected end of expression")
	}

	tok := p.tokens[p.pos]

	// check for unary operators
	if tok.Type == TokenUnaryPrefixOp {
		var op UnaryOp
		switch tok.Value {
		case "+":
			op = UnaryOpPlus
		case "-":
			op = UnaryOpMinus
		default:
			// not a unary operator, continue to parsePostfix
			return p.parsePostfix()
		}

		startPos := tok.Pos
		p.pos++
		operand, err := p.parseUnary() // recurse for chained unary operators
		if err != nil {
			return nil, err
		}

		return &UnaryOpNode{
			Op:       op,
			Operand:  operand,
			Position: NodePosition{Start: startPos, End: operand.GetPosition().End},
		}, nil
	}

	return p.parsePostfix()
}

// parsePostfix handles postfix operators (percent)
func (p *Parser) parsePostfix() (ASTNode, error) {
	node, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	// check for postfix percent
	if p.pos < len(p.tokens) && p.tokens[p.pos].Type == TokenUnaryPostfixOp && p.tokens[p.pos].Value == "%" {
		endPos := p.tokens[p.pos].Pos + 1
		p.pos++

		return &UnaryOpNode{
			Op:       UnaryOpPercent,
			Operand:  node,
			Position: NodePosition{Start: node.GetPosition().Start, End: endPos},
		}, nil
	}

	return node, nil
}

// parsePrimary handles primary expressions (literals, references,
// functions, parentheses)
func (p *Parser) parsePrimary() (ASTNode, error) {
	if p.pos >= len(p.tokens) {
		return nil, NewSpreadsheetError(ErrorCodeValue, "unexpected end of expression")
	}

	tok := p.tokens[p.pos]

	switch tok.Type {
	case TokenNumber:
		p.pos++
		val, err := strconv.ParseFloat(tok.Value, 64)
		if err != nil {
			return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("invalid number: %s", tok.Value))
		}
		return &NumberNode{
			Value:    val,
			Position: NodePosition{Start: tok.Pos, End: tok.Pos + len(tok.Value)},
		}, nil

	case TokenString:
		p.pos++
		return &StringNode{
			Value:    tok.Value,
			Position: NodePosition{Start: tok.Pos, End: tok.Pos + len(tok.Value) + 2}, // +2 for quotes
		}, nil

	case TokenBoolean:
		p.pos++
		value := tok.Value == "TRUE"
		return &BooleanNode{
			Value:    value,
			Position: NodePosition{Start: tok.Pos, End: tok.Pos + len(tok.Value)},
		}, nil

	case TokenCell:
		p.pos++
		return p.parseCellReference(tok)

	case TokenRange:
		p.pos++
		return p.parseRange(tok)

	case TokenIdentifier:
		p.pos++
		// could be a named range
		return &NamedRangeNode{
			Name:     tok.Value,
			Position: NodePosition{Start: tok.Pos, End: tok.Pos + len(tok.Value)},
		}, nil

	case TokenFunction:
		return p.parseFunctionCall()

	case TokenLeftParen:
		p.pos++
		node, err := p.parseComparison()
		if err != nil {
			return nil, err
		}

		if p.pos >= len(p.tokens) || p.tokens[p.pos].Type != TokenRightParen {
			return nil, NewSpreadsheetError(ErrorCodeValue, "expected closing parenthesis")
		}
		p.pos++

		return node, nil

	default:
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("unexpected token: %s", tok.Value))
	}
}

// parseFunctionCall parses a function call
func (p *Parser) parseFunctionCall() (ASTNode, error) {
	if p.pos >= len(p.tokens) || p.tokens[p.pos].Type != TokenFunction {
		return nil, NewSpreadsheetError(ErrorCodeName, "expected function name")
	}

	funcTok := p.tokens[p.pos]
	funcName := funcTok.Value
	startPos := funcTok.Pos
	p.pos++

	// expect opening parenthesis
	if p.pos >= len(p.tokens) || p.tokens[p.pos].Type != TokenLeftParen {
		return nil, NewSpreadsheetError(ErrorCodeValue, "expected '(' after function name")
	}
	p.pos++

	// parse arguments
	args := []ASTNode{}

	// check for empty argument list
	if p.pos < len(p.tokens) && p.tokens[p.pos].Type == TokenRightParen {
		p.pos++
		return &FunctionCallNode{
			Name:     funcName,
			Args:     args,
			Position: NodePosition{Start: startPos, End: p.tokens[p.pos-1].Pos + 1},
		}, nil
	}

	// parse arguments
	for {
		arg, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		if p.pos >= len(p.tokens) {
			return nil, NewSpreadsheetError(ErrorCodeValue, "unexpected end in function arguments")
		}

		if p.tokens[p.pos].Type == TokenRightParen {
			p.pos++
			break
		}

		if p.tokens[p.pos].Type != TokenComma {
			// check for cross-worksheet range syntax (Cell:Cell) in
			// function arguments
			if p.tokens[p.pos].Type == TokenColon {
				if _, isCellRefNode := arg.(*CellRefNode); isCellRefNode && p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].Type == TokenCell {
					// this is an attempt to create a range with Cell:Cell syntax
					// which is not supported when cells have worksheet prefixes
					return nil, NewSpreadsheetError(ErrorCodeRef, "Cross-worksheet ranges are not supported")
				}
			}
			return nil, NewSpreadsheetError(ErrorCodeValue, "expected ',' or ')' in function arguments")
		}
		p.pos++
	}

	return &FunctionCallNode{
		Name:     funcName,
		Args:     args,
		Position: NodePosition{Start: startPos, End: p.tokens[p.pos-1].Pos + 1},
	}, nil
}

// isValueToken checks if the token at position is a value token
func (p *Parser) isValueToken(pos int) bool {
	if pos >= len(p.tokens) {
		return false
	}

	switch p.tokens[pos].Type {
	case TokenNumber, TokenString, TokenBoolean, TokenCell, TokenRange,
		TokenIdentifier, TokenFunction, TokenLeftParen:
		return true
	case TokenUnaryPrefixOp:
		// Unary operators can start a value
		return p.tokens[pos].Value == "+" || p.tokens[pos].Value == "-"
	default:
		return false
	}
}

// parseCellReference parses a cell reference token into a CellRefNode
func (p *Parser) parseCellReference(tok Token) (ASTNode, error) {
	// extract worksheet info if present
	worksheetID := p.context.CurrentWorksheetID
	cellStr := tok.Value

	// check for worksheet reference (contains !)
	if idx := strings.Index(cellStr, "!"); idx != -1 {
		worksheetName := cellStr[:idx]
		cellStr = cellStr[idx+1:]

		// remove quotes if present
		if strings.HasPrefix(worksheetName, "'") && strings.HasSuffix(worksheetName, "'") {
			worksheetName = worksheetName[1 : len(worksheetName)-1]
		}

		// resolve worksheet name to ID
		if p.context.ResolveWorksheet != nil {
			worksheetID = p.context.ResolveWorksheet(worksheetName)
		}
	}

	// parse the cell reference
	col, row, err := p.parseCellAddress(cellStr)
	if err != nil {
		return nil, err
	}

	// calculate relative offsets
	rowOffset := row - p.context.CurrentRow
	colOffset := col - p.context.CurrentColumn

	return &CellRefNode{
		WorksheetID: worksheetID,
		RowOffset:   rowOffset,
		ColOffset:   colOffset,
		Position:    NodePosition{Start: tok.Pos, End: tok.Pos + len(tok.Value)},
	}, nil
}

// parseRange parses a range token into a RangeNode
func (p *Parser) parseRange(tok Token) (ASTNode, error) {
	// extract worksheet info if present
	worksheetID := p.context.CurrentWorksheetID
	rangeStr := tok.Value

	// check for worksheet reference (contains !)
	if idx := strings.Index(rangeStr, "!"); idx != -1 {
		worksheetName := rangeStr[:idx]
		rangeStr = rangeStr[idx+1:]

		// remove quotes if present
		if strings.HasPrefix(worksheetName, "'") && strings.HasSuffix(worksheetName, "'") {
			worksheetName = worksheetName[1 : len(worksheetName)-1]
		}

		// resolve worksheet name to ID
		if p.context.ResolveWorksheet != nil {
			worksheetID = p.context.ResolveWorksheet(worksheetName)
		}
	}

	// split the range
	parts := strings.Split(rangeStr, ":")
	if len(parts) != 2 {
		return nil, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("invalid range format: %s", rangeStr))
	}

	// parse start and end cells
	startCol, startRow, err := p.parseCellAddress(parts[0])
	if err != nil {
		return nil, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("invalid start cell in range: %s", parts[0]))
	}

	endCol, endRow, err := p.parseCellAddress(parts[1])
	if err != nil {
		return nil, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("invalid end cell in range: %s", parts[1]))
	}

	// calculate relative offsets
	startRowOffset := startRow - p.context.CurrentRow
	startColOffset := startCol - p.context.CurrentColumn
	endRowOffset := endRow - p.context.CurrentRow
	endColOffset := endCol - p.context.CurrentColumn

	return &RangeNode{
		WorksheetID:    worksheetID,
		StartRowOffset: startRowOffset,
		StartColOffset: startColOffset,
		EndRowOffset:   endRowOffset,
		EndColOffset:   endColOffset,
		Position:       NodePosition{Start: tok.Pos, End: tok.Pos + len(tok.Value)},
	}, nil
}

// parseCellAddress parses a cell address like "A1" into column and
// row indices (0-based)
func (p *Parser) parseCellAddress(cell string) (col int32, row int32, err error) {
	if len(cell) < 2 {
		return 0, 0, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("invalid cell reference: %s", cell))
	}

	// find where letters end and numbers begin
	letterEnd := 0
	for i, ch := range cell {
		if ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' {
			letterEnd = i + 1
		} else {
			break
		}
	}

	if letterEnd == 0 || letterEnd == len(cell) {
		return 0, 0, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("invalid cell reference: %s", cell))
	}

	// parse column (A=0, B=1, ..., Z=25, AA=26, AB=27, ...)
	colStr := strings.ToUpper(cell[:letterEnd])
	col = 0
	for i, ch := range colStr {
		col = col*26 + int32(ch-'A')
		if i < len(colStr)-1 {
			col++ // account for positional notation
		}
	}

	// parse row (1-based in notation, but we want 0-based)
	rowStr := cell[letterEnd:]
	rowNum, err := strconv.ParseInt(rowStr, 10, 32)
	if err != nil {
		return 0, 0, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("invalid row number: %s", rowStr))
	}

	if rowNum < 1 {
		return 0, 0, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("row number must be positive: %d", rowNum))
	}

	row = int32(rowNum - 1) // convert to 0-based

	return col, row, nil
}

// parseFullAddress parses a full cell address like "A1" or "Sheet1!B2". returns
// worksheet ID (0 for current/default), row and column indices (0-based)
func (p *Parser) parseFullAddress(address string) (worksheetID uint32, row int32, col int32, err error) {
	// use a context-aware lexer for references
	lexer := NewLexerForReference(address)
	tokens, lexErrors := lexer.Tokenize()
	if len(lexErrors) > 0 {
		return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("lexer errors: %v", lexErrors))
	}

	if len(tokens) == 0 {
		return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("no tokens found in address: %s", address))
	}

	token := tokens[0]

	// handle different token types
	switch token.Type {
	case TokenCell:
		// extract worksheet info if present
		cellStr := token.Value
		lastExclamation := strings.LastIndex(cellStr, "!")

		if lastExclamation != -1 {
			// worksheet reference found - resolve to ID
			worksheetName := cellStr[:lastExclamation]
			cellStr = cellStr[lastExclamation+1:]

			// remove quotes if present
			if strings.HasPrefix(worksheetName, "'") && strings.HasSuffix(worksheetName, "'") {
				worksheetName = worksheetName[1 : len(worksheetName)-1]
			}

			// resolve worksheet name to ID using context
			if p.context != nil && p.context.ResolveWorksheet != nil {
				worksheetID = p.context.ResolveWorksheet(worksheetName)
			} else {
				worksheetID = 0 // unknown worksheet
			}
		} else {
			// no worksheet reference - use current worksheet from context
			if p.context != nil {
				worksheetID = p.context.CurrentWorksheetID
			} else {
				worksheetID = 0 // no context, unknown worksheet
			}
		}

		// parse the cell part using existing parseCellAddress method
		col, row, err = p.parseCellAddress(cellStr)
		if err != nil {
			return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("invalid cell address in '%s': %v", address, err))
		}

		return worksheetID, row, col, nil

	case TokenRange:
		// for ranges, we'll parse the start cell
		rangeStr := token.Value
		lastExclamation := strings.LastIndex(rangeStr, "!")

		var cellPart string
		if lastExclamation != -1 {
			// worksheet reference found - resolve to ID
			worksheetName := rangeStr[:lastExclamation]
			cellPart = rangeStr[lastExclamation+1:]

			// remove quotes if present
			if strings.HasPrefix(worksheetName, "'") && strings.HasSuffix(worksheetName, "'") {
				worksheetName = worksheetName[1 : len(worksheetName)-1]
			}

			// resolve worksheet name to ID using context
			if p.context != nil && p.context.ResolveWorksheet != nil {
				worksheetID = p.context.ResolveWorksheet(worksheetName)
			} else {
				worksheetID = 0 // unknown worksheet
			}
		} else {
			// no worksheet reference - use current worksheet from context
			if p.context != nil {
				worksheetID = p.context.CurrentWorksheetID
			} else {
				worksheetID = 0 // no context, unknown worksheet
			}
		}

		// extract the start cell from the range (before the colon)
		colonIndex := strings.Index(cellPart, ":")
		if colonIndex == -1 {
			return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("invalid range format: %s", cellPart))
		}

		startCell := cellPart[:colonIndex]
		col, row, err = p.parseCellAddress(startCell)
		if err != nil {
			return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("invalid start cell in range '%s': %v", address, err))
		}

		return worksheetID, row, col, nil

	default:
		return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("address is not a valid cell reference or range: %s", address))
	}
}

// ParseRef parses a cell reference or range from a string. returns either
// a CellRefNode or RangeNode, or an error
func (p *Parser) ParseRef(input string) (ASTNode, error) {
	// Create a context-aware lexer for references
	lexer := NewLexerForReference(input)
	tokens, lexErrors := lexer.Tokenize()
	if len(lexErrors) > 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("lexer errors: %v", lexErrors))
	}

	if len(tokens) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("no tokens found in input: %s", input))
	}

	// check if it's a cell reference or range
	token := tokens[0]
	switch token.Type {
	case TokenCell:
		return p.parseCellReference(token)
	case TokenRange:
		return p.parseRange(token)
	default:
		return nil, NewSpreadsheetError(ErrorCodeRef, fmt.Sprintf("input is not a valid cell reference or range: %s", input))
	}
}

// ParseNumber parses a number from a string
func (p *Parser) ParseNumber(input string) (ASTNode, error) {
	// create a context-aware lexer for numbers
	lexer := NewLexerForNumber(input)
	tokens, lexErrors := lexer.Tokenize()
	if len(lexErrors) > 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("lexer errors: %v", lexErrors))
	}

	if len(tokens) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("no tokens found in input: %s", input))
	}

	// handle unary minus/plus
	value := 1.0
	tokenIndex := 0

	if len(tokens) >= 2 && tokens[0].Type == TokenUnaryPrefixOp {
		switch tokens[0].Value {
		case "-":
			value = -1.0
		case "+":
			value = 1.0
		default:
			return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("input is not a valid number: %s", input))
		}
		tokenIndex = 1
	}

	if tokenIndex >= len(tokens) {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("input is not a valid number: %s", input))
	}

	token := tokens[tokenIndex]
	if token.Type != TokenNumber {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("input is not a valid number: %s", input))
	}

	// parse the number value
	numberValue, err := strconv.ParseFloat(token.Value, 64)
	if err != nil {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("invalid number format: %s", token.Value))
	}

	finalValue := value * numberValue

	return &NumberNode{
		Value:    finalValue,
		Position: NodePosition{Start: tokens[0].Pos, End: token.Pos + len(token.Value)},
	}, nil
}

// ParseBoolean parses a boolean from a string
func (p *Parser) ParseBoolean(input string) (ASTNode, error) {
	// create a context-aware lexer for booleans
	lexer := NewLexerForBoolean(input)
	tokens, lexErrors := lexer.Tokenize()
	if len(lexErrors) > 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("lexer errors: %v", lexErrors))
	}

	if len(tokens) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("no tokens found in input: %s", input))
	}

	token := tokens[0]
	if token.Type != TokenBoolean {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("input is not a valid boolean: %s", input))
	}

	// parse the boolean value
	value := strings.ToUpper(token.Value) == "TRUE"

	return &BooleanNode{
		Value:    value,
		Position: NodePosition{Start: token.Pos, End: token.Pos + len(token.Value)},
	}, nil
}

// ParseString parses a string literal from a string
func (p *Parser) ParseString(input string) (ASTNode, error) {
	// create a context-aware lexer for strings
	lexer := NewLexerForString(input)
	tokens, lexErrors := lexer.Tokenize()
	if len(lexErrors) > 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("lexer errors: %v", lexErrors))
	}

	if len(tokens) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("no tokens found in input: %s", input))
	}

	token := tokens[0]
	if token.Type != TokenString {
		return nil, NewSpreadsheetError(ErrorCodeValue, fmt.Sprintf("input is not a valid string: %s", input))
	}

	return &StringNode{
		Value:    token.Value,
		Position: NodePosition{Start: token.Pos, End: token.Pos + len(token.Value)},
	}, nil
}

// comparePrimitives compares two primitive values. returns -1 if left < right,
// 0 if equal, 1 if left > right, -2 if not comparable
func comparePrimitives(left, right Primitive) int {
	// handle nil values
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return -1
	}
	if right == nil {
		return 1
	}

	// try numeric comparison first
	leftNum, leftIsNum := toNumber(left)
	rightNum, rightIsNum := toNumber(right)

	if leftIsNum && rightIsNum {
		if leftNum < rightNum {
			return -1
		} else if leftNum > rightNum {
			return 1
		}
		return 0
	}

	// try boolean comparison
	leftBool, leftIsBool := left.(bool)
	rightBool, rightIsBool := right.(bool)

	if leftIsBool && rightIsBool {
		if leftBool == rightBool {
			return 0
		} else if !leftBool && rightBool {
			return -1
		}
		return 1
	}

	// string comparison
	leftStr := toString(left)
	rightStr := toString(right)

	if leftStr < rightStr {
		return -1
	} else if leftStr > rightStr {
		return 1
	}
	return 0
}
