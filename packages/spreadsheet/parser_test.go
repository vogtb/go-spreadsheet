package main

import (
	"testing"
)

func createTestParser() *Parser {
	context := &ParserContext{
		CurrentWorksheetID: 1,
		CurrentRow:         0,
		CurrentColumn:      0,
		ResolveWorksheet: func(name string) uint32 {
			switch name {
			case "Sheet1":
				return 1
			case "Sheet2":
				return 2
			case "Sheet3":
				return 3
			default:
				return 0
			}
		},
	}
	return NewParser([]Token{}, context)
}

func parseFormula(formula string) bool {
	lexer := NewLexer(formula)
	tokens, lexErrors := lexer.Tokenize()

	if len(lexErrors) > 0 {
		return false
	}

	if len(tokens) == 0 {
		return false
	}

	parser := createTestParser()
	parser.tokens = tokens
	_, err := parser.Parse()
	return err == nil
}

func TestParserBasicFormulas(t *testing.T) {
	validFormulas := []string{
		"=1+2",
		"=A1",
		"=SUM(A1:A10)",
		"=Sheet2!A1",
		"=Sheet2!A1:B2",
		"=SUM(Sheet2!A1:A10)",
		"=Sheet2!A1 + Sheet3!B1",
		"=SUM(B2:A1)",
		"=SUM(A1:A1)",
		"=SUM(A1:Z1000)",
		`="Hello 世界"`,
		`="Test 😀 emoji"`,
		`=CONCATENATE("Hello ", "世界")`,
	}

	for _, formula := range validFormulas {
		t.Run(formula, func(t *testing.T) {
			if !parseFormula(formula) {
				t.Errorf("Failed to parse valid formula: %s", formula)
			}
		})
	}
}

func TestParserInvalidFormulas(t *testing.T) {
	invalidFormulas := []string{
		"=",
		"=SUM(",
		"=A1:",
		`="hello`,
	}

	for _, formula := range invalidFormulas {
		t.Run(formula, func(t *testing.T) {
			if parseFormula(formula) {
				t.Errorf("Expected formula to fail but it succeeded: %s", formula)
			}
		})
	}
}
