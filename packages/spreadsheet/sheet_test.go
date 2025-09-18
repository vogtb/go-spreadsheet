package main

import (
	"fmt"
	"math"
	"testing"
)

type SpreadsheetTestCase struct {
	t           *testing.T
	name        string
	spreadsheet *Spreadsheet
	err         error
	skipped     bool
}

func NewSpreadsheetTestCase(t *testing.T, name string) *SpreadsheetTestCase {
	tc := &SpreadsheetTestCase{
		t:           t,
		name:        name,
		spreadsheet: NewSpreadsheet(),
		err:         nil,
		skipped:     false,
	}
	return tc.AddWorksheet("Sheet1")
}

func (tc *SpreadsheetTestCase) Skip(reason string) *SpreadsheetTestCase {
	if !tc.skipped {
		tc.t.Skipf("%s: %s", tc.name, reason)
		tc.skipped = true
	}
	return tc
}

func (tc *SpreadsheetTestCase) Set(address string, value Primitive) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.Set(address, value)
	if tc.err != nil {
		tc.t.Errorf("%s: Set(%s) failed: %v", tc.name, address, tc.err)
	}
	return tc
}

func (tc *SpreadsheetTestCase) Remove(address string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.Remove(address)
	if tc.err != nil {
		tc.t.Errorf("%s: Remove(%s) failed: %v", tc.name, address, tc.err)
	}
	return tc
}

func (tc *SpreadsheetTestCase) AddWorksheet(name string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.AddWorksheet(name)
	return tc
}

func (tc *SpreadsheetTestCase) RemoveWorksheet(name string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.RemoveWorksheet(name)
	return tc
}

func (tc *SpreadsheetTestCase) RenameWorksheet(oldName, newName string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.RenameWorksheet(oldName, newName)
	return tc
}

func (tc *SpreadsheetTestCase) AddNamedRange(name string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.AddNamedRange(name)
	return tc
}

func (tc *SpreadsheetTestCase) RemoveNamedRange(name string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.RemoveNamedRange(name)
	return tc
}

func (tc *SpreadsheetTestCase) RenameNamedRange(oldName, newName string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.RenameNamedRange(oldName, newName)
	return tc
}

func (tc *SpreadsheetTestCase) Run() *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.Calculate()
	if tc.err != nil {
		tc.t.Errorf("%s: Calculate() failed: %v", tc.name, tc.err)
	}
	return tc
}

func (tc *SpreadsheetTestCase) RunAndAssertNoError() *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	tc.err = tc.spreadsheet.Calculate()
	if tc.err != nil {
		tc.t.Errorf("%s: Calculate() failed: %v", tc.name, tc.err)
		return tc
	}
	return tc
}

func (tc *SpreadsheetTestCase) AssertCellEq(address string, expected Primitive) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	actual, err := tc.spreadsheet.Get(address)
	if err != nil {
		tc.t.Errorf("%s: Get(%s) failed: %v", tc.name, address, err)
		return tc
	}

	switch exp := expected.(type) {
	case float64:
		if act, ok := actual.(float64); ok {
			if math.Abs(act-exp) > 1e-10 {
				tc.t.Errorf("%s: Cell %s = %v, want %v", tc.name, address, actual, expected)
			}
		} else {
			tc.t.Errorf("%s: Cell %s = %v (%T), want %v (float64)", tc.name, address, actual, actual, expected)
		}
	case int:
		// Convert int to float64 for comparison
		if act, ok := actual.(float64); ok {
			if math.Abs(act-float64(exp)) > 1e-10 {
				tc.t.Errorf("%s: Cell %s = %v, want %v", tc.name, address, actual, expected)
			}
		} else {
			tc.t.Errorf("%s: Cell %s = %v (%T), want %v (int)", tc.name, address, actual, actual, expected)
		}
	case nil:
		if actual != nil {
			tc.t.Errorf("%s: Cell %s = %v, want nil", tc.name, address, actual)
		}
	case ErrorCode:
		if spreadsheetErr, ok := actual.(*SpreadsheetError); ok {
			if spreadsheetErr.ErrorCode != exp {
				tc.t.Errorf("%s: Cell %s has error %v, want %v", tc.name, address, spreadsheetErr.ErrorCode, exp)
			}
		} else {
			tc.t.Errorf("%s: Cell %s = %v, want error %v", tc.name, address, actual, exp)
		}
	default:
		if actual != expected {
			tc.t.Errorf("%s: Cell %s = %v, want %v", tc.name, address, actual, expected)
		}
	}
	return tc
}

func (tc *SpreadsheetTestCase) AssertCellEmpty(address string) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	actual, err := tc.spreadsheet.Get(address)
	if err != nil {
		tc.t.Errorf("%s: Get(%s) failed: %v", tc.name, address, err)
		return tc
	}

	if actual != nil {
		tc.t.Errorf("%s: Cell %s = %v, want nil", tc.name, address, actual)
	}
	return tc
}

func (tc *SpreadsheetTestCase) AssertCellErr(address string, errorCode ErrorCode) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err != nil {
		return tc
	}
	actual, err := tc.spreadsheet.Get(address)
	if err != nil {
		tc.t.Errorf("%s: Get(%s) failed: %v", tc.name, address, err)
		return tc
	}

	if spreadsheetErr, ok := actual.(*SpreadsheetError); ok {
		if spreadsheetErr.ErrorCode != errorCode {
			tc.t.Errorf("%s: Cell %s has error %v, want %v", tc.name, address, spreadsheetErr.ErrorCode, errorCode)
		}
	} else {
		tc.t.Errorf("%s: Cell %s = %v, want error %v", tc.name, address, actual, errorCode)
	}
	return tc
}

func (tc *SpreadsheetTestCase) AssertCellFn(address string, fn func(value Primitive, t *testing.T)) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	actual, err := tc.spreadsheet.Get(address)
	if err != nil {
		tc.t.Errorf("%s: Get(%s) failed: %v", tc.name, address, err)
		return tc
	}
	fn(actual, tc.t)
	return tc
}

func (tc *SpreadsheetTestCase) AssertWorksheetExists(name string, shouldExist bool) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	exists := tc.spreadsheet.DoesWorksheetExist(name)
	if exists != shouldExist {
		tc.t.Errorf("%s: Worksheet %s exists=%v, want %v", tc.name, name, exists, shouldExist)
	}
	return tc
}

func (tc *SpreadsheetTestCase) AssertNamedRangeExists(name string, shouldExist bool) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	exists := tc.spreadsheet.DoesNamedRangeExist(name)
	if exists != shouldExist {
		tc.t.Errorf("%s: Named range %s exists=%v, want %v", tc.name, name, exists, shouldExist)
	}
	return tc
}

func (tc *SpreadsheetTestCase) ExpectAppError(expectedCode AppErrorCode) *SpreadsheetTestCase {
	if tc.skipped {
		return tc
	}
	if tc.err == nil {
		tc.t.Errorf("%s: Expected error with code %v, but got no error", tc.name, expectedCode)
		return tc
	}
	if appErr, ok := tc.err.(*AppError); ok {
		if appErr.Code != expectedCode {
			tc.t.Errorf("%s: Got error code %v, want %v", tc.name, appErr.Code, expectedCode)
		}
	} else {
		tc.t.Errorf("%s: Got error %v, want AppError with code %v", tc.name, tc.err, expectedCode)
	}
	tc.err = nil
	return tc
}

func (tc *SpreadsheetTestCase) End() {
}

func TestLexingAndParsing(t *testing.T) {
	t.Run("ValidFormulas", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Basic arithmetic").
			Set("Sheet1!A1", "=1+2").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3.0).
			End()

		NewSpreadsheetTestCase(t, "Cell reference").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A2", 10.0).
			End()

		NewSpreadsheetTestCase(t, "Function call").
			Set("Sheet1!A1", 5.0).
			Set("Sheet1!A2", 10.0).
			Set("Sheet1!A3", "=SUM(A1:A2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A3", 15.0).
			End()

		NewSpreadsheetTestCase(t, "String literal").
			Set("Sheet1!A1", `="hello"`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "hello").
			End()

		NewSpreadsheetTestCase(t, "Boolean literal").
			Set("Sheet1!A1", "=TRUE").
			Set("Sheet1!A2", "=FALSE").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			AssertCellEq("Sheet1!A2", false).
			End()

		NewSpreadsheetTestCase(t, "Worksheet reference").
			AddWorksheet("Sheet2").
			Set("Sheet2!A1", 42.0).
			Set("Sheet1!A1", "=Sheet2!A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 42.0).
			End()
	})

	t.Run("InvalidFormulas", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Empty formula").
			Set("Sheet1!A1", "=").
			Run().
			AssertCellErr("Sheet!A1", ErrorCodeValue).
			End()

		NewSpreadsheetTestCase(t, "Multiple unary plus operator").
			Set("Sheet1!A1", "=1++2").
			Set("Sheet1!A2", "=1++++++3").
			Set("Sheet1!A3", "=++++1++++++4").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3).
			AssertCellEq("Sheet1!A2", 4).
			AssertCellEq("Sheet1!A3", 5).
			End()

		NewSpreadsheetTestCase(t, "Unclosed function").
			Set("Sheet1!A1", "=SUM(").
			Run().
			AssertCellErr("Sheet!A1", ErrorCodeValue).
			End()

		NewSpreadsheetTestCase(t, "Incomplete range").
			Set("Sheet1!A1", "=A1:").
			Run().
			AssertCellErr("Sheet!A1", ErrorCodeValue).
			End()

		NewSpreadsheetTestCase(t, "Unterminated string").
			Set("Sheet1!A1", `="hello`).
			Run().
			AssertCellErr("Sheet!A1", ErrorCodeValue).
			End()
	})
}

func TestBasicTypes(t *testing.T) {
	t.Run("Numbers", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Integer").
			Set("Sheet1!A1", 42.0).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 42.0).
			End()

		NewSpreadsheetTestCase(t, "Float").
			Set("Sheet1!A1", 3.14159).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3.14159).
			End()

		NewSpreadsheetTestCase(t, "Negative").
			Set("Sheet1!A1", -123.45).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", -123.45).
			End()

		NewSpreadsheetTestCase(t, "Scientific notation").
			Set("Sheet1!A1", "=1.23E5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 123000.0).
			End()
	})

	t.Run("Booleans", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "True value").
			Set("Sheet1!A1", true).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "False value").
			Set("Sheet1!A1", false).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", false).
			End()

		NewSpreadsheetTestCase(t, "Boolean in formula").
			Set("Sheet1!A1", "=TRUE").
			Set("Sheet1!A2", "=FALSE").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			AssertCellEq("Sheet1!A2", false).
			End()
	})

	t.Run("Strings", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Simple string").
			Set("Sheet1!A1", "Hello World").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "Hello World").
			End()

		NewSpreadsheetTestCase(t, "Empty string").
			Set("Sheet1!A1", "").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "").
			End()

		NewSpreadsheetTestCase(t, "String in formula").
			Set("Sheet1!A1", `="test"`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "test").
			End()
	})

	t.Run("Nil", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Empty cell").
			RunAndAssertNoError().
			AssertCellEmpty("Sheet1!A1").
			End()

		NewSpreadsheetTestCase(t, "Removed cell").
			Set("Sheet1!A1", 10.0).
			RunAndAssertNoError().
			Remove("Sheet1!A1").
			RunAndAssertNoError().
			AssertCellEmpty("Sheet1!A1").
			End()
	})
}

func TestBinaryOperators(t *testing.T) {
	t.Run("Arithmetic", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Addition").
			Set("Sheet1!A1", "=2+3").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 5.0).
			End()

		NewSpreadsheetTestCase(t, "Subtraction").
			Set("Sheet1!A1", "=10-4").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 6.0).
			End()

		NewSpreadsheetTestCase(t, "Multiplication").
			Set("Sheet1!A1", "=3*4").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 12.0).
			End()

		NewSpreadsheetTestCase(t, "Division").
			Set("Sheet1!A1", "=15/3").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 5.0).
			End()

		NewSpreadsheetTestCase(t, "Power").
			Set("Sheet1!A1", "=2^3").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 8.0).
			End()

		NewSpreadsheetTestCase(t, "Division by zero").
			Set("Sheet1!A1", "=1/0").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()
	})

	t.Run("Comparison", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Equal").
			Set("Sheet1!A1", "=5=5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "Not equal").
			Set("Sheet1!A1", "=5<>3").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "Less than").
			Set("Sheet1!A1", "=3<5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "Less than or equal").
			Set("Sheet1!A1", "=5<=5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "Greater than").
			Set("Sheet1!A1", "=7>5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "Greater than or equal").
			Set("Sheet1!A1", "=5>=5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()
	})

	t.Run("StringConcatenation", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Concat strings").
			Set("Sheet1!A1", `="Hello"&" "&"World"`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "Hello World").
			End()

		NewSpreadsheetTestCase(t, "Concat with numbers").
			Set("Sheet1!A1", `="Value: "&123`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "Value: 123").
			End()
	})
}

func TestUnaryOperators(t *testing.T) {
	t.Run("UnaryPlus", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Unary plus").
			Set("Sheet1!A1", "=+5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 5.0).
			End()
	})

	t.Run("UnaryMinus", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Unary minus").
			Set("Sheet1!A1", "=-5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", -5.0).
			End()
	})

	t.Run("Percent", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Percent").
			Set("Sheet1!A1", "=50%").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.5).
			End()
	})
}

func TestAggregationFunctions(t *testing.T) {
	t.Run("SUM", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Sum numbers").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 60.0).
			End()

		NewSpreadsheetTestCase(t, "Sum with empty cells").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 40.0).
			End()

		NewSpreadsheetTestCase(t, "Sum with text").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "text").
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 40.0).
			End()

		NewSpreadsheetTestCase(t, "Sum direct values").
			Set("Sheet1!A1", "=SUM(1, 2, 3, 4, 5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 15.0).
			End()

		NewSpreadsheetTestCase(t, "Sum with DIV/0 error first").
			Set("Sheet1!A1", "=SUM(4, 5, 6, 1/0, NonExistentRange)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Sum with DIV/0 error").
			Set("Sheet1!A1", "=SUM(1/0)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Sum with NAME error first").
			Set("Sheet1!A1", "=SUM(4, 5, NonExistentRange, 1/0, 6)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			End()

		NewSpreadsheetTestCase(t, "Sum with VALUE error first").
			Set("Sheet1!A1", `=SUM(4, ABS("text"), 1/0, NonExistentRange)`).
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeValue).
			End()

		NewSpreadsheetTestCase(t, "Sum with NUM error first").
			Set("Sheet1!A1", "=SUM(SQRT(-1), 1/0, NonExistentRange, 5)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNum).
			End()

		NewSpreadsheetTestCase(t, "Sum with multiple errors in range").
			Set("Sheet1!A1", "=1/0").         // DIV/0 error
			Set("Sheet1!A2", `=ABS("text")`). // VALUE error
			Set("Sheet1!A3", "=SQRT(-1)").    // NUM error
			Set("Sheet1!B1", "=SUM(A1:A3)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0). // Returns DIV/0 error
			End()

		NewSpreadsheetTestCase(t, "Sum with error as direct argument and range").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!B1", "=SUM(1/0, A1:A2)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Sum range with DIV/0 error in middle").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=1/0"). // DIV/0 error
			Set("Sheet1!A3", 20.0).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0). // SUM returns first error in range
			End()

		NewSpreadsheetTestCase(t, "Sum range with multiple error types").
			Set("Sheet1!A1", 5.0).
			Set("Sheet1!A2", "=1/0").         // DIV/0 error
			Set("Sheet1!A3", `=ABS("text")`). // VALUE error
			Set("Sheet1!A4", 10.0).
			Set("Sheet1!A5", "=SQRT(-1)"). // NUM error
			Set("Sheet1!B1", "=SUM(A1:A5)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0). // SUM returns first error (DIV/0 at A2)
			End()

		NewSpreadsheetTestCase(t, "Sum multiple ranges with errors").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=1/0").
			Set("Sheet1!B1", 20.0).
			Set("Sheet1!B2", "=NonExistent").
			Set("Sheet1!C1", "=SUM(A1:A2, B1:B2)").
			Set("Sheet1!C2", "=SUM(B1:B2, A1:A2)").
			Run().
			AssertCellErr("Sheet1!C1", ErrorCodeDiv0). // Returns DIV/0 from first range A1:A2
			AssertCellErr("Sheet1!C2", ErrorCodeName). // Returns NAME from first range B1:B2
			End()

		NewSpreadsheetTestCase(t, "Sum range where all cells are errors").
			Set("Sheet1!A1", "=1/0").
			Set("Sheet1!A2", "=SQRT(-1)").
			Set("Sheet1!A3", `=ABS("text")`).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0). // Returns DIV/0 error
			End()
	})

	t.Run("AVERAGE", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Average numbers").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=AVERAGE(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 20.0).
			End()

		NewSpreadsheetTestCase(t, "Average with empty cells").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=AVERAGE(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 20.0).
			End()

		NewSpreadsheetTestCase(t, "Average no numeric values").
			Set("Sheet1!A1", "text").
			Set("Sheet1!B1", "=AVERAGE(A1)").
			Run().
			AssertCellEq("Sheet1!B1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Average with DIV/0 error").
			Set("Sheet1!A1", "=AVERAGE(1/0)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Average with error in range").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=1/0").
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=AVERAGE(A1:A3)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Average with error in range").
			Set("Sheet1!A1", "=AVERAGE(A3:A5)").
			Set("Sheet1!A3", "=1/0").
			Set("Sheet1!A4", 30.0).
			Set("Sheet1!A4", 40.0).
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Average with multiple errors returns first").
			Set("Sheet1!A1", "=AVERAGE(NonExistentRange, 1/0, SQRT(-1))").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			End()

		NewSpreadsheetTestCase(t, "Average with error and values").
			Set("Sheet1!A1", "=AVERAGE(10, 20, 1/0, 30)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()
	})

	t.Run("AVERAGEA", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "AVERAGEA with mixed types").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", true).
			Set("Sheet1!A3", "text").
			Set("Sheet1!B1", "=AVERAGEA(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 11.0/3.0).
			End()

		NewSpreadsheetTestCase(t, "AVERAGEA with false").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", false).
			Set("Sheet1!B1", "=AVERAGEA(A1:A2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 5.0).
			End()

		NewSpreadsheetTestCase(t, "AVERAGEA with DIV/0 error").
			Set("Sheet1!A1", "=AVERAGEA(1/0)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "AVERAGEA with error in range").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=1/0").
			Set("Sheet1!A3", 20.0).
			Set("Sheet1!B1", "=AVERAGEA(A1, 1/0)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Average with error in range").
			Set("Sheet1!A1", "=AVERAGEA(A3:A5)").
			Set("Sheet1!A3", "=1/0").
			Set("Sheet1!A4", 30.0).
			Set("Sheet1!A4", 40.0).
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "AVERAGEA with multiple errors returns first").
			Set("Sheet1!A1", "=AVERAGEA(SQRT(-1), 1/0, NonExistentRange)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNum).
			End()

		NewSpreadsheetTestCase(t, "AVERAGEA with error and values").
			Set("Sheet1!A1", "=AVERAGEA(10, 20, 1/0, 30)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "AVERAGEA with no values").
			Set("Sheet1!A1", "=AVERAGEA(B1:B20)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()
	})

	t.Run("COUNT", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Count numbers only").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "text").
			Set("Sheet1!A3", true).
			Set("Sheet1!A4", 20.0).
			Set("Sheet1!B1", "=COUNT(A1:A4)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.0).
			End()
	})

	t.Run("COUNTA", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Count non-empty").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "text").
			Set("Sheet1!A3", true).
			Set("Sheet1!A5", 20.0).
			Set("Sheet1!B1", "=COUNTA(A1:A5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 4.0).
			End()
	})

	t.Run("MAX", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Max of numbers").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 50.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=MAX(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 50.0).
			End()

		NewSpreadsheetTestCase(t, "Max with non-numeric").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "text").
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=MAX(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 30.0).
			End()

		NewSpreadsheetTestCase(t, "Max no values").
			Set("Sheet1!B1", "=MAX()").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 0.0).
			End()
	})

	t.Run("MIN", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Min of numbers").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 50.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=MIN(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 10.0).
			End()

		NewSpreadsheetTestCase(t, "Min with non-numeric").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "text").
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=MIN(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 10.0).
			End()
	})

	t.Run("MEDIAN", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Median odd count").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 3.0).
			Set("Sheet1!A3", 2.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.0).
			End()

		NewSpreadsheetTestCase(t, "Median even count").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!A3", 3.0).
			Set("Sheet1!A4", 4.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A4)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.5).
			End()

		NewSpreadsheetTestCase(t, "Median no values").
			Set("Sheet1!A1", "text").
			Set("Sheet1!B1", "=MEDIAN(A1)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeNum).
			End()

		NewSpreadsheetTestCase(t, "Median single value").
			Set("Sheet1!A1", 42.0).
			Set("Sheet1!B1", "=MEDIAN(A1)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 42.0).
			End()

		NewSpreadsheetTestCase(t, "Median two values").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 15.0).
			End()

		NewSpreadsheetTestCase(t, "Median unsorted values").
			Set("Sheet1!A1", 5.0).
			Set("Sheet1!A2", 1.0).
			Set("Sheet1!A3", 9.0).
			Set("Sheet1!A4", 3.0).
			Set("Sheet1!A5", 7.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 5.0).
			End()

		NewSpreadsheetTestCase(t, "Median with duplicates").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!A3", 2.0).
			Set("Sheet1!A4", 3.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A4)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.0).
			End()

		NewSpreadsheetTestCase(t, "Median with negative numbers").
			Set("Sheet1!A1", -5.0).
			Set("Sheet1!A2", 0.0).
			Set("Sheet1!A3", 5.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 0.0).
			End()

		NewSpreadsheetTestCase(t, "Median with decimals").
			Set("Sheet1!A1", 1.1).
			Set("Sheet1!A2", 2.2).
			Set("Sheet1!A3", 3.3).
			Set("Sheet1!A4", 4.4).
			Set("Sheet1!B1", "=MEDIAN(A1:A4)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.75).
			End()

		NewSpreadsheetTestCase(t, "Median with mixed types").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", "text").
			Set("Sheet1!A3", 3.0).
			Set("Sheet1!A4", true).
			Set("Sheet1!A5", 5.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.0).
			End()

		NewSpreadsheetTestCase(t, "Median direct values").
			Set("Sheet1!A1", "=MEDIAN(1, 2, 3, 4, 5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3.0).
			End()

		NewSpreadsheetTestCase(t, "Median large dataset").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!A3", 3.0).
			Set("Sheet1!A4", 4.0).
			Set("Sheet1!A5", 5.0).
			Set("Sheet1!A6", 6.0).
			Set("Sheet1!A7", 7.0).
			Set("Sheet1!A8", 8.0).
			Set("Sheet1!A9", 9.0).
			Set("Sheet1!A10", 10.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A10)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 5.5).
			End()

		NewSpreadsheetTestCase(t, "Median with empty cells in range").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A3", 3.0).
			Set("Sheet1!A5", 5.0).
			Set("Sheet1!B1", "=MEDIAN(A1:A5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 1.0).
			End()
	})

	t.Run("MODE", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Mode with clear winner").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!A3", 2.0).
			Set("Sheet1!A4", 3.0).
			Set("Sheet1!B1", "=MODE(A1:A4)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.0).
			End()

		NewSpreadsheetTestCase(t, "Mode no repeats").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!A3", 3.0).
			Set("Sheet1!B1", "=MODE(A1:A3)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeNA).
			End()
	})
}

func TestLogicalFunctions(t *testing.T) {
	t.Run("IF", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "IF true condition").
			Set("Sheet1!A1", "=IF(TRUE, 10, 20)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 10.0).
			End()

		NewSpreadsheetTestCase(t, "IF false condition").
			Set("Sheet1!A1", "=IF(FALSE, 10, 20)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 20.0).
			End()

		NewSpreadsheetTestCase(t, "IF two arguments").
			Set("Sheet1!A1", "=IF(TRUE, 10)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 10.0).
			End()

		NewSpreadsheetTestCase(t, "IF false two arguments").
			Set("Sheet1!A1", "=IF(FALSE, 10)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", false).
			End()

		NewSpreadsheetTestCase(t, "IF with comparison").
			Set("Sheet1!A1", 15.0).
			Set("Sheet1!B1", `=IF(A1>10, "big", "small")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", "big").
			End()
	})

	t.Run("AND", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "AND all true").
			Set("Sheet1!A1", "=AND(TRUE, TRUE, TRUE)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "AND with false").
			Set("Sheet1!A1", "=AND(TRUE, FALSE, TRUE)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", false).
			End()

		NewSpreadsheetTestCase(t, "AND with numbers").
			Set("Sheet1!A1", "=AND(1, 2, 3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "AND with zero").
			Set("Sheet1!A1", "=AND(1, 0, 1)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", false).
			End()
	})

	t.Run("OR", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "OR with true").
			Set("Sheet1!A1", "=OR(FALSE, TRUE, FALSE)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "OR all false").
			Set("Sheet1!A1", "=OR(FALSE, FALSE, FALSE)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", false).
			End()

		NewSpreadsheetTestCase(t, "OR with numbers").
			Set("Sheet1!A1", "=OR(0, 0, 1)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()
	})

	t.Run("NOT", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "NOT true").
			Set("Sheet1!A1", "=NOT(TRUE)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", false).
			End()

		NewSpreadsheetTestCase(t, "NOT false").
			Set("Sheet1!A1", "=NOT(FALSE)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "NOT number").
			Set("Sheet1!A1", "=NOT(0)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", true).
			End()

		NewSpreadsheetTestCase(t, "NOT wrong args").
			Set("Sheet1!A1", "=NOT()").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNA).
			End()
	})
}

func TestTextFunctions(t *testing.T) {
	t.Run("CONCATENATE", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Concatenate strings").
			Set("Sheet1!A1", `=CONCATENATE("Hello", " ", "World")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "Hello World").
			End()

		NewSpreadsheetTestCase(t, "Concatenate mixed types").
			Set("Sheet1!A1", `=CONCATENATE("Value: ", 123, " - ", TRUE)`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "Value: 123 - true").
			End()
	})

	t.Run("LEN", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Length of string").
			Set("Sheet1!A1", `=LEN("Hello")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 5.0).
			End()

		NewSpreadsheetTestCase(t, "Length of number").
			Set("Sheet1!A1", "=LEN(12345)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 5.0).
			End()

		NewSpreadsheetTestCase(t, "Length of empty").
			Set("Sheet1!A1", `=LEN("")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.0).
			End()
	})

	t.Run("UPPER", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Upper case").
			Set("Sheet1!A1", `=UPPER("hello world")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "HELLO WORLD").
			End()

		NewSpreadsheetTestCase(t, "Upper number").
			Set("Sheet1!A1", "=UPPER(123)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "123").
			End()
	})

	t.Run("LOWER", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Lower case").
			Set("Sheet1!A1", `=LOWER("HELLO WORLD")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "hello world").
			End()
	})

	t.Run("TRIM", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Trim spaces").
			Set("Sheet1!A1", `=TRIM("  hello world  ")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "hello world").
			End()
	})
}

func TestMathFunctions(t *testing.T) {
	t.Run("ABS", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "ABS positive").
			Set("Sheet1!A1", "=ABS(10)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 10.0).
			End()

		NewSpreadsheetTestCase(t, "ABS negative").
			Set("Sheet1!A1", "=ABS(-10)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 10.0).
			End()

		NewSpreadsheetTestCase(t, "ABS zero").
			Set("Sheet1!A1", "=ABS(0)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.0).
			End()

		NewSpreadsheetTestCase(t, "ABS non-numeric").
			Set("Sheet1!A1", `=ABS("text")`).
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeValue).
			End()
	})

	t.Run("ROUND", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Round default").
			Set("Sheet1!A1", "=ROUND(3.7)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 4.0).
			End()

		NewSpreadsheetTestCase(t, "Round down").
			Set("Sheet1!A1", "=ROUND(3.4)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3.0).
			End()

		NewSpreadsheetTestCase(t, "Round to decimals").
			Set("Sheet1!A1", "=ROUND(3.14159, 2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3.14).
			End()

		NewSpreadsheetTestCase(t, "Round negative places").
			Set("Sheet1!A1", "=ROUND(1234.5, -2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 1200.0).
			End()
	})

	t.Run("FLOOR", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Floor positive").
			Set("Sheet1!A1", "=FLOOR(3.7)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 3.0).
			End()

		NewSpreadsheetTestCase(t, "Floor negative").
			Set("Sheet1!A1", "=FLOOR(-3.7)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", -4.0).
			End()
	})

	t.Run("CEILING", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Ceiling positive").
			Set("Sheet1!A1", "=CEILING(3.2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 4.0).
			End()

		NewSpreadsheetTestCase(t, "Ceiling negative").
			Set("Sheet1!A1", "=CEILING(-3.2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", -3.0).
			End()
	})

	t.Run("SQRT", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Square root").
			Set("Sheet1!A1", "=SQRT(16)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 4.0).
			End()

		NewSpreadsheetTestCase(t, "Square root zero").
			Set("Sheet1!A1", "=SQRT(0)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.0).
			End()

		NewSpreadsheetTestCase(t, "Square root negative").
			Set("Sheet1!A1", "=SQRT(-1)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNum).
			End()
	})

	t.Run("POWER", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Power positive").
			Set("Sheet1!A1", "=POWER(2, 3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 8.0).
			End()

		NewSpreadsheetTestCase(t, "Power zero exponent").
			Set("Sheet1!A1", "=POWER(5, 0)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 1.0).
			End()

		NewSpreadsheetTestCase(t, "Power negative exponent").
			Set("Sheet1!A1", "=POWER(2, -2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.25).
			End()
	})

	t.Run("MOD", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Modulo").
			Set("Sheet1!A1", "=MOD(10, 3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 1.0).
			End()

		NewSpreadsheetTestCase(t, "Modulo negative").
			Set("Sheet1!A1", "=MOD(-10, 3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", -1.0).
			End()

		NewSpreadsheetTestCase(t, "Modulo by zero").
			Set("Sheet1!A1", "=MOD(10, 0)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()
	})

	t.Run("PI", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "PI constant").
			Set("Sheet1!A1", "=PI()").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", math.Pi).
			End()

		NewSpreadsheetTestCase(t, "PI with args").
			Set("Sheet1!A1", "=PI(1)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNA).
			End()
	})
}

func TestVolatileFunctions(t *testing.T) {
	t.Run("NOW", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "NOW function").
			Set("Sheet1!A1", "=NOW()").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); !ok || num <= 0 {
					t.Errorf("NOW() should return positive number, got %v", val)
				}
			}).
			End()
	})

	t.Run("TODAY", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "TODAY function").
			Set("Sheet1!A1", "=TODAY()").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); !ok || num <= 0 {
					t.Errorf("TODAY() should return positive number, got %v", val)
				}
			}).
			End()
	})

	t.Run("RAND", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "RAND function").
			Set("Sheet1!A1", "=RAND()").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); !ok || num < 0 || num >= 1 {
					t.Errorf("RAND() should return number in [0,1), got %v", val)
				}
			}).
			End()
	})
}

func TestCellReferences(t *testing.T) {
	t.Run("SimpleReferences", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Direct reference").
			Set("Sheet1!A1", 42.0).
			Set("Sheet1!B1", "=A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 42.0).
			End()

		NewSpreadsheetTestCase(t, "Chain reference").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!B1", "=A1*2").
			Set("Sheet1!C1", "=B1*2").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!C1", 40.0).
			End()
	})

	t.Run("CrossWorksheetNotImplemented", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Cross-sheet").
			AddWorksheet("Data").
			Set("Sheet1!Data!A1", 10.0).
			Set("Sheet1!Data!A2", 20.0).
			Set("Sheet1!A1", "=SUM(Data!A1:Data!A2)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()
	})

	t.Run("InvalidReferences", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Non-existent worksheet").
			Set("Sheet1!A1", "=NoSheet!A1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()
	})

	t.Run("CircularReferences", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Direct circular").
			Set("Sheet1!A1", "=A1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()

		NewSpreadsheetTestCase(t, "Indirect circular").
			Set("Sheet1!A1", "=B1").
			Set("Sheet1!B1", "=A1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()
	})
}

func TestRangeReferences(t *testing.T) {
	t.Run("BasicRanges", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Simple range").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!A3", 3.0).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 6.0).
			End()

		NewSpreadsheetTestCase(t, "Rectangle range").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!B1", 3.0).
			Set("Sheet1!B2", 4.0).
			Set("Sheet1!C1", "=SUM(A1:B2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!C1", 10.0).
			End()
	})

	t.Run("CrossWorksheetRanges", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Cross-sheet range").
			AddWorksheet("Data").
			Set("Sheet1!Data!A1", 10.0).
			Set("Sheet1!Data!A2", 20.0).
			Set("Sheet1!Data!A3", 30.0).
			Set("Sheet1!A1", "=SUM(Data!A1:A3)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 60.0).
			End()
	})
}

func TestWorksheetOperations(t *testing.T) {
	t.Run("AddWorksheet", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Add worksheet")
		tc.AddWorksheet("Sheet2").
			AssertWorksheetExists("Sheet2", true).
			End()

		tc = NewSpreadsheetTestCase(t, "Add duplicate worksheet")
		tc.AddWorksheet("Sheet2").
			AddWorksheet("Sheet2").
			ExpectAppError(AlreadyExists).
			End()
	})

	t.Run("RemoveWorksheet", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Remove worksheet")
		tc.AddWorksheet("Sheet2").
			RemoveWorksheet("Sheet2").
			AssertWorksheetExists("Sheet2", false).
			End()

		tc = NewSpreadsheetTestCase(t, "Remove non-existent")
		tc.RemoveWorksheet("NoSheet").
			ExpectAppError(NotFound).
			End()
	})

	t.Run("RenameWorksheet", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Rename worksheet")
		tc.AddWorksheet("OldName").
			RenameWorksheet("OldName", "NewName").
			AssertWorksheetExists("OldName", false).
			AssertWorksheetExists("NewName", true).
			End()

		tc = NewSpreadsheetTestCase(t, "Rename to existing")
		tc.AddWorksheet("Sheet2").
			AddWorksheet("Sheet3").
			RenameWorksheet("Sheet2", "Sheet3").
			ExpectAppError(AlreadyExists).
			End()
	})

	t.Run("DefaultWorksheet", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Default Sheet1").
			Set("Sheet1!A1", 10.0).
			RunAndAssertNoError().
			AssertWorksheetExists("Sheet1", true).
			AssertCellEq("Sheet1!A1", 10.0).
			End()
	})
}

func TestNamedRangeOperations(t *testing.T) {
	t.Run("AddNamedRange", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Add named range")
		tc.AddNamedRange("MyRange").
			AssertNamedRangeExists("MyRange", false).
			End()

		tc = NewSpreadsheetTestCase(t, "Add duplicate named range")
		tc.AddNamedRange("MyRange").
			RunAndAssertNoError().
			AddNamedRange("MyRange").
			ExpectAppError(AlreadyExists).
			End()
	})

	t.Run("RemoveNamedRange", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Remove named range")
		tc.AddNamedRange("MyRange").
			RunAndAssertNoError().
			RemoveNamedRange("MyRange").
			AssertNamedRangeExists("MyRange", false).
			End()

		tc = NewSpreadsheetTestCase(t, "Remove non-existent")
		tc.RemoveNamedRange("NoRange").
			ExpectAppError(NotFound).
			End()
	})

	t.Run("RenameNamedRange", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Rename named range")
		tc.AddNamedRange("OldRange").
			RenameNamedRange("OldRange", "NewRange").
			AssertNamedRangeExists("OldRange", false).
			End()

		tc = NewSpreadsheetTestCase(t, "Rename to existing")
		tc.AddNamedRange("Range1").
			RunAndAssertNoError().
			AddNamedRange("Range2").
			RunAndAssertNoError().
			RenameNamedRange("Range1", "Range2").
			ExpectAppError(AlreadyExists).
			End()
	})
}

func TestTypeConversions(t *testing.T) {
	t.Run("StringToNumber", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "String to number in SUM").
			Set("Sheet1!A1", "123").
			Set("Sheet1!B1", "=A1+1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 124.0).
			End()

		NewSpreadsheetTestCase(t, "Invalid string to number causes error").
			Set("Sheet1!A1", "abc").
			Set("Sheet1!B1", "=A1+1").
			RunAndAssertNoError().
			AssertCellErr("Sheet1!B1", ErrorCodeValue).
			End()
	})

	t.Run("BooleanToNumber", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "TRUE to number").
			Set("Sheet1!A1", true).
			Set("Sheet1!B1", "=A1+1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 2.0).
			End()

		NewSpreadsheetTestCase(t, "FALSE to number").
			Set("Sheet1!A1", false).
			Set("Sheet1!B1", "=A1+1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 1.0).
			End()
	})

	t.Run("NumberToString", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Number in concatenation").
			Set("Sheet1!A1", 123.0).
			Set("Sheet1!B1", `="Value: "&A1`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", "Value: 123").
			End()
	})

	t.Run("NilHandling", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Nil in arithmetic").
			Set("Sheet1!B1", "=A1+10").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 10.0).
			End()

		NewSpreadsheetTestCase(t, "Nil in string").
			Set("Sheet1!B1", `="Value: "&A1`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", "Value: ").
			End()
	})
}

func TestErrorPropagation(t *testing.T) {
	t.Run("ErrorInFormula", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Error propagation").
			Set("Sheet1!A1", "=1/0").
			Set("Sheet1!B1", "=A1+10").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()
	})

	t.Run("ErrorInFunction", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Error in SUM").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=1/0").
			Set("Sheet1!A3", 20.0).
			Set("Sheet1!B1", "=SUM(A1:A3)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0). // Returns DIV/0 error
			End()
	})

	t.Run("AllErrorCodes", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "DIV0 error").
			Set("Sheet1!A1", "=1/0").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "VALUE error").
			Set("Sheet1!A1", `=ABS("text")`).
			Run().
			AssertCellEq("Sheet1!A1", ErrorCodeValue).
			End()

		NewSpreadsheetTestCase(t, "NAME error").
			Set("Sheet1!A1", "=BAD()").
			Run().
			AssertCellEq("Sheet1!A1", ErrorCodeName).
			End()

		NewSpreadsheetTestCase(t, "NUM error").
			Set("Sheet1!A1", "=SQRT(-1)").
			Run().
			AssertCellEq("Sheet1!A1", ErrorCodeNum).
			End()

		NewSpreadsheetTestCase(t, "NA error").
			Set("Sheet1!A1", "=IF()").
			Run().
			AssertCellEq("Sheet1!A1", ErrorCodeNA).
			End()
	})
}

func TestComplexFormulas(t *testing.T) {
	t.Run("NestedFunctions", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Nested IF").
			Set("Sheet1!A1", 15.0).
			Set("Sheet1!B1", `=IF(A1>20, "big", IF(A1>10, "medium", "small"))`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", "medium").
			End()

		NewSpreadsheetTestCase(t, "Complex calculation").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!B1", "=ROUND(AVERAGE(A1:A3)*1.1, 2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 22.0).
			End()
	})

	t.Run("MixedOperations", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Math and logic").
			Set("Sheet1!A1", 5.0).
			Set("Sheet1!B1", 10.0).
			Set("Sheet1!C1", "=IF(A1+B1>12, SUM(A1:B1)*2, AVERAGE(A1:B1))").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!C1", 30.0).
			End()
	})

	t.Run("DependencyChains", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Long dependency chain").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", "=A1+1").
			Set("Sheet1!A3", "=A2+1").
			Set("Sheet1!A4", "=A3+1").
			Set("Sheet1!A5", "=A4+1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 1.0).
			AssertCellEq("Sheet1!A2", 2.0).
			AssertCellEq("Sheet1!A3", 3.0).
			AssertCellEq("Sheet1!A4", 4.0).
			AssertCellEq("Sheet1!A5", 5.0).
			End()
	})

	t.Run("OrderOfOperations", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Operator precedence").
			Set("Sheet1!A1", "=2+3*4").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 14.0).
			End()

		NewSpreadsheetTestCase(t, "Parentheses").
			Set("Sheet1!A1", "=(2+3)*4").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 20.0).
			End()

		NewSpreadsheetTestCase(t, "Power precedence").
			Set("Sheet1!A1", "=2^3*4").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 32.0).
			End()
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("EmptyCells", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Reference to empty cell").
			Set("Sheet1!B1", "=A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 0.0).
			End()

		NewSpreadsheetTestCase(t, "Empty in arithmetic").
			Set("Sheet1!B1", "=A1+10").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 10.0).
			End()
	})

	t.Run("LargeNumbers", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Very large number").
			Set("Sheet1!A1", 1e308).
			Set("Sheet1!B1", "=A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 1e308).
			End()

		NewSpreadsheetTestCase(t, "Very small number").
			Set("Sheet1!A1", 1e-308).
			Set("Sheet1!B1", "=A1*1000000").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 1e-302).
			End()
	})

	t.Run("SpecialValues", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Zero division").
			Set("Sheet1!A1", 0.0).
			Set("Sheet1!B1", "=1/A1").
			Run().
			AssertCellEq("Sheet1!B1", ErrorCodeDiv0).
			End()

		NewSpreadsheetTestCase(t, "Infinity handling").
			Set("Sheet1!A1", "=1/0").
			Set("Sheet1!B1", "=A1").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()
	})
}

func TestUpdateAndRecalculation(t *testing.T) {
	t.Run("CellUpdates", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Update dependent cells")
		tc.Set("Sheet1!A1", 10.0).
			Set("Sheet1!B1", "=A1*2").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 20.0).
			Set("Sheet1!A1", 15.0).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 30.0).
			End()
	})

	t.Run("FormulaChanges", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Change formula")
		tc.Set("Sheet1!A1", 10.0).
			Set("Sheet1!B1", "=A1*2").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 20.0).
			Set("Sheet1!B1", "=A1+5").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 15.0).
			End()
	})

	t.Run("RemoveAndRecalculate", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Remove referenced cell").Set("Sheet1!A1", 10.0).
			Set("Sheet1!B1", "=A1*2").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 20.0).
			Remove("Sheet1!A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 0.0).
			End()
	})
}

func TestNamedRangesInFormulas(t *testing.T) {
	t.Run("UsingNamedRangeInFormula", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Named range in SUM").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!A3", 30.0).
			AddNamedRange("DataRange").
			Set("Sheet1!B1", "=SUM(DataRange)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeName).
			End()
	})

	t.Run("UndefinedNamedRange", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Undefined named range").
			Set("Sheet1!A1", "=SUM(NonExistentRange)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			End()
	})

	t.Run("CircularReferenceViaNamedRange", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Circular via named range").
			AddNamedRange("MyRange").
			Set("Sheet1!A1", "=MyRange+1").
			Set("Sheet1!A2", "=A1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			End()
	})

	t.Run("RenameNamedRangeWithReferences", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Rename with active refs")
		tc.AddNamedRange("OldName").
			Set("Sheet1!A1", "=OldName").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			RenameNamedRange("OldName", "NewName").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			End()
	})
}

func TestVolatileFunctionBehavior(t *testing.T) {
	t.Run("VolatileRecalculation", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Volatile recalc")
		tc.Set("Sheet1!A1", "=RAND()").
			RunAndAssertNoError()

		var firstValue float64
		tc.AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
			if num, ok := val.(float64); ok {
				firstValue = num
				if num < 0 || num >= 1 {
					t.Errorf("RAND() out of range: %v", num)
				}
			} else {
				t.Errorf("RAND() didn't return float64: %T", val)
			}
		})

		tc.RunAndAssertNoError()
		tc.AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
			if num, ok := val.(float64); ok {
				if num == firstValue {
					t.Errorf("RAND() should produce different value on recalc, got same: %v", num)
				}
			}
		}).End()
	})

	t.Run("VolatileDependencyPropagation", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Volatile dependency")
		tc.Set("Sheet1!A1", "=RAND()").
			Set("Sheet1!B1", "=A1*100").
			RunAndAssertNoError()

		var firstB1 float64
		tc.AssertCellFn("Sheet1!B1", func(val Primitive, t *testing.T) {
			if num, ok := val.(float64); ok {
				firstB1 = num
			}
		})

		tc.RunAndAssertNoError()
		tc.AssertCellFn("Sheet1!B1", func(val Primitive, t *testing.T) {
			if num, ok := val.(float64); ok {
				if num == firstB1 {
					t.Errorf("B1 should recalc when A1 (volatile) changes")
				}
			}
		}).End()
	})

	t.Run("NOWSerialNumber", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "NOW serial number").
			Set("Sheet1!A1", "=NOW()").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); !ok {
					t.Errorf("NOW() should return float64, got %T", val)
				} else if num < 40000 || num > 50000 {
					t.Errorf("NOW() serial number seems wrong: %v", num)
				}
			}).
			End()
	})

	t.Run("TODAYSerialNumber", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "TODAY serial number").
			Set("Sheet1!A1", "=TODAY()").
			Set("Sheet1!A2", "=NOW()").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
				today, _ := val.(float64)
				if today != math.Floor(today) {
					t.Errorf("TODAY() should return whole number, got %v", today)
				}
			}).
			End()
	})
}

func TestCrossWorksheetReferences(t *testing.T) {
	t.Run("SimpleWorksheetReference", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Sheet reference").
			AddWorksheet("Data").
			Set("Sheet1!Data!A1", 42.0).
			Set("Sheet1!B1", "=Data!A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 42.0).
			End()
	})

	t.Run("WorksheetReferenceChain", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Cross-sheet chain").
			AddWorksheet("Sheet2").
			AddWorksheet("Sheet3").
			Set("Sheet1!A1", 10.0).
			Set("Sheet2!A1", "=Sheet1!A1*2").
			Set("Sheet3!A1", "=Sheet2!A1*2").
			RunAndAssertNoError().
			AssertCellEq("Sheet3!A1", 40.0).
			End()
	})

	t.Run("RemoveWorksheetWithDependents", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Remove sheet with deps")
		tc.AddWorksheet("Data").
			Set("Sheet1!Data!A1", 100.0).
			Set("Sheet1!B1", "=Data!A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 100.0).
			RemoveWorksheet("Data").
			RunAndAssertNoError().
			AssertCellErr("Sheet1!B1", ErrorCodeRef).
			End()
	})

	t.Run("RenameWorksheetWithReferences", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Rename sheet with refs")
		tc.AddWorksheet("OldSheet").
			Set("OldSheet!A1", 50.0).
			Set("Sheet1!B1", "=OldSheet!A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 50.0).
			RenameWorksheet("OldSheet", "NewSheet").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 50.0).
			End()
	})
}

func TestAdvancedCircularReferences(t *testing.T) {
	t.Run("ThreeCellCircular", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Three cell circular").
			Set("Sheet1!A1", "=C1").
			Set("Sheet1!B1", "=A1").
			Set("Sheet1!C1", "=B1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			AssertCellErr("Sheet1!B1", ErrorCodeRef).
			AssertCellErr("Sheet1!C1", ErrorCodeRef).
			End()
	})

	t.Run("CircularThroughRange", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Circular via range").
			Set("Sheet1!A1", "=SUM(A1:A3)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()
	})

	t.Run("IndirectCircularViaIF", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Circular via IF").
			Set("Sheet1!A1", "=IF(B1>0, B1, 0)").
			Set("Sheet1!B1", "=A1+1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			AssertCellErr("Sheet1!B1", ErrorCodeRef).
			End()
	})

	t.Run("DeepCircularChain", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Deep circular chain").
			Set("Sheet1!A1", "=A2").
			Set("Sheet1!A2", "=A3").
			Set("Sheet1!A3", "=A4").
			Set("Sheet1!A4", "=A5").
			Set("Sheet1!A5", "=A1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			End()
	})
}

func TestRangeEdgeCases(t *testing.T) {
	t.Run("InvertedRange", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Inverted range").
			Set("Sheet1!A1", 1.0).
			Set("Sheet1!A2", 2.0).
			Set("Sheet1!B1", 3.0).
			Set("Sheet1!B2", 4.0).
			Set("Sheet1!C1", "=SUM(B2:A1)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!C1", 10.0).
			End()
	})

	t.Run("SingleCellRange", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Single cell range").
			Set("Sheet1!A1", 42.0).
			Set("Sheet1!B1", "=SUM(A1:A1)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 42.0).
			End()
	})

	t.Run("LargeRange", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Large range")
		for i := 1; i <= 100; i++ {
			tc.Set(fmt.Sprintf("Sheet1!A%d", i), float64(i))
		}
		tc.Set("Sheet1!B1", "=SUM(A1:A100)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 5050.0).
			End()
	})

	t.Run("RangeWithMixedEmptyAndError", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Range with mixed cells").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", "=1/0").
			Set("Sheet1!A4", 20.0).
			Set("Sheet1!B1", "=SUM(A1:A5)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()
	})
}

func TestUnicodeAndSpecialCharacters(t *testing.T) {
	t.Run("UnicodeInStrings", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Unicode strings").
			Set("Sheet1!A1", `=CONCATENATE("Hello", " ", "世界")`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", "Hello 世界").
			End()
	})

	t.Run("SpecialCharsInFormula", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Special chars").
			Set("Sheet1!A1", `="Line1" & CHAR(10) & "Line2"`).
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			End()
	})

	t.Run("MixedScripts", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Mixed scripts").
			Set("Sheet1!A1", "café 北京").
			Set("Sheet1!A2", "=UPPER(A1)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A2", "CAFÉ 北京").
			End()
	})
}

func TestPerformanceAndScale(t *testing.T) {
	t.Run("DeepDependencyChain", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Deep dependency")
		for i := 1; i <= 20; i++ {
			if i == 1 {
				tc.Set("Sheet1!A1", 1.0)
			} else {
				tc.Set(fmt.Sprintf("Sheet1!A%d", i), fmt.Sprintf("=A%d+1", i-1))
			}
		}
		tc.RunAndAssertNoError().
			AssertCellEq("Sheet1!A20", 20.0).
			End()
	})

	t.Run("WideDependencyGraph", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Wide dependency")
		tc.Set("Sheet1!A1", 1.0)
		for i := 1; i <= 50; i++ {
			tc.Set(fmt.Sprintf("Sheet1!B%d", i), "=A1*2")
		}
		tc.RunAndAssertNoError().
			AssertCellEq("Sheet1!B25", 2.0).
			Set("Sheet1!A1", 5.0).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B25", 10.0).
			End()
	})

	t.Run("ComplexNestedFormulas", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Complex nested").
			Set("Sheet1!A1", 5.0).
			Set("Sheet1!A2", 10.0).
			Set("Sheet1!B1", "=IF(AND(A1>0, A2>0), MAX(A1, A2) * MIN(A1, A2) / AVERAGE(A1:A2), 0)").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!B1", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); ok {
					expected := 10.0 * 5.0 / 7.5
					if math.Abs(num-expected) > 0.01 {
						t.Errorf("Complex formula = %v, want %v", num, expected)
					}
				}
			}).
			End()
	})

	t.Run("ManyVolatileCells", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Many volatile")
		for i := 1; i <= 10; i++ {
			tc.Set(fmt.Sprintf("Sheet1!A%d", i), "=RAND()")
			tc.Set(fmt.Sprintf("Sheet1!B%d", i), fmt.Sprintf("=A%d*100", i))
		}
		tc.RunAndAssertNoError()

		var values [10]float64
		for i := 1; i <= 10; i++ {
			idx := i - 1
			tc.AssertCellFn(fmt.Sprintf("Sheet1!B%d", i), func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); ok {
					values[idx] = num
				}
			})
		}

		tc.RunAndAssertNoError()
		changed := 0
		for i := 1; i <= 10; i++ {
			idx := i - 1
			expectedChanged := values[idx]
			tc.AssertCellFn(fmt.Sprintf("Sheet1!B%d", i), func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); ok {
					if num != expectedChanged {
						changed++
					}
				}
			})
		}
		if changed < 5 {
			tc.t.Errorf("Expected at least half of volatile cells to change on recalc, got %d", changed)
		}
		tc.End()
	})
}

func TestErrorPropagationAdvanced(t *testing.T) {
	t.Run("ErrorInNestedIF", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Error in nested IF").
			Set("Sheet1!A1", "=1/0").
			Set("Sheet1!B1", "=IF(TRUE, A1, 100)").
			Run().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()
	})

	t.Run("MixedErrorTypes", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Mixed errors").
			Set("Sheet1!A1", "=1/0").
			Set("Sheet1!A2", "=SQRT(-1)").
			Set("Sheet1!A3", "=A1+A2").
			Run().
			AssertCellErr("Sheet1!A3", ErrorCodeDiv0).
			End()
	})

	t.Run("ErrorInAggregation", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Ref to error cell in AVERAGE").
			Set("Sheet1!A1", "=1/0").
			Set("Sheet1!A2", 10.0).
			Set("Sheet1!A3", 20.0).
			Set("Sheet1!B1", "=AVERAGE(A1:A3)").
			RunAndAssertNoError().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()
		NewSpreadsheetTestCase(t, "Error in AVERAGE").
			Set("Sheet1!B1", "=AVERAGE(1, 2, 3, 1/0)").
			RunAndAssertNoError().
			AssertCellErr("Sheet1!B1", ErrorCodeDiv0).
			End()
	})

	t.Run("CascadingErrors", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Cascading errors").
			Set("Sheet1!A1", "=UNKNOWN()").
			Set("Sheet1!A2", "=A1*2").
			Set("Sheet1!A3", "=A2+A1").
			Set("Sheet1!A4", "=SUM(A1:A3)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeName).
			AssertCellErr("Sheet1!A2", ErrorCodeName).
			AssertCellErr("Sheet1!A3", ErrorCodeName).
			AssertCellErr("Sheet1!A4", ErrorCodeName).
			End()
	})
}

func TestNumericEdgeCases(t *testing.T) {
	t.Run("InfinityHandling", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Infinity ops").
			Set("Sheet1!A1", 1e308).
			Set("Sheet1!A2", "=A1*10").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A2", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); ok {
					if !math.IsInf(num, 1) {
						t.Logf("Large multiplication = %v", num)
					}
				}
			}).
			End()
	})

	t.Run("NaNPropagation", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "NaN handling").
			Set("Sheet1!A1", "=0/0").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeDiv0).
			End()
	})

	t.Run("PrecisionLoss", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Precision").
			Set("Sheet1!A1", 0.1).
			Set("Sheet1!A2", 0.2).
			Set("Sheet1!A3", "=A1+A2").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A3", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); ok {
					if math.Abs(num-0.3) > 1e-10 {
						t.Logf("0.1 + 0.2 = %.17f", num)
					}
				}
			}).
			End()
	})

	t.Run("ScientificNotationExtreme", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Extreme scientific").
			Set("Sheet1!A1", "=1.23E-300").
			Set("Sheet1!A2", "=9.99E299").
			RunAndAssertNoError().
			AssertCellFn("Sheet1!A1", func(val Primitive, t *testing.T) {
				if num, ok := val.(float64); ok && num == 0 {
					t.Logf("Very small number became zero")
				}
			}).
			End()
	})
}

func TestFunctionArgumentValidation(t *testing.T) {
	t.Run("WrongArgumentCount", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Too many args").
			Set("Sheet1!A1", "=ABS(1, 2)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNA).
			End()

		NewSpreadsheetTestCase(t, "Too few args").
			Set("Sheet1!A1", "=POWER(2)").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeNA).
			End()
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "String to SQRT").
			Set("Sheet1!A1", `=SQRT("text")`).
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeValue).
			End()

		NewSpreadsheetTestCase(t, "Bool in MOD").
			Set("Sheet1!A1", "=MOD(TRUE, 2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 1.0).
			End()
	})

	t.Run("EmptyArguments", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Empty SUM").
			Set("Sheet1!A1", "=SUM()").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.0).
			End()

		NewSpreadsheetTestCase(t, "Empty MAX").
			Set("Sheet1!A1", "=MAX()").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 0.0).
			End()
	})

	t.Run("MixedTypeArguments", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Mixed in SUM").
			Set("Sheet1!A1", `=SUM(1, "2", TRUE, FALSE)`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!A1", 4.0).
			End()
	})
}

func TestWorksheetLifecycleAdvanced(t *testing.T) {
	t.Run("MultipleSheetsWithSameCells", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Same cells different sheets").
			AddWorksheet("Sheet2").
			Set("Sheet1!A1", 100.0).
			Set("Sheet2!A1", 200.0).
			Set("Sheet1!B1", "=A1").
			Set("Sheet2!B1", "=A1").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 100.0).
			AssertCellEq("Sheet2!B1", 200.0).
			End()
	})

	t.Run("CrossSheetCircular", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Cross-sheet circular").
			AddWorksheet("Sheet2").
			Set("Sheet1!A1", "=Sheet2!A1").
			Set("Sheet2!A1", "=Sheet1!A1").
			Run().
			AssertCellErr("Sheet1!A1", ErrorCodeRef).
			AssertCellErr("Sheet2!A1", ErrorCodeRef).
			End()
	})

	t.Run("RemoveSheetWithCrossReferences", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Remove with cross refs")
		tc.AddWorksheet("Sheet2").
			AddWorksheet("Sheet3").
			Set("Sheet1!A1", 10.0).
			Set("Sheet2!A1", "=Sheet1!A1*2").
			Set("Sheet3!A1", "=Sheet2!A1*2").
			RunAndAssertNoError().
			AssertCellEq("Sheet3!A1", 40.0).
			RemoveWorksheet("Sheet2").
			RunAndAssertNoError().
			AssertCellErr("Sheet3!A1", ErrorCodeRef). // Sheet3 references removed Sheet2, should be REF error
			End()
	})
}

func TestComplexRealWorldScenarios(t *testing.T) {
	t.Run("FinancialCalculation", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Financial calc").
			Set("Sheet1!A1", 1000.0).
			Set("Sheet1!A2", 0.05).
			Set("Sheet1!A3", 12.0).
			Set("Sheet1!B1", "=A1*(1+A2/A3)^(A3*2)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 1104.9413355583).
			End()
	})

	t.Run("ConditionalAggregation", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Conditional sum").
			Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!A4", 40.0).
			Set("Sheet1!B1", "=IF(SUM(A1:A4)>50, AVERAGE(A1:A4), MAX(A1:A4))").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", 25.0).
			End()
	})

	t.Run("DataValidation", func(t *testing.T) {
		NewSpreadsheetTestCase(t, "Data validation").
			Set("Sheet1!A1", -5.0).
			Set("Sheet1!B1", `=IF(A1<0, "ERROR: Negative", IF(A1>100, "ERROR: Too large", "OK"))`).
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B1", "ERROR: Negative").
			End()
	})

	t.Run("RollingCalculations", func(t *testing.T) {
		tc := NewSpreadsheetTestCase(t, "Rolling calc")
		tc.Set("Sheet1!A1", 10.0).
			Set("Sheet1!A2", 20.0).
			Set("Sheet1!A3", 30.0).
			Set("Sheet1!A4", 40.0).
			Set("Sheet1!A5", 50.0).
			Set("Sheet1!B3", "=AVERAGE(A1:A3)").
			Set("Sheet1!B4", "=AVERAGE(A2:A4)").
			Set("Sheet1!B5", "=AVERAGE(A3:A5)").
			RunAndAssertNoError().
			AssertCellEq("Sheet1!B3", 20.0).
			AssertCellEq("Sheet1!B4", 30.0).
			AssertCellEq("Sheet1!B5", 40.0).
			End()
	})
}
