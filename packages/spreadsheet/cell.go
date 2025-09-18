package main

// Primitive represents basic spreadsheet value types.
// types:
//   - float64: numeric values (integers are converted to float64)
//   - string: text values
//   - bool: boolean values (TRUE/FALSE)
//   - nil: empty/null cells
//   - SpreadsheetError: error values (#DIV/0!, #VALUE!, etc.)
type Primitive any

// ErrorCode represents standard spreadsheet error codes following
// Excel conventions
type ErrorCode uint8

const (
	ErrorCodeNull  ErrorCode = 1 // #NULL! - no cells in common between ranges
	ErrorCodeDiv0  ErrorCode = 2 // #DIV/0! - division by zero
	ErrorCodeValue ErrorCode = 3 // #VALUE! - wrong type of argument or operand
	ErrorCodeRef   ErrorCode = 4 // #REF! - invalid cell reference
	ErrorCodeName  ErrorCode = 5 // #NAME? - unrecognized function name
	ErrorCodeNum   ErrorCode = 6 // #NUM! - number too large or small to be represented
	ErrorCodeNA    ErrorCode = 7 // #N/A - not enough arguments for function
	ErrorCodeOther ErrorCode = 8 // #ERROR! - all other errors
)

// ErrorMapper maps error code numbers to their string representations
var ErrorMapper = map[ErrorCode]string{
	ErrorCodeNull:  "#NULL!",
	ErrorCodeDiv0:  "#DIV/0!",
	ErrorCodeValue: "#VALUE!",
	ErrorCodeRef:   "#REF!",
	ErrorCodeName:  "#NAME?",
	ErrorCodeNum:   "#NUM!",
	ErrorCodeNA:    "#N/A",
	ErrorCodeOther: "#ERROR!",
}

// SpreadsheetError preserves error code for display in cells
type SpreadsheetError struct {
	ErrorCode ErrorCode
	Message   string
}

func (e *SpreadsheetError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return ErrorMapper[e.ErrorCode]
}

func NewSpreadsheetError(code ErrorCode, message string) *SpreadsheetError {
	if message == "" {
		message = ErrorMapper[code]
	}
	return &SpreadsheetError{
		ErrorCode: code,
		Message:   message,
	}
}

// CellType represents numeric constants for cell value
// types (external API)
type CellType uint8

const (
	CellValueTypeEmpty   CellType = 0
	CellValueTypeNumber  CellType = 1
	CellValueTypeString  CellType = 2
	CellValueTypeDate    CellType = 3
	CellValueTypeBoolean CellType = 4
	CellValueTypeError   CellType = 5
)

// CellValue represents a calculated cell value with type information
type CellValue struct {
	Type    CellType
	Value   Primitive
	Error   *ErrorCode
	Formula string
}

type CellAddress struct {
	WorksheetID uint32
	Row         uint32
	Column      uint32
}

// Cell represents a spreadsheet cell with its data and metadata
type Cell struct {
	Type              CellType  // cell type constant (0-6) indicating data type
	Row               uint32    // zero-based row index
	Col               uint32    // zero-based column index
	Value             Primitive // actual cell value - type depends on cell type
	StringID          uint32    // internal string table ID for STRING/ERROR types
	Formula           string    // formula text for FORMULA type cells
	FormulaID         uint32    // internal formula table ID for FORMULA type
	FormulaResultType CellType  // for FORMULA cells, the returned type
}
