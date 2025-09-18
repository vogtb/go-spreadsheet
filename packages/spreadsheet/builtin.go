package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"
)

// Clock interface provides time functionality for testing
type Clock interface {
	Now() time.Time
}

// WallClock is the default implementation using system time
type WallClock struct{}

func (w *WallClock) Now() time.Time {
	return time.Now()
}

// RandomGenerator interface provides random number generation for testing
type RandomGenerator interface {
	Float64() float64
}

// DefaultRandomGenerator uses the standard library's rand package
type DefaultRandomGenerator struct{}

func (d *DefaultRandomGenerator) Float64() float64 {
	return rand.Float64()
}

// BuiltInFunctions contains all spreadsheet built-in functions
type BuiltInFunctions struct {
	clock Clock
	rng   RandomGenerator
}

// checkForError returns the error if value is a *SpreadsheetError, nil otherwise
func checkForError(value Primitive) *SpreadsheetError {
	if err, ok := value.(*SpreadsheetError); ok {
		return err
	}
	return nil
}

// NewDefaultBuiltInFunctions creates a BuiltInFunctions with default
// implementations
func NewDefaultBuiltInFunctions() *BuiltInFunctions {
	return &BuiltInFunctions{
		clock: &WallClock{},
		rng:   &DefaultRandomGenerator{},
	}
}

// Call invokes a built-in function by name with the given arguments
func (bf *BuiltInFunctions) Call(name string, args ...any) (Primitive, error) {
	switch strings.ToUpper(name) {
	case "SUM":
		return bf.SUM(args...)
	case "AVERAGE":
		return bf.AVERAGE(args...)
	case "AVERAGEA":
		return bf.AVERAGEA(args...)
	case "COUNT":
		return bf.COUNT(args...)
	case "COUNTA":
		return bf.COUNTA(args...)
	case "MAX":
		return bf.MAX(args...)
	case "MIN":
		return bf.MIN(args...)
	case "MEDIAN":
		return bf.MEDIAN(args...)
	case "MODE":
		return bf.MODE(args...)
	case "IF":
		return bf.IF(args...)
	case "AND":
		return bf.AND(args...)
	case "OR":
		return bf.OR(args...)
	case "NOT":
		return bf.NOT(args...)
	case "CONCATENATE":
		return bf.CONCATENATE(args...)
	case "LEN":
		return bf.LEN(args...)
	case "UPPER":
		return bf.UPPER(args...)
	case "LOWER":
		return bf.LOWER(args...)
	case "TRIM":
		return bf.TRIM(args...)
	case "ABS":
		return bf.ABS(args...)
	case "ROUND":
		return bf.ROUND(args...)
	case "FLOOR":
		return bf.FLOOR(args...)
	case "CEILING":
		return bf.CEILING(args...)
	case "SQRT":
		return bf.SQRT(args...)
	case "POWER":
		return bf.POWER(args...)
	case "MOD":
		return bf.MOD(args...)
	case "PI":
		return bf.PI(args...)
	case "NOW":
		return bf.NOW(args...)
	case "TODAY":
		return bf.TODAY(args...)
	case "RAND":
		return bf.RAND(args...)
	default:
		return nil, NewSpreadsheetError(ErrorCodeName, fmt.Sprintf("Unknown function: %s", name))
	}
}

func (bf *BuiltInFunctions) SUM(args ...any) (Primitive, error) {
	sum := 0.0
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := checkForError(value); err != nil {
					return nil, err
				}
				if num, ok := toNumber(value); ok && !math.IsNaN(num) {
					sum += num
				}
			}
		} else {
			if num, ok := toNumber(arg); ok && !math.IsNaN(num) {
				sum += num
			}
		}
	}
	rounded, _ := strconv.ParseFloat(fmt.Sprintf("%.15f", sum), 64)
	return rounded, nil
}

func (bf *BuiltInFunctions) AVERAGE(args ...any) (Primitive, error) {
	sum := 0.0
	count := 0
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}
		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := checkForError(value); err != nil {
					return nil, err
				}
				if value != nil {
					if num, ok := toNumber(value); ok && !math.IsNaN(num) {
						sum += num
						count++
					}
				}
			}
		} else {
			if num, ok := toNumber(arg); ok && !math.IsNaN(num) {
				sum += num
				count++
			}
		}
	}

	if count == 0 {
		return nil, NewSpreadsheetError(ErrorCodeDiv0, "Division by zero")
	}

	return sum / float64(count), nil
}

func (bf *BuiltInFunctions) AVERAGEA(args ...any) (Primitive, error) {
	sum := 0.0
	count := 0

	// helper function to process a single value
	processValue := func(value Primitive) error {
		// nil values (empty cells) are ignored - only from Range iteration
		if value == nil {
			return nil
		}

		// errors propagate
		if err := checkForError(value); err != nil {
			return err
		}
		// AVERAGEA includes all non-empty values in the count but only
		// numeric values contribute to the sum
		switch v := value.(type) {
		case float64:
			sum += v
			count++
		case bool:
			// TRUE = 1, FALSE = 0
			if v {
				sum += 1
			}
			count++
		case string:
			// text values count as 0 (don't affect sum) but do increase count
			count++
		}
		return nil
	}
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := processValue(value); err != nil {
					return nil, err
				}
			}
		} else {
			// Direct args are never nil, process them directly
			if err := processValue(arg); err != nil {
				return nil, err
			}
		}
	}

	if count == 0 {
		return nil, NewSpreadsheetError(ErrorCodeRef, "AVERAGEA has no values")
	}

	return sum / float64(count), nil
}

func (bf *BuiltInFunctions) COUNT(args ...any) (Primitive, error) {
	count := 0

	// helper function to check if a value should be counted
	// COUNT only counts numeric values
	shouldCount := func(value Primitive) bool {
		switch value.(type) {
		case float64:
			// only float64 numeric type is counted
			return true
		case bool:
			// booleans are NOT counted by COUNT (different from COUNTA)
			return false
		case string:
			// strings are NOT counted, even if they look like numbers
			return false
		case nil:
			// empty cells are not counted (only from Range iteration)
			return false
		case *SpreadsheetError:
			// errors are not counted
			return false
		default:
			return false
		}
	}

	for _, arg := range args {
		// Direct args that are errors should propagate
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				// COUNT doesn't propagate errors from Range values, just skips them
				if _, isErr := value.(*SpreadsheetError); !isErr && shouldCount(value) {
					count++
				}
			}
		} else {
			if shouldCount(arg) {
				count++
			}
		}
	}

	return float64(count), nil
}

func (bf *BuiltInFunctions) COUNTA(args ...any) (Primitive, error) {
	count := 0

	// COUNTA counts all non-empty values regardless of type. this includes:
	// numbers, text, booleans, and errors (errors are counted, not propagated).
	for _, arg := range args {
		// Direct args that are errors should propagate
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				// COUNTA counts errors as non-empty cells, doesn't propagate them
				// count everything except nil (empty cells)
				if value != nil {
					count++
				}
			}
		} else {
			// Direct args are never nil
			count++
		}
	}

	return float64(count), nil
}

func (bf *BuiltInFunctions) MAX(args ...any) (Primitive, error) {
	max := math.Inf(-1)
	hasValues := false

	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := checkForError(value); err != nil {
					return nil, err
				}
				if num, ok := toNumber(value); ok && !math.IsNaN(num) {
					if num > max {
						max = num
					}
					hasValues = true
				}
			}
		} else {
			if num, ok := toNumber(arg); ok && !math.IsNaN(num) {
				if num > max {
					max = num
				}
				hasValues = true
			}
		}
	}

	if hasValues {
		return max, nil
	}
	return 0.0, nil
}

func (bf *BuiltInFunctions) MIN(args ...any) (Primitive, error) {
	min := math.Inf(1)
	hasValues := false

	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := checkForError(value); err != nil {
					return nil, err
				}
				if num, ok := toNumber(value); ok && !math.IsNaN(num) {
					if num < min {
						min = num
					}
					hasValues = true
				}
			}
		} else {
			if num, ok := toNumber(arg); ok && !math.IsNaN(num) {
				if num < min {
					min = num
				}
				hasValues = true
			}
		}
	}

	if hasValues {
		return min, nil
	}
	return 0.0, nil
}

func (bf *BuiltInFunctions) MEDIAN(args ...any) (Primitive, error) {
	values := []float64{}
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := checkForError(value); err != nil {
					return nil, err
				}
				if num, ok := toNumber(value); ok && !math.IsNaN(num) {
					values = append(values, num)
				}
			}
		} else {
			if num, ok := toNumber(arg); ok && !math.IsNaN(num) {
				values = append(values, num)
			}
		}
	}

	if len(values) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeNum, "MEDIAN has no numeric values")
	}

	// sort values
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}

	mid := len(values) / 2
	if len(values)%2 == 0 {
		// even count: average of two middle values
		return (values[mid-1] + values[mid]) / 2, nil
	}
	// odd count: middle value
	return values[mid], nil
}

func (bf *BuiltInFunctions) MODE(args ...any) (Primitive, error) {
	frequencyMap := make(map[float64]int)

	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}

		if r, ok := arg.(Range); ok {
			for value := range r.IterateValues() {
				if err := checkForError(value); err != nil {
					return nil, err
				}
				if num, ok := toNumber(value); ok && !math.IsNaN(num) {
					frequencyMap[num]++
				}
			}
		} else {
			if num, ok := toNumber(arg); ok && !math.IsNaN(num) {
				frequencyMap[num]++
			}
		}
	}

	if len(frequencyMap) == 0 {
		return nil, NewSpreadsheetError(ErrorCodeNum, "MODE has no numeric values")
	}

	// Find the maximum frequency
	maxFreq := 0
	for _, freq := range frequencyMap {
		if freq > maxFreq {
			maxFreq = freq
		}
	}

	// Collect all values with maximum frequency
	var modes []float64
	for value, freq := range frequencyMap {
		if freq == maxFreq {
			modes = append(modes, value)
		}
	}

	// If all values have the same frequency (no mode), return error
	if maxFreq == 1 && len(modes) == len(frequencyMap) {
		return nil, NewSpreadsheetError(ErrorCodeNA, "MODE: no value appears more than once")
	}

	// Sort modes for deterministic behavior
	for i := 0; i < len(modes); i++ {
		for j := i + 1; j < len(modes); j++ {
			if modes[j] < modes[i] {
				modes[i], modes[j] = modes[j], modes[i]
			}
		}
	}

	// Return the smallest mode (Excel-compatible behavior for ties)
	return modes[0], nil
}

func (bf *BuiltInFunctions) IF(args ...any) (Primitive, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "IF requires 2 or 3 arguments")
	}

	// Check for errors in condition before evaluating
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}

	condition := isTruthy(args[0])
	if condition {
		return args[1], nil
	}

	if len(args) == 3 {
		return args[2], nil
	}

	return false, nil
}

func (bf *BuiltInFunctions) AND(args ...any) (Primitive, error) {
	for _, arg := range args {
		// Check for errors before evaluating
		if err := checkForError(arg); err != nil {
			return nil, err
		}
		if !isTruthy(arg) {
			return false, nil
		}
	}
	return true, nil
}

func (bf *BuiltInFunctions) OR(args ...any) (Primitive, error) {
	for _, arg := range args {
		// Check for errors before evaluating
		if err := checkForError(arg); err != nil {
			return nil, err
		}
		if isTruthy(arg) {
			return true, nil
		}
	}
	return false, nil
}

func (bf *BuiltInFunctions) NOT(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "NOT requires exactly 1 argument")
	}
	// Check for errors before evaluating
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	return !isTruthy(args[0]), nil
}

func (bf *BuiltInFunctions) CONCATENATE(args ...any) (Primitive, error) {
	var result strings.Builder
	for _, arg := range args {
		// Check for errors before processing
		if err := checkForError(arg); err != nil {
			return nil, err
		}
		result.WriteString(toString(arg))
	}
	return result.String(), nil
}

func (bf *BuiltInFunctions) LEN(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "LEN requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	return float64(len(toString(args[0]))), nil
}

func (bf *BuiltInFunctions) UPPER(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "UPPER requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	return strings.ToUpper(toString(args[0])), nil
}

func (bf *BuiltInFunctions) LOWER(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "LOWER requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	return strings.ToLower(toString(args[0])), nil
}

func (bf *BuiltInFunctions) TRIM(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "TRIM requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	return strings.TrimSpace(toString(args[0])), nil
}

func (bf *BuiltInFunctions) ABS(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "ABS requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	num, ok := toNumber(args[0])
	if !ok {
		return nil, NewSpreadsheetError(ErrorCodeValue, "ABS requires a numeric argument")
	}
	return math.Abs(num), nil
}

func (bf *BuiltInFunctions) ROUND(args ...any) (Primitive, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "ROUND requires 1 or 2 arguments")
	}

	// Check for errors in all arguments
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}
	}

	num, ok := toNumber(args[0])
	if !ok {
		return nil, NewSpreadsheetError(ErrorCodeValue, "ROUND requires a numeric first argument")
	}

	places := 0.0
	if len(args) == 2 {
		places, ok = toNumber(args[1])
		if !ok {
			return nil, NewSpreadsheetError(ErrorCodeValue, "ROUND requires a numeric second argument")
		}
	}

	multiplier := math.Pow(10, places)
	return math.Round(num*multiplier) / multiplier, nil
}

func (bf *BuiltInFunctions) FLOOR(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "FLOOR requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	num, ok := toNumber(args[0])
	if !ok {
		return nil, NewSpreadsheetError(ErrorCodeValue, "FLOOR requires a numeric argument")
	}
	return math.Floor(num), nil
}

func (bf *BuiltInFunctions) CEILING(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "CEILING requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	num, ok := toNumber(args[0])
	if !ok {
		return nil, NewSpreadsheetError(ErrorCodeValue, "CEILING requires a numeric argument")
	}
	return math.Ceil(num), nil
}

func (bf *BuiltInFunctions) SQRT(args ...any) (Primitive, error) {
	if len(args) != 1 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "SQRT requires exactly 1 argument")
	}
	// Check for errors before processing
	if err := checkForError(args[0]); err != nil {
		return nil, err
	}
	num, ok := toNumber(args[0])
	if !ok {
		return nil, NewSpreadsheetError(ErrorCodeValue, "SQRT requires a numeric argument")
	}
	if num < 0 {
		return nil, NewSpreadsheetError(ErrorCodeNum, "SQRT requires a non-negative argument")
	}
	return math.Sqrt(num), nil
}

func (bf *BuiltInFunctions) POWER(args ...any) (Primitive, error) {
	if len(args) != 2 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "POWER requires exactly 2 arguments")
	}
	// Check for errors in all arguments
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}
	}
	base, ok1 := toNumber(args[0])
	exp, ok2 := toNumber(args[1])
	if !ok1 || !ok2 {
		return nil, NewSpreadsheetError(ErrorCodeValue, "POWER requires numeric arguments")
	}
	return math.Pow(base, exp), nil
}

func (bf *BuiltInFunctions) MOD(args ...any) (Primitive, error) {
	if len(args) != 2 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "MOD requires exactly 2 arguments")
	}
	// Check for errors in all arguments
	for _, arg := range args {
		if err := checkForError(arg); err != nil {
			return nil, err
		}
	}
	dividend, ok1 := toNumber(args[0])
	divisor, ok2 := toNumber(args[1])
	if !ok1 || !ok2 {
		return nil, NewSpreadsheetError(ErrorCodeValue, "MOD requires numeric arguments")
	}
	if divisor == 0 {
		return nil, NewSpreadsheetError(ErrorCodeDiv0, "Division by zero")
	}
	return math.Mod(dividend, divisor), nil
}

func (bf *BuiltInFunctions) PI(args ...any) (Primitive, error) {
	if len(args) != 0 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "PI takes no arguments")
	}
	return math.Pi, nil
}

// Excel date/time constants
const (
	// Excel epoch: January 1, 1900 00:00:00 UTC in Unix milliseconds
	// Note: Excel incorrectly treats 1900 as a leap year, but we'll use the
	// standard calculation
	EXCEL_EPOCH_MS = -2209075200000 // corrected: December 30, 1899 00:00:00 UTC
	MS_PER_DAY     = 86400000       // milliseconds in a day
)

func (bf *BuiltInFunctions) NOW(args ...any) (Primitive, error) {
	if len(args) != 0 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "NOW takes no arguments")
	}
	// return current time as Excel serial number (days since Excel epoch)
	now := bf.clock.Now()
	diffMs := float64(now.UnixMilli() - EXCEL_EPOCH_MS)
	return diffMs / MS_PER_DAY, nil
}

func (bf *BuiltInFunctions) TODAY(args ...any) (Primitive, error) {
	if len(args) != 0 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "TODAY takes no arguments")
	}
	now := bf.clock.Now()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	diffMs := float64(midnight.UnixMilli() - EXCEL_EPOCH_MS)
	return math.Floor(diffMs / MS_PER_DAY), nil
}

func (bf *BuiltInFunctions) RAND(args ...any) (Primitive, error) {
	if len(args) != 0 {
		return nil, NewSpreadsheetError(ErrorCodeNA, "RAND takes no arguments")
	}
	return bf.rng.Float64(), nil
}

func (r RangeAddress) Contains(worksheetID uint32, row, col uint32) bool {
	return r.WorksheetID == worksheetID &&
		row >= r.StartRow && row <= r.EndRow &&
		col >= r.StartColumn && col <= r.EndColumn
}

// isVolatileFunction returns true if the function should trigger recalculation
// on every Calculate() call
func isVolatileFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "NOW", "TODAY", "RAND":
		return true
	default:
		return false
	}
}

// toNumber converts value to number, returning ok=false if conversion fails
func toNumber(value Primitive) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case bool:
		if v {
			return 1, true
		}
		return 0, true
	case string:
		num, err := strconv.ParseFloat(v, 64) // Parse as 64-bit float
		if err != nil {
			return 0, false
		}
		return num, true
	case nil:
		return 0, true
	default:
		return 0, false
	}
}

// toString converts value to string
func toString(value Primitive) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

// isTruthy checks if value is truthy
func isTruthy(value Primitive) bool {
	switch v := value.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case int:
		return v != 0
	case string:
		return v != ""
	case nil:
		return false
	default:
		return true
	}
}
