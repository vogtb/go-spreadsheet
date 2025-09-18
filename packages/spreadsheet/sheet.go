package main

import (
	"fmt"
	"sort"
	"strings"
)

// AppErrorCode represents gRPC-style error codes for application-level errors.
// note that we are skipping error codes that don't make sense for our use-case,
// like unauthenticated, or permission denied.
type AppErrorCode int

const (
	// OK indicates the operation completed successfully.
	OK AppErrorCode = 0

	// Unknown error. Errors raised by APIs that do not return enough error
	// information may be converted to this error.
	Unknown AppErrorCode = 2

	// InvalidArgument indicates client specified an invalid argument.
	InvalidArgument AppErrorCode = 3

	// NotFound means some requested entity (e.g., worksheet or named range)
	// was not found.
	NotFound AppErrorCode = 5

	// AlreadyExists means an attempt to create an entity failed because one
	// already exists.
	AlreadyExists AppErrorCode = 6

	// ResourceExhausted indicates some resource has been exhausted, perhaps
	// a per-user quota, or perhaps the entire file system is out of space.
	ResourceExhausted AppErrorCode = 8

	// FailedPrecondition indicates operation was rejected because the
	// system is not in a state required for the operation's execution.
	FailedPrecondition AppErrorCode = 9

	// OutOfRange means operation was attempted past the valid range.
	OutOfRange AppErrorCode = 11

	// Unimplemented indicates operation is not implemented or not
	// supported/enabled in this service.
	Unimplemented AppErrorCode = 12

	// Internal errors. Means some invariants expected by underlying
	// system has been broken.
	Internal AppErrorCode = 13
)

// AppError represents errors at the application level (not
// spreadsheet formula errors)
type AppError struct {
	Code    AppErrorCode
	Message string
}

func (e *AppError) Error() string {
	return e.Message
}

// NewApplicationError creates a new application error
func NewApplicationError(code AppErrorCode, message string) *AppError {
	return &AppError{
		Code:    code,
		Message: message,
	}
}

// Spreadsheet is the main spreadsheet class that combines storage, parsing,
// dependency tracking, and formula evaluation into a unified API
type Spreadsheet struct {
	storage          *Storage
	calculationStack *CalculationStack
	functions        *BuiltInFunctions
	currentAddress   CellAddress
}

// NewSpreadsheet creates a new spreadsheet instance
func NewSpreadsheet() *Spreadsheet {
	storage := &Storage{
		worksheets:      NewWorksheetTable(),
		namedRanges:     NewNamedRangeTable(),
		strings:         NewStringTable(),
		formulas:        NewFormulaTable(),
		dependencyGraph: NewDependencyGraph(),
	}

	return &Spreadsheet{
		storage:          storage,
		calculationStack: NewCalculationStack(),
		functions:        NewDefaultBuiltInFunctions(),
	}
}

// resolveAddress parses a cell address and resolves it to worksheet ID, row, and column
// Returns worksheet ID (0 for unknown), row and column indices (0-based), or an error
func (s *Spreadsheet) resolveAddress(address string) (worksheetID uint32, row uint32, col uint32, err error) {
	// create a parser with context that can resolve worksheet names
	parser := NewParserWithContext(&ParserContext{
		CurrentWorksheetID: 0, // no current worksheet context for standalone address resolution
		CurrentRow:         0,
		CurrentColumn:      0,
		ResolveWorksheet:   s.resolveWorksheetByName, // use spreadsheet's worksheet resolution
	})

	// use the parseFullAddress method to get worksheet ID and
	// absolute coordinates
	worksheetID, rowInt32, colInt32, err := parser.parseFullAddress(address)
	if err != nil {
		return 0, 0, 0, NewApplicationError(InvalidArgument, fmt.Sprintf("Invalid address: %v", err))
	}

	// convert from int32 to uint32
	row = uint32(rowInt32)
	col = uint32(colInt32)

	return worksheetID, row, col, nil
}

// resolveWorksheetByName resolves a worksheet name to its ID
func (s *Spreadsheet) resolveWorksheetByName(name string) uint32 {
	worksheet, exists := s.storage.worksheets.GetWorksheetByName(name)
	if !exists {
		return 0 // return 0 for non-existent worksheets
	}
	return worksheet.worksheetID
}

type SpreadsheetInterface interface {
	// cell methods

	Get(address string) (Primitive, error)
	Set(address string, value Primitive) error
	Remove(address string) error

	// worksheet methods

	AddWorksheet(name string) error
	RemoveWorksheet(name string) error
	RenameWorksheet(oldName string, newName string) error
	DoesWorksheetExist(name string) bool
	ListWorksheets() []string
	ListReferencedWorksheets() []string

	// named range methods

	AddNamedRange(name string) error
	RemoveNamedRange(name string) error
	RenameNamedRange(oldName string, newName string) error
	DoesNamedRangeExist(name string) bool
	ListNamedRanges() []string
	ListReferencedNamedRanges() []string

	// common methods

	Calculate() error
}

// Implementation of SpreadsheetInterface

var _ SpreadsheetInterface = (*Spreadsheet)(nil)

// Get retrieves the value of a cell
func (s *Spreadsheet) Get(address string) (Primitive, error) {
	worksheetID, row, col, err := s.resolveAddress(address)
	if err != nil {
		return nil, err
	}

	// handle unknown worksheet (ID = 0)
	if worksheetID == 0 {
		return NewSpreadsheetError(ErrorCodeValue, "Worksheet not found"), nil
	}

	// get worksheet by ID
	worksheet, exists := s.storage.worksheets.GetWorksheet(worksheetID)
	if !exists {
		return nil, nil // return nil for non-existent worksheet
	}

	// get cell
	cell := worksheet.GetCell(row, col)
	if cell == nil {
		return nil, nil
	}

	return cell.Value, nil
}

// Set sets the value of a cell
func (s *Spreadsheet) Set(address string, value Primitive) error {
	// first, try to handle the special case
	// "WorksheetA!WorksheetB!CellRef" -> "WorksheetB!CellRef"
	originalAddress := address
	parts := strings.Split(address, "!")
	if len(parts) == 3 {
		// try corrected address "WorksheetB!CellRef"
		address = parts[1] + "!" + parts[2]
	}

	worksheetID, row, col, err := s.resolveAddress(address)
	if err != nil {
		// if correction failed, try original error handling
		if appErr, ok := err.(*AppError); ok && appErr.Code == InvalidArgument {
			// extract worksheet name from original address (before first !)
			if exclamationIdx := strings.Index(originalAddress, "!"); exclamationIdx > 0 {
				worksheetName := originalAddress[:exclamationIdx]
				// ensure worksheet exists
				if !s.storage.worksheets.Contains(worksheetName) {
					worksheet := NewWorksheet(s.storage, 0)
					worksheetID = s.storage.worksheets.DefineWorksheet(worksheetName, worksheet)
					worksheet.worksheetID = worksheetID
				} else {
					ws, _ := s.storage.worksheets.GetWorksheetByName(worksheetName)
					worksheetID = ws.worksheetID
				}
				// store error in A1 (0,0) of the worksheet
				worksheet, _ := s.storage.worksheets.GetWorksheet(worksheetID)
				worksheet.SetCell(0, 0, NewSpreadsheetError(ErrorCodeRef, "Invalid address format"), "")
				return nil
			}
		}
		return err
	}

	// handle unknown worksheet (ID = 0)
	if worksheetID == 0 {
		return NewApplicationError(InvalidArgument, "Cannot set cell on unknown worksheet")
	}

	// get worksheet by ID
	worksheet, exists := s.storage.worksheets.GetWorksheet(worksheetID)
	if !exists {
		return NewApplicationError(InvalidArgument, fmt.Sprintf("Worksheet with ID %d not found", worksheetID))
	}

	cellAddr := CellAddress{
		WorksheetID: worksheetID,
		Row:         row,
		Column:      col,
	}

	// check if value is a formula (starts with =)
	var formula string
	if str, ok := value.(string); ok && len(str) > 0 && str[0] == '=' {
		formula = str // keep the = sign for the lexer
		value = nil   // formula cells don't have a direct value

		// parse the formula
		lexer := NewLexer(formula)
		tokens, lexErrors := lexer.Tokenize()
		if len(lexErrors) > 0 {
			// check if this is an invalid range reference (cross-worksheet range)
			// or worksheet reference
			errorMsg := strings.Join(lexErrors, "; ")
			if strings.Contains(errorMsg, "invalid range reference") || strings.Contains(errorMsg, "invalid cell reference after worksheet") {
				worksheet.SetCell(row, col, NewSpreadsheetError(ErrorCodeRef, errorMsg), "")
			} else {
				// store error in cell
				worksheet.SetCell(row, col, NewSpreadsheetError(ErrorCodeValue, errorMsg), "")
			}
			return nil
		}

		parserContext := &ParserContext{
			CurrentWorksheetID: worksheet.worksheetID,
			CurrentRow:         int32(row),
			CurrentColumn:      int32(col),
			ResolveWorksheet: func(name string) uint32 {
				id, _ := s.storage.worksheets.GetWorksheetID(name)
				if id == 0 {
					// intern the worksheet name for future reference
					id = s.storage.worksheets.InternWorksheet(name)
				}
				return id
			},
		}

		parser := NewParser(tokens, parserContext)
		ast, parseErr := parser.Parse()
		if parseErr != nil {
			// check if this is a REF error for cross-worksheet ranges
			if strings.HasPrefix(parseErr.Error(), "REF:") {
				worksheet.SetCell(row, col, NewSpreadsheetError(ErrorCodeRef, strings.TrimPrefix(parseErr.Error(), "REF: ")), "")
			} else {
				// store error in cell
				worksheet.SetCell(row, col, NewSpreadsheetError(ErrorCodeValue, parseErr.Error()), "")
			}
			return nil
		}

		// intern the formula
		formulaID := s.storage.formulas.InternFormula(ast, cellAddr)

		// extract dependencies from AST and update dependency graph
		s.extractDependencies(ast, cellAddr)

		// mark this cell as having a formula in the dependency graph
		s.storage.dependencyGraph.SetFormula(cellAddr, formula)

		// store formula ID in cell
		worksheet.SetCell(row, col, nil, formula)

		// store formula ID directly in chunk
		chunkRow := row / ChunkRows
		chunkCol := col / ChunkCols
		localRow := row % ChunkRows
		localCol := col % ChunkCols
		chunk := worksheet.getChunk(chunkRow, chunkCol)
		idx := localCol*ChunkRows + localRow
		if chunk.FormulaIDs == nil {
			chunk.FormulaIDs = make([]uint32, ChunkSize)
		}
		chunk.FormulaIDs[idx] = formulaID

		// mark cell as dirty for calculation
		s.storage.dependencyGraph.MarkDirty(cellAddr)
	} else {
		// Clear any existing dependencies
		s.storage.dependencyGraph.ClearDependencies(cellAddr)

		// Set the value
		worksheet.SetCell(row, col, value, "")

		// Mark dependent cells as dirty - non-formula cells need immediate propagation
		s.storage.dependencyGraph.MarkCellIfInRangeDirty(cellAddr)
		for _, dep := range s.storage.dependencyGraph.GetDirectDependents(cellAddr) {
			s.storage.dependencyGraph.MarkDirty(dep)
		}
	}

	return nil
}

// Remove removes a cell
func (s *Spreadsheet) Remove(address string) error {
	worksheetID, row, col, err := s.resolveAddress(address)
	if err != nil {
		return err
	}

	// handle unknown worksheet (ID = 0)
	if worksheetID == 0 {
		return nil // Nothing to remove from unknown worksheet
	}

	// get worksheet by ID
	worksheet, exists := s.storage.worksheets.GetWorksheet(worksheetID)
	if !exists {
		return nil // nothing to remove
	}

	cellAddr := CellAddress{
		WorksheetID: worksheetID,
		Row:         row,
		Column:      col,
	}

	// get dependents before clearing dependencies
	dependents := s.storage.dependencyGraph.GetDirectDependents(cellAddr)

	// clear dependencies
	s.storage.dependencyGraph.ClearDependencies(cellAddr)

	// remove the cell
	worksheet.RemoveCell(row, col)

	// mark dependent cells as dirty - removed cells affect their dependents
	s.storage.dependencyGraph.MarkCellIfInRangeDirty(cellAddr)
	for _, dep := range dependents {
		s.storage.dependencyGraph.MarkDirty(dep)
	}

	// remove from dependency graph
	s.storage.dependencyGraph.RemoveNode(cellAddr)

	return nil
}

// AddWorksheet adds a new worksheet
func (s *Spreadsheet) AddWorksheet(name string) error {
	if s.storage.worksheets.Contains(name) {
		return NewApplicationError(AlreadyExists, "Worksheet already exists")
	}

	worksheet := NewWorksheet(s.storage, 0)
	worksheetID := s.storage.worksheets.DefineWorksheet(name, worksheet)
	worksheet.worksheetID = worksheetID

	return nil
}

// RemoveWorksheet removes a worksheet
func (s *Spreadsheet) RemoveWorksheet(name string) error {
	if !s.storage.worksheets.Contains(name) {
		return NewApplicationError(NotFound, "Worksheet not found")
	}

	// get the worksheet ID before removing
	worksheet, _ := s.storage.worksheets.GetWorksheetByName(name)
	worksheetID := worksheet.worksheetID

	// mark all cells that depend on this worksheet as dirty. we need to check
	// all nodes in the dependency graph
	for cellAddr, node := range s.storage.dependencyGraph.nodes {
		// check cell precedents
		for precedentAddr := range node.CellPrecedents {
			if precedentAddr.WorksheetID == worksheetID {
				s.storage.dependencyGraph.MarkDirty(cellAddr)
				break
			}
		}

		// check range precedents
		for rangeAddr := range node.RangePrecedents {
			if rangeAddr.WorksheetID == worksheetID {
				s.storage.dependencyGraph.MarkDirty(cellAddr)
				break
			}
		}
	}

	// remove all cells from the removed worksheet from the dependency graph. this
	// prevents them from being in the dirty set
	cellsToRemove := []CellAddress{}
	for cellAddr := range s.storage.dependencyGraph.nodes {
		if cellAddr.WorksheetID == worksheetID {
			cellsToRemove = append(cellsToRemove, cellAddr)
		}
	}
	for _, cellAddr := range cellsToRemove {
		s.storage.dependencyGraph.RemoveNode(cellAddr)
	}

	s.storage.worksheets.UndefineWorksheet(name)
	return nil
}

// RenameWorksheet renames a worksheet
func (s *Spreadsheet) RenameWorksheet(oldName string, newName string) error {
	if !s.storage.worksheets.Contains(oldName) {
		return NewApplicationError(NotFound, "Worksheet not found")
	}

	if s.storage.worksheets.Contains(newName) {
		return NewApplicationError(AlreadyExists, "Worksheet name already exists")
	}

	worksheet, _ := s.storage.worksheets.GetWorksheetByName(oldName)

	s.storage.worksheets.UndefineWorksheet(oldName)

	s.storage.worksheets.DefineWorksheet(newName, worksheet)

	return nil
}

// DoesWorksheetExist checks if a worksheet exists
func (s *Spreadsheet) DoesWorksheetExist(name string) bool {
	id, exists := s.storage.worksheets.GetWorksheetID(name)
	return exists && s.storage.worksheets.IsWorksheetDefined(id)
}

// ListWorksheets returns all defined worksheet names
func (s *Spreadsheet) ListWorksheets() []string {
	worksheets := s.storage.worksheets.GetAllDefinedWorksheets()
	result := make([]string, 0, len(worksheets))
	for name := range worksheets {
		result = append(result, name)
	}
	return result
}

// ListReferencedWorksheets returns all referenced but undefined worksheet names
func (s *Spreadsheet) ListReferencedWorksheets() []string {
	return s.storage.worksheets.GetAllUndefinedWorksheets()
}

// AddNamedRange adds a named range
func (s *Spreadsheet) AddNamedRange(name string) error {
	if s.storage.namedRanges.Contains(name) {
		return NewApplicationError(AlreadyExists, "Named range already exists")
	}

	// For now, just intern the name without defining it
	s.storage.namedRanges.InternNamedRange(name)
	return nil
}

// RemoveNamedRange removes a named range
func (s *Spreadsheet) RemoveNamedRange(name string) error {
	if !s.storage.namedRanges.Contains(name) {
		return NewApplicationError(NotFound, "Named range not found")
	}

	s.storage.namedRanges.UndefineNamedRange(name)
	return nil
}

// RenameNamedRange renames a named range
func (s *Spreadsheet) RenameNamedRange(oldName string, newName string) error {
	if !s.storage.namedRanges.Contains(oldName) {
		return NewApplicationError(NotFound, "Named range not found")
	}

	if s.storage.namedRanges.Contains(newName) {
		return NewApplicationError(AlreadyExists, "Named range already exists")
	}

	// Get the range address if defined
	id, _ := s.storage.namedRanges.GetNamedRangeID(oldName)
	rangeAddr, isDefined := s.storage.namedRanges.GetRangeAddress(id)

	// Remove old name
	s.storage.namedRanges.UndefineNamedRange(oldName)

	// Add with new name
	if isDefined {
		s.storage.namedRanges.DefineNamedRange(newName, rangeAddr)
	} else {
		s.storage.namedRanges.InternNamedRange(newName)
	}

	return nil
}

// DoesNamedRangeExist checks if a named range exists
func (s *Spreadsheet) DoesNamedRangeExist(name string) bool {
	id, exists := s.storage.namedRanges.GetNamedRangeID(name)
	return exists && s.storage.namedRanges.IsRangeDefined(id)
}

// ListNamedRanges returns all defined named range names
func (s *Spreadsheet) ListNamedRanges() []string {
	ranges := s.storage.namedRanges.GetAllDefinedRanges()
	result := make([]string, 0, len(ranges))
	for name := range ranges {
		result = append(result, name)
	}
	return result
}

// ListReferencedNamedRanges returns all referenced but undefined named range names
func (s *Spreadsheet) ListReferencedNamedRanges() []string {
	return s.storage.namedRanges.GetAllUndefinedRanges()
}

// Calculate recalculates all dirty cells in the spreadsheet
func (s *Spreadsheet) Calculate() error {
	// mark all volatile cells as dirty (they should always be recalculated)
	s.storage.dependencyGraph.MarkAllVolatileDirty()

	// reset calculation stack
	s.calculationStack.reset()

	// keep processing while there are dirty cells
	for len(s.storage.dependencyGraph.dirtySet) > 0 {
		// Collect all dirty cells
		var dirtyCells []CellAddress
		for addr := range s.storage.dependencyGraph.dirtySet {
			dirtyCells = append(dirtyCells, addr)
		}

		// Sort cells for deterministic order (by worksheet, then row, then column)
		sort.Slice(dirtyCells, func(i, j int) bool {
			if dirtyCells[i].WorksheetID != dirtyCells[j].WorksheetID {
				return dirtyCells[i].WorksheetID < dirtyCells[j].WorksheetID
			}
			if dirtyCells[i].Row != dirtyCells[j].Row {
				return dirtyCells[i].Row < dirtyCells[j].Row
			}
			return dirtyCells[i].Column < dirtyCells[j].Column
		})

		// process each dirty cell in sorted order
		for _, cellAddr := range dirtyCells {
			// skip if not dirty or already calculated
			if _, isDirty := s.storage.dependencyGraph.dirtySet[cellAddr]; !isDirty {
				continue
			}

			if s.calculationStack.isCompleted(cellAddr) {
				// already calculated, remove from dirty set
				s.storage.dependencyGraph.ClearDirty(cellAddr)
				continue
			}

			// calculate this cell
			if err := s.calculateCell(cellAddr); err != nil {
				// only REF errors should be returned from calculateCell and they've
				// already been stored in the cell. error already stored in cell
				continue
			}
		}
	}

	// clear all dirty flags (should already be empty but ensure clean state)
	s.storage.dependencyGraph.ClearAllDirty()

	return nil
}

// calculateCell calculates a single cell and its dependencies
func (s *Spreadsheet) calculateCell(cellAddr CellAddress) error {
	if s.calculationStack.isCompleted(cellAddr) {
		return nil
	}

	if s.calculationStack.isProcessing(cellAddr) {
		// cell is already being calculated, we have a circular reference
		return NewSpreadsheetError(ErrorCodeRef, "Circular reference detected")
	}

	// push to stack (processing this cell)
	s.calculationStack.push(cellAddr)
	defer func() {
		s.calculationStack.pop()
		s.calculationStack.markCompleted(cellAddr)
	}()

	// get worksheet
	worksheet, exists := s.storage.worksheets.GetWorksheet(cellAddr.WorksheetID)
	if !exists {
		return nil // worksheet not found
	}

	cell := worksheet.GetCell(cellAddr.Row, cellAddr.Column)
	if cell == nil || cell.FormulaID == 0 {
		// no formula to calculate - non-formula cells don't need propagation
		// they only change when explicitly set, not during calculation
		s.storage.dependencyGraph.ClearDirty(cellAddr)
		return nil
	}

	// get formula AST
	ast, exists := s.storage.formulas.GetAST(cell.FormulaID)
	if !exists {
		return nil // formula not found... should not happen
	}

	// check for circular reference through ranges. a cell cannot depend on a
	// range that includes itself
	rangePrecedents := s.storage.dependencyGraph.GetRangePrecedents(cellAddr)
	for _, rangeAddr := range rangePrecedents {
		if s.storage.dependencyGraph.IsInRange(cellAddr, rangeAddr) {
			// cell depends on a range that includes itself - circular reference
			circularErr := NewSpreadsheetError(ErrorCodeRef, "Circular reference detected")
			worksheet.SetFormulaResult(cellAddr.Row, cellAddr.Column, circularErr)
			s.storage.dependencyGraph.ClearDirty(cellAddr)
			return circularErr
		}
	}

	// calculate cell dependencies first
	precedents := s.storage.dependencyGraph.GetDirectPrecedents(cellAddr)
	for _, precedent := range precedents {
		if err := s.calculateCell(precedent); err != nil {
			// circular reference detected - store it in this cell and propagate
			if spreadsheetErr, ok := err.(*SpreadsheetError); ok && spreadsheetErr.ErrorCode == ErrorCodeRef {
				// store REF error from precedent
				worksheet.SetFormulaResult(cellAddr.Row, cellAddr.Column, spreadsheetErr)
				s.storage.dependencyGraph.ClearDirty(cellAddr)
				return spreadsheetErr
			}
			// non-REF errors shouldn't be returned from calculateCell
			// should be stored in the cell and propagate through formula evaluation
		}
	}

	// calculate range dependencies - ensure all cells in ranges we depend on are calculated
	rangePrecedents = s.storage.dependencyGraph.GetRangePrecedents(cellAddr)
	for _, rangeAddr := range rangePrecedents {
		// calculate all cells in the range in deterministic order
		for row := rangeAddr.StartRow; row <= rangeAddr.EndRow; row++ {
			for col := rangeAddr.StartColumn; col <= rangeAddr.EndColumn; col++ {
				rangeCell := CellAddress{
					WorksheetID: rangeAddr.WorksheetID,
					Row:         row,
					Column:      col,
				}
				// only calculate if it's dirty and not already being processed
				if _, isDirty := s.storage.dependencyGraph.dirtySet[rangeCell]; isDirty {
					if err := s.calculateCell(rangeCell); err != nil {
						// handle circular reference errors
						if spreadsheetErr, ok := err.(*SpreadsheetError); ok && spreadsheetErr.ErrorCode == ErrorCodeRef {
							// don't propagate REF errors from range cells,
							// they'll be picked up during evaluation
						}
					}
				}
			}
		}
	}

	// set current addr for relative references (after calculating dependencies)
	s.currentAddress = cellAddr

	// evaluate the formula
	result, err := ast.Eval(s)
	if err != nil {
		// store error in cell
		if spreadsheetErr, ok := err.(*SpreadsheetError); ok {
			// store formula evaluation error
			worksheet.SetFormulaResult(cellAddr.Row, cellAddr.Column, spreadsheetErr)
		} else {
			worksheet.SetFormulaResult(cellAddr.Row, cellAddr.Column,
				NewSpreadsheetError(ErrorCodeValue, err.Error()))
		}
		return nil // continue calculating other cells
	}

	// check if result is an error (e.g. from a cell reference that has an error)
	if spreadsheetErr, ok := result.(*SpreadsheetError); ok {
		// Formula result is an error
		worksheet.SetFormulaResult(cellAddr.Row, cellAddr.Column, spreadsheetErr)
		return nil
	}

	// store result (handle nil results as 0)
	if result == nil {
		result = 0.0
	}
	worksheet.SetFormulaResult(cellAddr.Row, cellAddr.Column, result)

	// clear dirty flag
	s.storage.dependencyGraph.ClearDirty(cellAddr)

	// mark direct dependents as dirty (lazy propagation)
	dependents := s.storage.dependencyGraph.GetDirectDependents(cellAddr)
	for _, dep := range dependents {
		s.storage.dependencyGraph.MarkDirty(dep)
	}

	return nil
}

// extractDependencies extracts cell and range dependencies from an AST
func (s *Spreadsheet) extractDependencies(node ASTNode, cellAddr CellAddress) {
	if node == nil {
		return
	}

	// clear existing dependencies and volatile status
	s.storage.dependencyGraph.ClearDependencies(cellAddr)
	s.storage.dependencyGraph.UnmarkVolatile(cellAddr)

	// recursively extract dependencies
	s.extractDependenciesRecursive(node, cellAddr)
}

// extractDependenciesRecursive recursively extracts dependencies from AST nodes
func (s *Spreadsheet) extractDependenciesRecursive(node ASTNode, cellAddr CellAddress) {
	switch n := node.(type) {
	case *CellRefNode:
		// calculate absolute address from relative offset
		targetRow := int32(cellAddr.Row) + n.RowOffset
		targetCol := int32(cellAddr.Column) + n.ColOffset

		if targetRow >= 0 && targetCol >= 0 {
			targetAddr := CellAddress{
				WorksheetID: n.WorksheetID,
				Row:         uint32(targetRow),
				Column:      uint32(targetCol),
			}
			if n.WorksheetID == 0 {
				targetAddr.WorksheetID = cellAddr.WorksheetID
			}
			s.storage.dependencyGraph.AddCellDependency(cellAddr, targetAddr)
		}

	case *RangeNode:
		// calculate absolute range from relative offsets
		startRow := int32(cellAddr.Row) + n.StartRowOffset
		startCol := int32(cellAddr.Column) + n.StartColOffset
		endRow := int32(cellAddr.Row) + n.EndRowOffset
		endCol := int32(cellAddr.Column) + n.EndColOffset

		if startRow >= 0 && startCol >= 0 && endRow >= 0 && endCol >= 0 {
			rangeAddr := RangeAddress{
				WorksheetID: n.WorksheetID,
				StartRow:    uint32(startRow),
				StartColumn: uint32(startCol),
				EndRow:      uint32(endRow),
				EndColumn:   uint32(endCol),
			}
			if n.WorksheetID == 0 {
				rangeAddr.WorksheetID = cellAddr.WorksheetID
			}
			s.storage.dependencyGraph.AddRangeDependency(cellAddr, rangeAddr)
		}

	case *BinaryOpNode:
		s.extractDependenciesRecursive(n.Left, cellAddr)
		s.extractDependenciesRecursive(n.Right, cellAddr)

	case *UnaryOpNode:
		s.extractDependenciesRecursive(n.Operand, cellAddr)

	case *FunctionCallNode:
		// check if this function is volatile
		if isVolatileFunction(n.Name) {
			s.storage.dependencyGraph.MarkVolatile(cellAddr)
		}
		for _, arg := range n.Args {
			s.extractDependenciesRecursive(arg, cellAddr)
		}

	case *NamedRangeNode:
		// track named range usage
		if s.storage.formulas != nil && s.storage.namedRanges != nil {
			formulaID, exists := s.storage.formulas.formulaAtCell[cellAddr]
			if exists {
				// get or intern the named range ID
				nameID := s.storage.namedRanges.InternNamedRange(n.Name)
				s.storage.formulas.TrackNamedRangeReference(formulaID, nameID)
			}
		}

	case *StringNode, *NumberNode, *BooleanNode:
		// literal nodes don't have dependencies
	}
}

// GetCurrentAddress returns the current cell address being calculated
func (s *Spreadsheet) GetCurrentAddress() CellAddress {
	return s.currentAddress
}

// GetWorksheet returns a worksheet by name for diagnostic purposes
func (s *Spreadsheet) GetWorksheet(name string) (*Worksheet, bool) {
	return s.storage.worksheets.GetWorksheetByName(name)
}

// GetDependencyGraph returns the dependency graph for diagnostic purposes
func (s *Spreadsheet) GetDependencyGraph() *DependencyGraph {
	return s.storage.dependencyGraph
}

// CalculationStack manages the stack-based calculation order
type CalculationStack struct {
	items      []CellAddress            // stack of cells to process
	processing map[CellAddress]struct{} // currently being processed (cycle detection)
	completed  map[CellAddress]struct{} // already calculated in this pass
}

// NewCalculationStack creates a new calculation stack
func NewCalculationStack() *CalculationStack {
	return &CalculationStack{
		items:      make([]CellAddress, 0),
		processing: make(map[CellAddress]struct{}),
		completed:  make(map[CellAddress]struct{}),
	}
}

// push adds a cell to the stack
func (cs *CalculationStack) push(addr CellAddress) {
	cs.items = append(cs.items, addr)
	cs.processing[addr] = struct{}{}
}

// pop removes and returns the top cell from the stack
func (cs *CalculationStack) pop() (CellAddress, bool) {
	if len(cs.items) == 0 {
		return CellAddress{}, false
	}
	addr := cs.items[len(cs.items)-1]
	cs.items = cs.items[:len(cs.items)-1]
	delete(cs.processing, addr)
	return addr, true
}

// isProcessing checks if a cell is currently being processed
func (cs *CalculationStack) isProcessing(addr CellAddress) bool {
	_, exists := cs.processing[addr]
	return exists
}

// markCompleted marks a cell as calculated
func (cs *CalculationStack) markCompleted(addr CellAddress) {
	cs.completed[addr] = struct{}{}
}

// isCompleted checks if a cell has been calculated
func (cs *CalculationStack) isCompleted(addr CellAddress) bool {
	_, exists := cs.completed[addr]
	return exists
}

// reset clears the stack
func (cs *CalculationStack) reset() {
	cs.items = cs.items[:0]
	cs.processing = make(map[CellAddress]struct{})
	cs.completed = make(map[CellAddress]struct{})
}

// RunnableSpreadsheet provides a chainable interface for
// spreadsheet operations. wraps the standard Spreadsheet and tracks
// errors internally
type RunnableSpreadsheet struct {
	spreadsheet *Spreadsheet
	err         error
	printLn     func(string)
}

// NewRunnableSpreadsheet creates a new RunnableSpreadsheet. printLn is
// required and will be used for all logging operations (Log, CheckError)
func NewRunnableSpreadsheet(printLn func(string)) *RunnableSpreadsheet {
	return &RunnableSpreadsheet{
		spreadsheet: NewSpreadsheet(),
		err:         nil,
		printLn:     printLn,
	}
}

// Set sets a cell value (chainable)
func (r *RunnableSpreadsheet) Set(address string, value Primitive) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.Set(address, value)
	return r
}

// Get retrieves a cell value (chainable)
func (r *RunnableSpreadsheet) Get(address string) (*RunnableSpreadsheet, Primitive) {
	if r.err != nil {
		return r, nil // no-op if there's already an error
	}
	val, err := r.spreadsheet.Get(address)
	if err != nil {
		r.err = err
	}
	return r, val
}

// Remove removes a cell (chainable)
func (r *RunnableSpreadsheet) Remove(address string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.Remove(address)
	return r
}

// AddWorksheet adds a new worksheet (chainable)
func (r *RunnableSpreadsheet) AddWorksheet(name string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.AddWorksheet(name)
	return r
}

// RemoveWorksheet removes a worksheet (chainable)
func (r *RunnableSpreadsheet) RemoveWorksheet(name string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.RemoveWorksheet(name)
	return r
}

// RenameWorksheet renames a worksheet (chainable)
func (r *RunnableSpreadsheet) RenameWorksheet(oldName, newName string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.RenameWorksheet(oldName, newName)
	return r
}

// AddNamedRange adds a named range (chainable)
func (r *RunnableSpreadsheet) AddNamedRange(name string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.AddNamedRange(name)
	return r
}

// RemoveNamedRange removes a named range (chainable)
func (r *RunnableSpreadsheet) RemoveNamedRange(name string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.RemoveNamedRange(name)
	return r
}

// RenameNamedRange renames a named range (chainable)
func (r *RunnableSpreadsheet) RenameNamedRange(oldName, newName string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.RenameNamedRange(oldName, newName)
	return r
}

// Calculate recalculates all formulas (chainable)
func (r *RunnableSpreadsheet) Calculate() *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}
	r.err = r.spreadsheet.Calculate()
	return r
}

// Run executes a final calculation and returns the spreadsheet and any error.
// typically the last method in the chain
func (r *RunnableSpreadsheet) Run() (*Spreadsheet, error) {
	if r.err != nil {
		return nil, r.err
	}

	// final calculation to ensure all formulas are up to date
	r.err = r.spreadsheet.Calculate()
	if r.err != nil {
		return nil, r.err
	}

	return r.spreadsheet, nil
}

// RunOrPanic executes a final calculation and panics if there's an
// error. useful for examples and tests where you want to fail fast
func (r *RunnableSpreadsheet) RunOrPanic() *Spreadsheet {
	spreadsheet, err := r.Run()
	if err != nil {
		panic(err)
	}
	return spreadsheet
}

// Error returns the current error state
func (r *RunnableSpreadsheet) Error() error {
	return r.err
}

// CheckError logs the current error using the PrintLn function (chainable)
func (r *RunnableSpreadsheet) CheckError() *RunnableSpreadsheet {
	if r.err != nil {
		r.printLn(fmt.Sprintf("ERROR: %v", r.err))
	} else {
		r.printLn("No errors")
	}
	return r
}

// Spreadsheet returns the underlying spreadsheet. use with caution as it
// bypasses error tracking.
func (r *RunnableSpreadsheet) Spreadsheet() *Spreadsheet {
	return r.spreadsheet
}

// Reset clears the error state (chainable)
func (r *RunnableSpreadsheet) Reset() *RunnableSpreadsheet {
	r.err = nil
	return r
}

// Then allows conditional execution based on current error state
func (r *RunnableSpreadsheet) Then(fn func(*RunnableSpreadsheet) *RunnableSpreadsheet) *RunnableSpreadsheet {
	if r.err != nil {
		return r // skip if there's an error
	}
	return fn(r)
}

// OnError allows error handling in the chain
func (r *RunnableSpreadsheet) OnError(fn func(error) error) *RunnableSpreadsheet {
	if r.err != nil {
		r.err = fn(r.err)
	}
	return r
}

// Must panics if there's an error (chainable). useful for ensuring
// critical operations succeed
func (r *RunnableSpreadsheet) Must() *RunnableSpreadsheet {
	if r.err != nil {
		panic(r.err)
	}
	return r
}

// SetBatch sets multiple cells at once (chainable)
func (r *RunnableSpreadsheet) SetBatch(cells map[string]Primitive) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}

	for address, value := range cells {
		if err := r.spreadsheet.Set(address, value); err != nil {
			r.err = err
			return r
		}
	}
	return r
}

// GetBatch retrieves multiple cell values
func (r *RunnableSpreadsheet) GetBatch(addresses ...string) (*RunnableSpreadsheet, map[string]Primitive) {
	if r.err != nil {
		return r, nil // no-op if there's already an error
	}

	results := make(map[string]Primitive)
	for _, address := range addresses {
		val, err := r.spreadsheet.Get(address)
		if err != nil {
			r.err = err
			return r, nil
		}
		results[address] = val
	}
	return r, results
}

// WithWorksheet ensures a worksheet exists before continuing (chainable)
func (r *RunnableSpreadsheet) WithWorksheet(name string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}

	if !r.spreadsheet.DoesWorksheetExist(name) {
		r.err = r.spreadsheet.AddWorksheet(name)
	}
	return r
}

// If allows conditional operations in the chain
func (r *RunnableSpreadsheet) If(condition bool, fn func(*RunnableSpreadsheet) *RunnableSpreadsheet) *RunnableSpreadsheet {
	if r.err != nil || !condition {
		return r // skip if there's an error or condition is false
	}
	return fn(r)
}

// ForEach applies a function to a range of cells (chainable)
func (r *RunnableSpreadsheet) ForEach(startRow, endRow int, startCol, endCol int, fn func(row, col int, r *RunnableSpreadsheet)) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}

	for row := startRow; row <= endRow; row++ {
		for col := startCol; col <= endCol; col++ {
			fn(row, col, r)
			if r.err != nil {
				return r // stop on first error
			}
		}
	}
	return r
}

// Value is a helper to get a single value from the chain.
// example: val := NewRunnableSpreadsheet().Set("A1", 10).Set("A2", "=A1*2").Calculate().Value("A2")
func (r *RunnableSpreadsheet) Value(address string) Primitive {
	if r.err != nil {
		return nil
	}

	val, err := r.spreadsheet.Get(address)
	if err != nil {
		r.err = err
		return nil
	}
	return val
}

// Values is a helper to get multiple values from the chain
func (r *RunnableSpreadsheet) Values(addresses ...string) []Primitive {
	if r.err != nil {
		return nil
	}

	values := make([]Primitive, len(addresses))
	for i, address := range addresses {
		val, err := r.spreadsheet.Get(address)
		if err != nil {
			r.err = err
			return nil
		}
		values[i] = val
	}
	return values
}

// Log logs the value of a cell using the provided PrintLn function (chainable)
func (r *RunnableSpreadsheet) Log(address string) *RunnableSpreadsheet {
	if r.err != nil {
		return r // no-op if there's already an error
	}

	val, err := r.spreadsheet.Get(address)
	if err != nil {
		r.err = err
		return r
	}

	// fmt the output
	var output string
	if val == nil {
		output = fmt.Sprintf("%s: <empty>", address)
	} else {
		output = fmt.Sprintf("%s: %v", address, val)
	}

	r.printLn(output)
	return r
}
