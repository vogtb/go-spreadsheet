package main

// ASTKey represents a normalized AST used as a key for formula deduplication,
// two formulas with the same structure (ignoring whitespace) will have the
// same ASTKey. ASTKey represents a normalized AST for formula deduplication
// we use a string key because maps are not comparable
type ASTKey string

// FormulaTable stores formulas centrally and tracks worksheet and named
// range references.
type FormulaTable struct {
	// core formula storage

	astIndex  map[ASTKey]uint32  // normalized AST -> formula ID
	astCache  map[uint32]ASTNode // formula ID -> cached parsed AST
	refCounts map[uint32]int     // formula ID -> reference count

	// cell tracking

	cellsUsingFormula map[uint32]map[CellAddress]struct{} // formula ID -> cells using it
	formulaAtCell     map[CellAddress]uint32              // cell -> formula ID (reverse index)

	// worksheet tracking

	owningWorksheets     map[uint32]map[uint32]struct{} // formula ID -> worksheets containing it
	referencedWorksheets map[uint32]map[uint32]struct{} // formula ID -> worksheets it references

	// named range tracking

	namedRangesUsed         map[uint32]map[uint32]struct{} // formula ID -> named range IDs it uses
	formulasUsingNamedRange map[uint32]map[uint32]struct{} // named range ID -> formula IDs using it

	nextID uint32
}

// NewFormulaTable creates a new formula table
func NewFormulaTable() *FormulaTable {
	return &FormulaTable{
		astIndex:                make(map[ASTKey]uint32),
		astCache:                make(map[uint32]ASTNode),
		refCounts:               make(map[uint32]int),
		cellsUsingFormula:       make(map[uint32]map[CellAddress]struct{}),
		formulaAtCell:           make(map[CellAddress]uint32),
		owningWorksheets:        make(map[uint32]map[uint32]struct{}),
		referencedWorksheets:    make(map[uint32]map[uint32]struct{}),
		namedRangesUsed:         make(map[uint32]map[uint32]struct{}),
		formulasUsingNamedRange: make(map[uint32]map[uint32]struct{}),
		nextID:                  1, // Start at 1, reserve 0 for no formula
	}
}

// normalizeAST converts an AST to its normalized string representation
func (ft *FormulaTable) normalizeAST(ast ASTNode) ASTKey {
	if ast == nil {
		return ""
	}
	return ASTKey(ast.ToString())
}

// InternFormula adds a formula or increments its reference count if it
// already exists. tracks the cell using this formula. Returns the formula ID.
func (ft *FormulaTable) InternFormula(ast ASTNode, cell CellAddress) uint32 {
	key := ft.normalizeAST(ast)

	// check if formula already exists
	if id, exists := ft.astIndex[key]; exists {
		ft.refCounts[id]++
		ft.trackCellUsage(id, cell)
		return id
	}

	// add new formula
	id := ft.nextID
	ft.astIndex[key] = id
	ft.astCache[id] = ast
	ft.refCounts[id] = 1
	ft.trackCellUsage(id, cell)
	ft.nextID++

	return id
}

// trackCellUsage adds a cell to the set of cells using a formula
func (ft *FormulaTable) trackCellUsage(formulaID uint32, cell CellAddress) {
	// remove old formula from cell if exists
	if oldFormulaID, exists := ft.formulaAtCell[cell]; exists && oldFormulaID != formulaID {
		if cells, ok := ft.cellsUsingFormula[oldFormulaID]; ok {
			delete(cells, cell)
			if len(cells) == 0 {
				delete(ft.cellsUsingFormula, oldFormulaID)
			}
		}
	}

	// add cell to new formula's usage set
	if ft.cellsUsingFormula[formulaID] == nil {
		ft.cellsUsingFormula[formulaID] = make(map[CellAddress]struct{})
	}
	ft.cellsUsingFormula[formulaID][cell] = struct{}{}
	ft.formulaAtCell[cell] = formulaID

	// track worksheet ownership
	ft.TrackWorksheetOwnership(formulaID, cell.WorksheetID)
}

// GetAST retrieves the cached AST for a formula ID
func (ft *FormulaTable) GetAST(id uint32) (ASTNode, bool) {
	ast, exists := ft.astCache[id]
	return ast, exists
}

// GetFormulaID returns the ID for a normalized AST
func (ft *FormulaTable) GetFormulaID(ast ASTNode) (uint32, bool) {
	key := ft.normalizeAST(ast)
	id, exists := ft.astIndex[key]
	return id, exists
}

// AddCellReference adds a cell reference to an existing formula
func (ft *FormulaTable) AddCellReference(formulaID uint32, cell CellAddress) bool {
	if _, exists := ft.astCache[formulaID]; !exists {
		return false
	}

	ft.refCounts[formulaID]++
	ft.trackCellUsage(formulaID, cell)
	return true
}

// RemoveCellReference removes a cell reference from a formula. returns true
// if the formula was removed due to zero references.
func (ft *FormulaTable) RemoveCellReference(formulaID uint32, cell CellAddress) bool {
	// remove cell from tracking
	if cells, exists := ft.cellsUsingFormula[formulaID]; exists {
		delete(cells, cell)
		if len(cells) == 0 {
			delete(ft.cellsUsingFormula, formulaID)
		}
	}
	delete(ft.formulaAtCell, cell)

	// decrement reference count
	ft.refCounts[formulaID]--
	if ft.refCounts[formulaID] <= 0 {
		// clean up formula completely
		ft.removeFormula(formulaID)
		return true
	}

	// update worksheet ownership if no more cells from that worksheet
	ft.updateWorksheetOwnership(formulaID, cell.WorksheetID)

	return false
}

// removeFormula removes a formula and all its tracking data
func (ft *FormulaTable) removeFormula(formulaID uint32) {
	// get AST to find key
	if ast, exists := ft.astCache[formulaID]; exists {
		key := ft.normalizeAST(ast)
		delete(ft.astIndex, key)
	}

	// remove from all maps
	delete(ft.astCache, formulaID)
	delete(ft.refCounts, formulaID)
	delete(ft.cellsUsingFormula, formulaID)
	delete(ft.owningWorksheets, formulaID)
	delete(ft.referencedWorksheets, formulaID)

	// clean up named range tracking
	if namedRanges, exists := ft.namedRangesUsed[formulaID]; exists {
		for namedRangeID := range namedRanges {
			if formulas, ok := ft.formulasUsingNamedRange[namedRangeID]; ok {
				delete(formulas, formulaID)
				if len(formulas) == 0 {
					delete(ft.formulasUsingNamedRange, namedRangeID)
				}
			}
		}
		delete(ft.namedRangesUsed, formulaID)
	}
}

// updateWorksheetOwnership updates worksheet ownership after removing a cell
func (ft *FormulaTable) updateWorksheetOwnership(formulaID uint32, worksheetID uint32) {
	// check if any cells from this worksheet still use the formula
	stillUsed := false
	if cells, exists := ft.cellsUsingFormula[formulaID]; exists {
		for cell := range cells {
			if cell.WorksheetID == worksheetID {
				stillUsed = true
				break
			}
		}
	}

	// remove worksheet from ownership if no longer used
	if !stillUsed {
		if worksheets, exists := ft.owningWorksheets[formulaID]; exists {
			delete(worksheets, worksheetID)
			if len(worksheets) == 0 {
				delete(ft.owningWorksheets, formulaID)
			}
		}
	}
}

// GetReferenceCount returns the reference count for a formula
func (ft *FormulaTable) GetReferenceCount(id uint32) int {
	return ft.refCounts[id]
}

// TrackWorksheetOwnership marks a worksheet as owning (containing) a formula
func (ft *FormulaTable) TrackWorksheetOwnership(formulaID uint32, worksheetID uint32) {
	if ft.owningWorksheets[formulaID] == nil {
		ft.owningWorksheets[formulaID] = make(map[uint32]struct{})
	}
	ft.owningWorksheets[formulaID][worksheetID] = struct{}{}
}

// TrackWorksheetReference marks a worksheet as being referenced by a formula
func (ft *FormulaTable) TrackWorksheetReference(formulaID uint32, worksheetID uint32) {
	if ft.referencedWorksheets[formulaID] == nil {
		ft.referencedWorksheets[formulaID] = make(map[uint32]struct{})
	}
	ft.referencedWorksheets[formulaID][worksheetID] = struct{}{}
}

// GetOwningWorksheets returns the IDs of worksheets containing a formula
func (ft *FormulaTable) GetOwningWorksheets(formulaID uint32) []uint32 {
	worksheets := ft.owningWorksheets[formulaID]
	result := make([]uint32, 0, len(worksheets))
	for id := range worksheets {
		result = append(result, id)
	}
	return result
}

// GetReferencedWorksheets returns the IDs of worksheets referenced by a formula
func (ft *FormulaTable) GetReferencedWorksheets(formulaID uint32) []uint32 {
	worksheets := ft.referencedWorksheets[formulaID]
	result := make([]uint32, 0, len(worksheets))
	for id := range worksheets {
		result = append(result, id)
	}
	return result
}

// TrackNamedRangeReference tracks that a formula uses a named range
func (ft *FormulaTable) TrackNamedRangeReference(formulaID uint32, namedRangeID uint32) {
	// track formula -> named ranges
	if ft.namedRangesUsed[formulaID] == nil {
		ft.namedRangesUsed[formulaID] = make(map[uint32]struct{})
	}
	ft.namedRangesUsed[formulaID][namedRangeID] = struct{}{}

	// track named range -> formulas (reverse index)
	if ft.formulasUsingNamedRange[namedRangeID] == nil {
		ft.formulasUsingNamedRange[namedRangeID] = make(map[uint32]struct{})
	}
	ft.formulasUsingNamedRange[namedRangeID][formulaID] = struct{}{}
}

// RemoveNamedRangeReference removes a named range reference from a formula
func (ft *FormulaTable) RemoveNamedRangeReference(formulaID uint32, namedRangeID uint32) {
	// remove from formula -> named ranges
	if namedRanges, exists := ft.namedRangesUsed[formulaID]; exists {
		delete(namedRanges, namedRangeID)
		if len(namedRanges) == 0 {
			delete(ft.namedRangesUsed, formulaID)
		}
	}

	// remove from named range -> formulas
	if formulas, exists := ft.formulasUsingNamedRange[namedRangeID]; exists {
		delete(formulas, formulaID)
		if len(formulas) == 0 {
			delete(ft.formulasUsingNamedRange, namedRangeID)
		}
	}
}

// GetFormulasUsingNamedRange returns formula IDs that use a specific
// named range
func (ft *FormulaTable) GetFormulasUsingNamedRange(namedRangeID uint32) []uint32 {
	formulas := ft.formulasUsingNamedRange[namedRangeID]
	result := make([]uint32, 0, len(formulas))
	for id := range formulas {
		result = append(result, id)
	}
	return result
}

// GetCellsUsingFormula returns all cells using a specific formula
func (ft *FormulaTable) GetCellsUsingFormula(formulaID uint32) []CellAddress {
	cells := ft.cellsUsingFormula[formulaID]
	result := make([]CellAddress, 0, len(cells))
	for cell := range cells {
		result = append(result, cell)
	}
	return result
}

// GetFormulaAtCell returns the formula ID at a specific cell
func (ft *FormulaTable) GetFormulaAtCell(cell CellAddress) (uint32, bool) {
	id, exists := ft.formulaAtCell[cell]
	return id, exists
}

// Count returns the number of unique formulas
func (ft *FormulaTable) Count() int {
	return len(ft.astIndex)
}

// TotalReferences returns the total number of references across all formulas
func (ft *FormulaTable) TotalReferences() int {
	total := 0
	for _, count := range ft.refCounts {
		total += count
	}
	return total
}

// Clear removes all formulas from the table
func (ft *FormulaTable) Clear() {
	ft.astIndex = make(map[ASTKey]uint32)
	ft.astCache = make(map[uint32]ASTNode)
	ft.refCounts = make(map[uint32]int)
	ft.cellsUsingFormula = make(map[uint32]map[CellAddress]struct{})
	ft.formulaAtCell = make(map[CellAddress]uint32)
	ft.owningWorksheets = make(map[uint32]map[uint32]struct{})
	ft.referencedWorksheets = make(map[uint32]map[uint32]struct{})
	ft.namedRangesUsed = make(map[uint32]map[uint32]struct{})
	ft.formulasUsingNamedRange = make(map[uint32]map[uint32]struct{})
	ft.nextID = 1
}
