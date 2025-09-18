package main

// WorksheetTable manages worksheet storage and ID mappings
type WorksheetTable struct {
	// core name/ID mapping (for all worksheets, defined or not)

	nameToID map[string]uint32 // name -> ID for all worksheets
	idToName map[uint32]string // ID -> name for all worksheets

	// worksheet definitions

	definedWorksheets map[uint32]*Worksheet // ID -> worksheet for defined worksheets

	// track undefined worksheets (referenced but not yet defined)

	undefinedIDs map[uint32]struct{} // set of IDs that are undefined

	// reference counting

	refCounts map[uint32]int // ID -> reference count
	nextID    uint32
}

// NewWorksheetTable creates a new worksheet table
func NewWorksheetTable() *WorksheetTable {
	return &WorksheetTable{
		nameToID:          make(map[string]uint32),
		idToName:          make(map[uint32]string),
		definedWorksheets: make(map[uint32]*Worksheet),
		undefinedIDs:      make(map[uint32]struct{}),
		refCounts:         make(map[uint32]int),
		nextID:            1, // start at 1, reserve 0 for no worksheet
	}
}

// InternWorksheet adds a reference to a worksheet (defined or not). returns
// the ID of the worksheet.
func (wt *WorksheetTable) InternWorksheet(name string) uint32 {
	// check if name already exists
	if id, exists := wt.nameToID[name]; exists {
		wt.refCounts[id]++
		return id
	}

	// add new undefined worksheet
	id := wt.nextID
	wt.nameToID[name] = id
	wt.idToName[id] = name
	wt.undefinedIDs[id] = struct{}{} // start as undefined
	wt.refCounts[id] = 1
	wt.nextID++

	return id
}

// DefineWorksheet defines or redefines a worksheet with a Worksheet instance.
// if the worksheet was previously undefined, it transitions to defined state.
// returns the ID of the worksheet.
func (wt *WorksheetTable) DefineWorksheet(name string, worksheet *Worksheet) uint32 {
	// check if name already exists
	if id, exists := wt.nameToID[name]; exists {
		// update the definition
		wt.definedWorksheets[id] = worksheet
		delete(wt.undefinedIDs, id) // remove from undefined if present
		wt.refCounts[id]++          // increment reference since we're defining it
		// update the worksheet's ID
		if worksheet != nil {
			worksheet.worksheetID = id
		}
		return id
	}

	// create new defined worksheet
	id := wt.nextID
	wt.nameToID[name] = id
	wt.idToName[id] = name
	wt.definedWorksheets[id] = worksheet
	wt.refCounts[id] = 1
	wt.nextID++

	// update the worksheet's ID
	if worksheet != nil {
		worksheet.worksheetID = id
	}

	return id
}

// UndefineWorksheet removes the definition of a worksheet. if the worksheet
// still has references, it transitions to undefined state. if it has no
// references, it's removed completely. returns true if the worksheet was
// removed completely.
func (wt *WorksheetTable) UndefineWorksheet(name string) bool {
	id, exists := wt.nameToID[name]
	if !exists {
		return false
	}

	// remove the definition
	delete(wt.definedWorksheets, id)

	// check if there are still references
	if wt.refCounts[id] > 0 {
		// keep as undefined
		wt.undefinedIDs[id] = struct{}{}
		return false
	}

	// no references, remove completely
	wt.removeWorksheet(id)
	return true
}

// removeWorksheet removes a worksheet completely from all tracking maps
func (wt *WorksheetTable) removeWorksheet(id uint32) {
	name := wt.idToName[id]
	delete(wt.nameToID, name)
	delete(wt.idToName, id)
	delete(wt.definedWorksheets, id)
	delete(wt.undefinedIDs, id)
	delete(wt.refCounts, id)
}

// AddReference increments the reference count for a worksheet ID
func (wt *WorksheetTable) AddReference(id uint32) bool {
	if _, exists := wt.idToName[id]; !exists {
		return false
	}
	wt.refCounts[id]++
	return true
}

// RemoveReference decrements the reference count for a worksheet ID. if the
// count reaches 0 and the worksheet is undefined, it's removed. returns true
// if the worksheet was removed.
func (wt *WorksheetTable) RemoveReference(id uint32) bool {
	if _, exists := wt.idToName[id]; !exists {
		return false
	}

	wt.refCounts[id]--
	if wt.refCounts[id] <= 0 {
		// check if it's undefined
		if _, isUndefined := wt.undefinedIDs[id]; isUndefined {
			// remove undefined worksheet with no references
			wt.removeWorksheet(id)
			return true
		}
		// defined worksheets stay even with 0 references
	}

	return false
}

// GetWorksheet returns the Worksheet for a given ID
func (wt *WorksheetTable) GetWorksheet(id uint32) (*Worksheet, bool) {
	worksheet, exists := wt.definedWorksheets[id]
	return worksheet, exists
}

// GetWorksheetByName returns the Worksheet for a given name
func (wt *WorksheetTable) GetWorksheetByName(name string) (*Worksheet, bool) {
	id, exists := wt.nameToID[name]
	if !exists {
		return nil, false
	}
	return wt.GetWorksheet(id)
}

// IsWorksheetDefined checks if a worksheet has a definition
func (wt *WorksheetTable) IsWorksheetDefined(id uint32) bool {
	_, exists := wt.definedWorksheets[id]
	return exists
}

// GetWorksheetID returns the ID for a worksheet name
func (wt *WorksheetTable) GetWorksheetID(name string) (uint32, bool) {
	id, exists := wt.nameToID[name]
	return id, exists
}

// GetWorksheetName returns the name for a worksheet ID
func (wt *WorksheetTable) GetWorksheetName(id uint32) (string, bool) {
	name, exists := wt.idToName[id]
	return name, exists
}

// Contains checks if a worksheet exists (defined or undefined)
func (wt *WorksheetTable) Contains(name string) bool {
	_, exists := wt.nameToID[name]
	return exists
}

// GetReferenceCount returns the reference count for a worksheet ID
func (wt *WorksheetTable) GetReferenceCount(id uint32) int {
	return wt.refCounts[id]
}

// GetAllDefinedWorksheets returns all defined worksheets
func (wt *WorksheetTable) GetAllDefinedWorksheets() map[string]*Worksheet {
	result := make(map[string]*Worksheet)
	for id, worksheet := range wt.definedWorksheets {
		if name, exists := wt.idToName[id]; exists {
			result[name] = worksheet
		}
	}
	return result
}

// GetAllUndefinedWorksheets returns all undefined (referenced but not
// defined) worksheet names
func (wt *WorksheetTable) GetAllUndefinedWorksheets() []string {
	result := make([]string, 0, len(wt.undefinedIDs))
	for id := range wt.undefinedIDs {
		if name, exists := wt.idToName[id]; exists {
			result = append(result, name)
		}
	}
	return result
}

// Count returns the total number of worksheets (defined and undefined)
func (wt *WorksheetTable) Count() int {
	return len(wt.nameToID)
}

// CountDefined returns the number of defined worksheets
func (wt *WorksheetTable) CountDefined() int {
	return len(wt.definedWorksheets)
}

// CountUndefined returns the number of undefined worksheets
func (wt *WorksheetTable) CountUndefined() int {
	return len(wt.undefinedIDs)
}

// TotalReferences returns the total number of references across all worksheets
func (wt *WorksheetTable) TotalReferences() int {
	total := 0
	for _, count := range wt.refCounts {
		total += count
	}
	return total
}

// Clear removes all worksheets from the table
func (wt *WorksheetTable) Clear() {
	wt.nameToID = make(map[string]uint32)
	wt.idToName = make(map[uint32]string)
	wt.definedWorksheets = make(map[uint32]*Worksheet)
	wt.undefinedIDs = make(map[uint32]struct{})
	wt.refCounts = make(map[uint32]int)
	wt.nextID = 1
}

// ChunkKey represents the key for indexing chunks in Worksheet
type ChunkKey struct {
	ChunkRow uint32
	ChunkCol uint32
}

// Worksheet provides high-performance sparse spreadsheet
// storage optimized for typical spreadsheet access patterns.
//
// architecture:
// - cells are partitioned into 256x256 chunks for spatial locality
// - each chunk allocates arrays lazily based on actual cell types present
// - string deduplication via StringTable reduces memory for repeated text
// - formula table provide centralized management
//
// performance characteristics:
// - O(1) cell access within loaded chunks
// - memory allocated only for non-empty regions
// - optimized for spreadsheets with clustered data (typical use case)
// - chunk granularity balances memory usage vs allocation overhead
type Worksheet struct {
	chunks      map[ChunkKey]*Chunk // sparse map of chunks indexed by ChunkKey
	totalCells  int                 // stats tracking total number of cells
	cellsByType [8]uint32           // cells by type for diagnostic use
	storage     *Storage            // storage accessible to help
	worksheetID uint32              // worksheet that owns this chunk
}

const (
	ChunkRows uint32 = 256                   // rows per chunk - power of 2 for efficient modulo
	ChunkCols uint32 = 256                   // columns per chunk - matches typical viewport size
	ChunkSize        = ChunkRows * ChunkCols // 65536 cells per chunk
)

// Chunk represents a 256x256 region of cells using structure-of-arrays layout
// for cache efficiency and minimal memory overhead. arrays are allocated
// lazily - only Types and OccupiedBitmap exist initially.
type Chunk struct {
	// always allocated fields.

	Types          []uint8 // cell type for each position (always allocated)
	NonEmptyCount  int     // count of non-empty cells
	OccupiedBitmap []int   // bit-packed array tracking which cells have data

	// lazily allocated fields.

	Numbers                []float64 // numeric values for NUMBER/DATE/BOOLEAN cells (lazy)
	StringIDs              []uint32  // interned string IDs for STRING/ERROR cells (lazy)
	FormulaIDs             []uint32  // formula table IDs for FORMULA cells (lazy)
	FormulaResultTypes     []uint8   // result types for FORMULA cells (lazy)
	FormulaResultNumbers   []float64 // numeric results for FORMULA cells (lazy)
	FormulaResultStringIDs []uint32  // string ID results for FORMULA cells (lazy)
	FormulaResultBooleans  []uint8   // boolean results for FORMULA cells (lazy)
}

// NewWorksheet creates a new worksheet
func NewWorksheet(storage *Storage, worksheetID uint32) *Worksheet {
	return &Worksheet{
		chunks:      make(map[ChunkKey]*Chunk),
		storage:     storage,
		worksheetID: worksheetID,
	}
}

// getChunk retrieves or creates a chunk at the given chunk coordinates
func (w *Worksheet) getChunk(chunkRow, chunkCol uint32) *Chunk {
	key := ChunkKey{ChunkRow: chunkRow, ChunkCol: chunkCol}
	chunk, exists := w.chunks[key]
	if !exists {
		chunk = &Chunk{
			Types:          make([]uint8, ChunkSize),
			OccupiedBitmap: make([]int, (ChunkSize+63)/64), // bit-packed, 64 bits per int
		}
		w.chunks[key] = chunk
	}
	return chunk
}

// GetCell retrieves a cell at the given row and column
func (w *Worksheet) GetCell(row, col uint32) *Cell {
	chunkRow := row / ChunkRows
	chunkCol := col / ChunkCols
	localRow := row % ChunkRows
	localCol := col % ChunkCols

	key := ChunkKey{ChunkRow: chunkRow, ChunkCol: chunkCol}
	chunk, exists := w.chunks[key]
	if !exists {
		return nil
	}

	// column-first indexing for better cache locality
	idx := localCol*ChunkRows + localRow

	// check if this is a formula cell
	hasFormula := chunk.FormulaIDs != nil && idx < uint32(len(chunk.FormulaIDs)) && chunk.FormulaIDs[idx] != 0

	if chunk.Types[idx] == uint8(CellValueTypeEmpty) && !hasFormula {
		return nil
	}

	cell := &Cell{
		Type: CellType(chunk.Types[idx]),
		Row:  row,
		Col:  col,
	}

	// retrieve value based on type
	switch cell.Type {
	case CellValueTypeNumber, CellValueTypeDate:
		if chunk.Numbers != nil && idx < uint32(len(chunk.Numbers)) {
			cell.Value = chunk.Numbers[idx]
		}
	case CellValueTypeString:
		if chunk.StringIDs != nil && idx < uint32(len(chunk.StringIDs)) {
			cell.StringID = chunk.StringIDs[idx]
			if w.storage != nil && w.storage.strings != nil {
				if str, ok := w.storage.strings.GetString(cell.StringID); ok {
					cell.Value = str
				}
			}
		}
	case CellValueTypeBoolean:
		if chunk.Numbers != nil && idx < uint32(len(chunk.Numbers)) {
			cell.Value = chunk.Numbers[idx] != 0
		}
	case CellValueTypeError:
		var errorCode ErrorCode
		var message string

		// get error code from Numbers array
		if chunk.Numbers != nil && idx < uint32(len(chunk.Numbers)) {
			errorCode = ErrorCode(chunk.Numbers[idx])
		}

		// get error message from string table
		if chunk.StringIDs != nil && idx < uint32(len(chunk.StringIDs)) {
			cell.StringID = chunk.StringIDs[idx]
			if w.storage != nil && w.storage.strings != nil {
				if str, ok := w.storage.strings.GetString(cell.StringID); ok {
					message = str
				}
			}
		}

		cell.Value = &SpreadsheetError{
			ErrorCode: errorCode,
			Message:   message,
		}
	}

	// handle formula cells
	if chunk.FormulaIDs != nil && idx < uint32(len(chunk.FormulaIDs)) && chunk.FormulaIDs[idx] != 0 {
		cell.FormulaID = chunk.FormulaIDs[idx]
		if w.storage != nil && w.storage.formulas != nil {
			if ast, ok := w.storage.formulas.GetAST(cell.FormulaID); ok {
				cell.Formula = ast.ToString()
			}
		}

		// get formula result
		if chunk.FormulaResultTypes != nil && idx < uint32(len(chunk.FormulaResultTypes)) {
			cell.FormulaResultType = CellType(chunk.FormulaResultTypes[idx])

			switch cell.FormulaResultType {
			case CellValueTypeNumber, CellValueTypeDate:
				if chunk.FormulaResultNumbers != nil && idx < uint32(len(chunk.FormulaResultNumbers)) {
					cell.Value = chunk.FormulaResultNumbers[idx]
				}
			case CellValueTypeString:
				if chunk.FormulaResultStringIDs != nil && idx < uint32(len(chunk.FormulaResultStringIDs)) {
					stringID := chunk.FormulaResultStringIDs[idx]
					if w.storage != nil && w.storage.strings != nil {
						if str, ok := w.storage.strings.GetString(stringID); ok {
							cell.Value = str
						}
					}
				}
			case CellValueTypeBoolean:
				if chunk.FormulaResultBooleans != nil && idx < uint32(len(chunk.FormulaResultBooleans)) {
					cell.Value = chunk.FormulaResultBooleans[idx] != 0
				}
			case CellValueTypeError:
				var errorCode ErrorCode
				var message string

				// Get error code from FormulaResultNumbers
				if chunk.FormulaResultNumbers != nil && idx < uint32(len(chunk.FormulaResultNumbers)) {
					errorCode = ErrorCode(chunk.FormulaResultNumbers[idx])
				}

				// Get error message from string table
				if chunk.FormulaResultStringIDs != nil && idx < uint32(len(chunk.FormulaResultStringIDs)) {
					stringID := chunk.FormulaResultStringIDs[idx]
					if w.storage != nil && w.storage.strings != nil {
						if str, ok := w.storage.strings.GetString(stringID); ok {
							message = str
						}
					}
				}

				cell.Value = &SpreadsheetError{
					ErrorCode: errorCode,
					Message:   message,
				}
			}
		}
	}

	return cell
}

// SetCell sets a cell value at the given row and column
func (w *Worksheet) SetCell(row, col uint32, value Primitive, formula string) error {
	chunkRow := row / ChunkRows
	chunkCol := col / ChunkCols
	localRow := row % ChunkRows
	localCol := col % ChunkCols

	chunk := w.getChunk(chunkRow, chunkCol)
	idx := localCol*ChunkRows + localRow

	// track if this was previously empty and get old type for statistics
	wasEmpty := chunk.Types[idx] == uint8(CellValueTypeEmpty)
	oldType := CellType(chunk.Types[idx])

	// clear any existing formula
	if chunk.FormulaIDs != nil && idx < uint32(len(chunk.FormulaIDs)) {
		if oldFormulaID := chunk.FormulaIDs[idx]; oldFormulaID != 0 {
			if w.storage != nil && w.storage.formulas != nil {
				cellAddr := CellAddress{WorksheetID: w.worksheetID, Row: row, Column: col}
				w.storage.formulas.RemoveCellReference(oldFormulaID, cellAddr)
			}
			chunk.FormulaIDs[idx] = 0
		}
	}

	// handle formula if present
	var formulaID uint32
	if formula != "" && w.storage != nil && w.storage.formulas != nil {
		// this will be parsed and interned later during calculation. for now, just
		// store a placeholder
		cellAddr := CellAddress{WorksheetID: w.worksheetID, Row: row, Column: col}
		// we need to parse the formula first, which happens in Spreadsheet.Set
		_ = cellAddr
	}

	// determine cell type and store value
	if value == nil && formula == "" {
		chunk.Types[idx] = uint8(CellValueTypeEmpty)
		if !wasEmpty {
			chunk.NonEmptyCount--
			w.totalCells--
		}
	} else if formula != "" {
		// formula cell - keep it non-empty even if value is nil, will be updated
		// when formula is calculated
		if wasEmpty {
			chunk.NonEmptyCount++
			w.totalCells++
		}
	} else {
		if wasEmpty {
			chunk.NonEmptyCount++
			w.totalCells++
		}

		switch v := value.(type) {
		case float64, int, int64:
			chunk.Types[idx] = uint8(CellValueTypeNumber)
			if chunk.Numbers == nil {
				chunk.Numbers = make([]float64, ChunkSize)
			}
			switch num := v.(type) {
			case float64:
				chunk.Numbers[idx] = num
			case int:
				chunk.Numbers[idx] = float64(num)
			case int64:
				chunk.Numbers[idx] = float64(num)
			}

		case string:
			chunk.Types[idx] = uint8(CellValueTypeString)
			if chunk.StringIDs == nil {
				chunk.StringIDs = make([]uint32, ChunkSize)
			}
			if w.storage != nil && w.storage.strings != nil {
				stringID := w.storage.strings.Intern(v)
				chunk.StringIDs[idx] = stringID
			}

		case bool:
			chunk.Types[idx] = uint8(CellValueTypeBoolean)
			if chunk.Numbers == nil {
				chunk.Numbers = make([]float64, ChunkSize)
			}
			if v {
				chunk.Numbers[idx] = 1
			} else {
				chunk.Numbers[idx] = 0
			}

		case *SpreadsheetError:
			chunk.Types[idx] = uint8(CellValueTypeError)
			if chunk.StringIDs == nil {
				chunk.StringIDs = make([]uint32, ChunkSize)
			}
			if chunk.Numbers == nil {
				chunk.Numbers = make([]float64, ChunkSize)
			}
			// store error code in Numbers array
			chunk.Numbers[idx] = float64(v.ErrorCode)
			// store error message in string table
			if w.storage != nil && w.storage.strings != nil {
				stringID := w.storage.strings.Intern(v.Message)
				chunk.StringIDs[idx] = stringID
			}

		default:
			// unknown type, treat as empty
			chunk.Types[idx] = uint8(CellValueTypeEmpty)
			if !wasEmpty {
				chunk.NonEmptyCount--
				w.totalCells--
			}
		}
	}

	// update cell type statistics
	newType := CellType(chunk.Types[idx])
	if oldType != newType {
		// decrement old type count (with bounds checking)
		if oldType < CellType(len(w.cellsByType)) && w.cellsByType[oldType] > 0 {
			w.cellsByType[oldType]--
		}
		// increment new type count
		if newType < CellType(len(w.cellsByType)) {
			w.cellsByType[newType]++
		}
	}

	// update occupied bitmap
	bitIdx := idx / 64
	bitPos := idx % 64
	if chunk.Types[idx] != uint8(CellValueTypeEmpty) {
		chunk.OccupiedBitmap[bitIdx] |= (1 << bitPos)
	} else {
		chunk.OccupiedBitmap[bitIdx] &^= (1 << bitPos)
	}

	// store formula ID if we have one
	if formulaID != 0 {
		if chunk.FormulaIDs == nil {
			chunk.FormulaIDs = make([]uint32, ChunkSize)
		}
		chunk.FormulaIDs[idx] = formulaID
	}

	return nil
}

// RemoveCell removes a cell at the given row and column
func (w *Worksheet) RemoveCell(row, col uint32) {
	chunkRow := row / ChunkRows
	chunkCol := col / ChunkCols
	localRow := row % ChunkRows
	localCol := col % ChunkCols

	key := ChunkKey{ChunkRow: chunkRow, ChunkCol: chunkCol}
	chunk, exists := w.chunks[key]
	if !exists {
		return
	}

	idx := localCol*ChunkRows + localRow

	if chunk.Types[idx] == uint8(CellValueTypeEmpty) {
		return
	}

	// get the cell type for statistics before removing
	cellType := CellType(chunk.Types[idx])

	// clear formula reference if exists
	if chunk.FormulaIDs != nil && idx < uint32(len(chunk.FormulaIDs)) {
		if oldFormulaID := chunk.FormulaIDs[idx]; oldFormulaID != 0 {
			if w.storage != nil && w.storage.formulas != nil {
				cellAddr := CellAddress{WorksheetID: w.worksheetID, Row: row, Column: col}
				w.storage.formulas.RemoveCellReference(oldFormulaID, cellAddr)
			}
			chunk.FormulaIDs[idx] = 0
		}
	}

	// remove string reference if exists
	if chunk.Types[idx] == uint8(CellValueTypeString) || chunk.Types[idx] == uint8(CellValueTypeError) {
		if chunk.StringIDs != nil && idx < uint32(len(chunk.StringIDs)) {
			if stringID := chunk.StringIDs[idx]; stringID != 0 {
				if w.storage != nil && w.storage.strings != nil {
					w.storage.strings.RemoveReference(stringID)
				}
				chunk.StringIDs[idx] = 0
			}
		}
	}

	// clear the cell
	chunk.Types[idx] = uint8(CellValueTypeEmpty)
	chunk.NonEmptyCount--
	w.totalCells--

	// update cell type statistics
	if cellType < CellType(len(w.cellsByType)) && w.cellsByType[cellType] > 0 {
		w.cellsByType[cellType]--
	}

	// update occupied bitmap
	bitIdx := idx / 64
	bitPos := idx % 64
	chunk.OccupiedBitmap[bitIdx] &^= (1 << bitPos)

	// if chunk is now empty, we could remove it to save memory
	if chunk.NonEmptyCount == 0 {
		delete(w.chunks, key)
	}
}

// SetFormulaResult stores the calculated result of a formula cell
func (w *Worksheet) SetFormulaResult(row, col uint32, result Primitive) {
	chunkRow := row / ChunkRows
	chunkCol := col / ChunkCols
	localRow := row % ChunkRows
	localCol := col % ChunkCols

	key := ChunkKey{ChunkRow: chunkRow, ChunkCol: chunkCol}
	chunk, exists := w.chunks[key]
	if !exists {
		return
	}

	idx := localCol*ChunkRows + localRow

	// initialize formula result arrays if needed
	if chunk.FormulaResultTypes == nil {
		chunk.FormulaResultTypes = make([]uint8, ChunkSize)
	}

	// store result based on type
	switch v := result.(type) {
	case float64, int, int64:
		chunk.FormulaResultTypes[idx] = uint8(CellValueTypeNumber)
		if chunk.FormulaResultNumbers == nil {
			chunk.FormulaResultNumbers = make([]float64, ChunkSize)
		}
		switch num := v.(type) {
		case float64:
			chunk.FormulaResultNumbers[idx] = num
		case int:
			chunk.FormulaResultNumbers[idx] = float64(num)
		case int64:
			chunk.FormulaResultNumbers[idx] = float64(num)
		}

	case string:
		chunk.FormulaResultTypes[idx] = uint8(CellValueTypeString)
		if chunk.FormulaResultStringIDs == nil {
			chunk.FormulaResultStringIDs = make([]uint32, ChunkSize)
		}
		if w.storage != nil && w.storage.strings != nil {
			stringID := w.storage.strings.Intern(v)
			chunk.FormulaResultStringIDs[idx] = stringID
		}

	case bool:
		chunk.FormulaResultTypes[idx] = uint8(CellValueTypeBoolean)
		if chunk.FormulaResultBooleans == nil {
			chunk.FormulaResultBooleans = make([]uint8, ChunkSize)
		}
		if v {
			chunk.FormulaResultBooleans[idx] = 1
		} else {
			chunk.FormulaResultBooleans[idx] = 0
		}

	case *SpreadsheetError:
		chunk.FormulaResultTypes[idx] = uint8(CellValueTypeError)
		if chunk.FormulaResultStringIDs == nil {
			chunk.FormulaResultStringIDs = make([]uint32, ChunkSize)
		}
		if chunk.FormulaResultNumbers == nil {
			chunk.FormulaResultNumbers = make([]float64, ChunkSize)
		}
		// store error code in FormulaResultNumbers
		chunk.FormulaResultNumbers[idx] = float64(v.ErrorCode)
		if w.storage != nil && w.storage.strings != nil {
			stringID := w.storage.strings.Intern(v.Message)
			chunk.FormulaResultStringIDs[idx] = stringID
		}

	case nil:
		chunk.FormulaResultTypes[idx] = uint8(CellValueTypeEmpty)
	}
}

// GetCellsByType returns the count of cells by type for diagnostic purposes
func (w *Worksheet) GetCellsByType() [8]uint32 {
	return w.cellsByType
}

// GetCellTypeCount returns the count of cells of a specific type
func (w *Worksheet) GetCellTypeCount(cellType CellType) uint32 {
	if cellType < CellType(len(w.cellsByType)) {
		return w.cellsByType[cellType]
	}
	return 0
}

// GetTotalCells returns the total number of non-empty cells
func (w *Worksheet) GetTotalCells() int {
	return w.totalCells
}
