package main

import "iter"

// RangeAddress represents a range of cells within a single worksheet
type RangeAddress struct {
	WorksheetID uint32
	StartRow    uint32
	StartColumn uint32
	EndRow      uint32
	EndColumn   uint32
}

// NamedRangeTable manages named ranges with ID tracking for efficient renaming.
// supports both defined and non-existent named ranges with reference counting
type NamedRangeTable struct {
	// core name/ID mapping (for all ranges, defined or not)

	nameToID map[string]uint32 // name -> ID for all ranges
	idToName map[uint32]string // ID -> name for all ranges

	// range definitions

	definedRanges map[uint32]RangeAddress // ID -> address for defined ranges

	// track undefined ranges (referenced but not yet defined)

	undefinedIDs map[uint32]struct{} // Set of IDs that are undefined

	// Reference counting

	refCounts map[uint32]int // ID -> reference count
	nextID    uint32
}

// NewNamedRangeTable creates a new named range table
func NewNamedRangeTable() *NamedRangeTable {
	return &NamedRangeTable{
		nameToID:      make(map[string]uint32),
		idToName:      make(map[uint32]string),
		definedRanges: make(map[uint32]RangeAddress),
		undefinedIDs:  make(map[uint32]struct{}),
		refCounts:     make(map[uint32]int),
		nextID:        1, // start at 1, reserve 0 for no range
	}
}

// InternNamedRange adds a reference to a named range (defined or not). returns
// the ID of the named range.
func (nrt *NamedRangeTable) InternNamedRange(name string) uint32 {
	// check if name already exists
	if id, exists := nrt.nameToID[name]; exists {
		nrt.refCounts[id]++
		return id
	}

	// add new undefined range
	id := nrt.nextID
	nrt.nameToID[name] = id
	nrt.idToName[id] = name
	nrt.undefinedIDs[id] = struct{}{} // start as undefined
	nrt.refCounts[id] = 1
	nrt.nextID++

	return id
}

// DefineNamedRange defines or redefines a named range with an address. if
// the range was previously undefined, it transitions to defined state. returns
// the ID of the named range.
func (nrt *NamedRangeTable) DefineNamedRange(name string, address RangeAddress) uint32 {
	// check if name already exists
	if id, exists := nrt.nameToID[name]; exists {
		// update the definition
		nrt.definedRanges[id] = address
		delete(nrt.undefinedIDs, id) // remove from undefined if present
		nrt.refCounts[id]++          // increment reference since we're defining it
		return id
	}

	// create new defined range
	id := nrt.nextID
	nrt.nameToID[name] = id
	nrt.idToName[id] = name
	nrt.definedRanges[id] = address
	nrt.refCounts[id] = 1
	nrt.nextID++

	return id
}

// UndefineNamedRange removes the definition of a named range. if the range
// still has references, it transitions to undefined state. if it has no
// references, it's removed completely. returns true if the range was
// removed completely.
func (nrt *NamedRangeTable) UndefineNamedRange(name string) bool {
	id, exists := nrt.nameToID[name]
	if !exists {
		return false
	}

	// remove the definition
	delete(nrt.definedRanges, id)

	// check if there are still references
	if nrt.refCounts[id] > 0 {
		// keep as undefined
		nrt.undefinedIDs[id] = struct{}{}
		return false
	}

	// no references, remove completely
	nrt.removeRange(id)
	return true
}

// removeRange removes a range completely from all tracking maps
func (nrt *NamedRangeTable) removeRange(id uint32) {
	name := nrt.idToName[id]
	delete(nrt.nameToID, name)
	delete(nrt.idToName, id)
	delete(nrt.definedRanges, id)
	delete(nrt.undefinedIDs, id)
	delete(nrt.refCounts, id)
}

// AddReference increments the reference count for a named range ID
func (nrt *NamedRangeTable) AddReference(id uint32) bool {
	if _, exists := nrt.idToName[id]; !exists {
		return false
	}
	nrt.refCounts[id]++
	return true
}

// RemoveReference decrements the reference count for a named range ID. if
// the count reaches 0 and the range is undefined, it's removed. returns
// true if the range was removed.
func (nrt *NamedRangeTable) RemoveReference(id uint32) bool {
	if _, exists := nrt.idToName[id]; !exists {
		return false
	}

	nrt.refCounts[id]--
	if nrt.refCounts[id] <= 0 {
		// check if it's undefined
		if _, isUndefined := nrt.undefinedIDs[id]; isUndefined {
			// remove undefined range with no references
			nrt.removeRange(id)
			return true
		}
		// defined ranges stay even with 0 references
	}

	return false
}

// GetRangeAddress returns the address of a defined named range
func (nrt *NamedRangeTable) GetRangeAddress(id uint32) (RangeAddress, bool) {
	addr, exists := nrt.definedRanges[id]
	return addr, exists
}

// IsRangeDefined checks if a named range has a definition
func (nrt *NamedRangeTable) IsRangeDefined(id uint32) bool {
	_, exists := nrt.definedRanges[id]
	return exists
}

// GetNamedRangeID returns the ID for a named range
func (nrt *NamedRangeTable) GetNamedRangeID(name string) (uint32, bool) {
	id, exists := nrt.nameToID[name]
	return id, exists
}

// GetNamedRangeName returns the name for a named range ID
func (nrt *NamedRangeTable) GetNamedRangeName(id uint32) (string, bool) {
	name, exists := nrt.idToName[id]
	return name, exists
}

// Contains checks if a named range exists (defined or undefined)
func (nrt *NamedRangeTable) Contains(name string) bool {
	_, exists := nrt.nameToID[name]
	return exists
}

// GetReferenceCount returns the reference count for a named range ID
func (nrt *NamedRangeTable) GetReferenceCount(id uint32) int {
	return nrt.refCounts[id]
}

// GetAllDefinedRanges returns all defined named ranges
func (nrt *NamedRangeTable) GetAllDefinedRanges() map[string]RangeAddress {
	result := make(map[string]RangeAddress)
	for id, addr := range nrt.definedRanges {
		if name, exists := nrt.idToName[id]; exists {
			result[name] = addr
		}
	}
	return result
}

// GetAllUndefinedRanges returns all undefined (referenced but not defined)
// named ranges
func (nrt *NamedRangeTable) GetAllUndefinedRanges() []string {
	result := make([]string, 0, len(nrt.undefinedIDs))
	for id := range nrt.undefinedIDs {
		if name, exists := nrt.idToName[id]; exists {
			result = append(result, name)
		}
	}
	return result
}

// Count returns the total number of named ranges (defined and undefined)
func (nrt *NamedRangeTable) Count() int {
	return len(nrt.nameToID)
}

// CountDefined returns the number of defined named ranges
func (nrt *NamedRangeTable) CountDefined() int {
	return len(nrt.definedRanges)
}

// CountUndefined returns the number of undefined named ranges
func (nrt *NamedRangeTable) CountUndefined() int {
	return len(nrt.undefinedIDs)
}

// TotalReferences returns the total number of references across all
// named ranges
func (nrt *NamedRangeTable) TotalReferences() int {
	total := 0
	for _, count := range nrt.refCounts {
		total += count
	}
	return total
}

// Clear removes all named ranges from the table
func (nrt *NamedRangeTable) Clear() {
	nrt.nameToID = make(map[string]uint32)
	nrt.idToName = make(map[uint32]string)
	nrt.definedRanges = make(map[uint32]RangeAddress)
	nrt.undefinedIDs = make(map[uint32]struct{})
	nrt.refCounts = make(map[uint32]int)
	nrt.nextID = 1
}

// Range represents a lazy range type for memory-efficient formula evaluation
type Range interface {
	GetBounds() RangeAddress
	Iterate() iter.Seq[*Cell]
	IterateValues() iter.Seq[Primitive]
}

// CellRange implements Range for lazy cell iteration
type CellRange struct {
	worksheetID uint32
	startRow    uint32
	startCol    uint32
	endRow      uint32
	endCol      uint32
	worksheet   *Worksheet
	storage     *Storage
}

// GetBounds returns the range boundaries
func (r *CellRange) GetBounds() RangeAddress {
	return RangeAddress{
		WorksheetID: r.worksheetID,
		StartRow:    r.startRow,
		StartColumn: r.startCol,
		EndRow:      r.endRow,
		EndColumn:   r.endCol,
	}
}

// Iterate returns an iterator over all cells in the range
func (r *CellRange) Iterate() iter.Seq[*Cell] {
	return func(yield func(*Cell) bool) {
		if r.worksheet == nil {
			return
		}

		// iterate through all cells in the range
		for row := r.startRow; row <= r.endRow; row++ {
			for col := r.startCol; col <= r.endCol; col++ {
				cell := r.worksheet.GetCell(row, col)
				if cell == nil {
					// return nil for empty cells
					cell = &Cell{
						Type:  CellValueTypeEmpty,
						Row:   row,
						Col:   col,
						Value: nil,
					}
				}
				if !yield(cell) {
					return
				}
			}
		}
	}
}

// IterateValues returns an iterator over cell values in the range
func (r *CellRange) IterateValues() iter.Seq[Primitive] {
	return func(yield func(Primitive) bool) {
		for cell := range r.Iterate() {
			if !yield(cell.Value) {
				return
			}
		}
	}
}
