package main

// StringTable provides string interning for efficient string storage with
// reference counting
type StringTable struct {
	strings    map[string]uint32
	reverseMap map[uint32]string
	refCounts  map[uint32]int // reference count for each string ID
	nextID     uint32
}

// NewStringTable creates a new string table
func NewStringTable() *StringTable {
	return &StringTable{
		strings:    make(map[string]uint32),
		reverseMap: make(map[uint32]string),
		refCounts:  make(map[uint32]int),
		nextID:     1, // start at 1, reserve 0 for nil/empty
	}
}

// Intern adds a string to the table or increments its reference count if
// it already exists. returns the ID of the string.
func (st *StringTable) Intern(s string) uint32 {
	// check if string already exists
	if id, exists := st.strings[s]; exists {
		st.refCounts[id]++
		return id
	}

	// add new string
	id := st.nextID
	st.strings[s] = id
	st.reverseMap[id] = s
	st.refCounts[id] = 1
	st.nextID++

	return id
}

// GetString retrieves a string by its ID
func (st *StringTable) GetString(id uint32) (string, bool) {
	s, exists := st.reverseMap[id]
	return s, exists
}

// Contains checks if a string exists in the table and returns its ID
func (st *StringTable) Contains(s string) (uint32, bool) {
	id, exists := st.strings[s]
	return id, exists
}

// AddReference increments the reference count for a string ID
func (st *StringTable) AddReference(id uint32) bool {
	if _, exists := st.reverseMap[id]; !exists {
		return false
	}
	st.refCounts[id]++
	return true
}

// RemoveReference decrements the reference count for a string ID. if the
// count reaches 0, the string is removed from the table. returns true if
// the string was removed, false otherwise.
func (st *StringTable) RemoveReference(id uint32) bool {
	s, exists := st.reverseMap[id]
	if !exists {
		return false
	}

	st.refCounts[id]--
	if st.refCounts[id] <= 0 {
		// remove the string from all maps
		delete(st.strings, s)
		delete(st.reverseMap, id)
		delete(st.refCounts, id)
		return true
	}

	return false
}

// GetReferenceCount returns the reference count for a string ID
func (st *StringTable) GetReferenceCount(id uint32) int {
	return st.refCounts[id]
}

// Count returns the number of unique strings in the table
func (st *StringTable) Count() int {
	return len(st.strings)
}

// TotalReferences returns the total number of references across all strings
func (st *StringTable) TotalReferences() int {
	total := 0
	for _, count := range st.refCounts {
		total += count
	}
	return total
}

// Clear removes all strings from the table
func (st *StringTable) Clear() {
	st.strings = make(map[string]uint32)
	st.reverseMap = make(map[uint32]string)
	st.refCounts = make(map[uint32]int)
	st.nextID = 1
}
