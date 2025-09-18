package main

// Storage holds references to shared tables needed by storage operations
type Storage struct {
	worksheets      *WorksheetTable
	namedRanges     *NamedRangeTable
	strings         *StringTable
	formulas        *FormulaTable
	dependencyGraph *DependencyGraph
}
