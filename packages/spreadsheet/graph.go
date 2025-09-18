package main

// DependencyNode represents a cell in the dependency graph
type DependencyNode struct {
	// address of *THIS* node
	WorksheetID uint32
	Row         uint32
	Col         uint32

	// cell-to-cell dependencies
	CellPrecedents map[CellAddress]*DependencyNode // cells this cell depends on
	CellDependents map[CellAddress]*DependencyNode // cells that depend on this cell

	// range dependencies (only for formula cells that depend on ranges)
	RangePrecedents map[RangeAddress]struct{} // ranges this cell depends on (lazy)

	// formula and value, which will always be present because nodes only
	// exist for cells with formulas.
	Formula string    // formula if it's a formula cell
	Value   Primitive // cached calculated value

	// dirty tracking
	IsDirty bool // whether this cell needs recalculation
}

// DependencyGraph manages cell dependencies and calculation order
type DependencyGraph struct {
	nodes          map[CellAddress]*DependencyNode           // all nodes in the graph
	rangeObservers map[RangeAddress]map[CellAddress]struct{} // range -> cells that depend on it
	dirtySet       map[CellAddress]struct{}                  // cells needing recalculation
	volatileCells  map[CellAddress]struct{}                  // cells with volatile functions (always recalculate)
}

// NewDependencyGraph creates a new dependency graph
func NewDependencyGraph() *DependencyGraph {
	return &DependencyGraph{
		nodes:          make(map[CellAddress]*DependencyNode),
		rangeObservers: make(map[RangeAddress]map[CellAddress]struct{}),
		dirtySet:       make(map[CellAddress]struct{}),
		volatileCells:  make(map[CellAddress]struct{}),
	}
}

// GetOrCreateNode gets an existing node or creates a new one
func (dg *DependencyGraph) GetOrCreateNode(addr CellAddress) *DependencyNode {
	if node, exists := dg.nodes[addr]; exists {
		return node
	}

	node := &DependencyNode{
		WorksheetID:     addr.WorksheetID,
		Row:             addr.Row,
		Col:             addr.Column,
		CellPrecedents:  make(map[CellAddress]*DependencyNode),
		CellDependents:  make(map[CellAddress]*DependencyNode),
		RangePrecedents: make(map[RangeAddress]struct{}),
	}
	dg.nodes[addr] = node
	return node
}

// GetNode retrieves a node if it exists
func (dg *DependencyGraph) GetNode(addr CellAddress) (*DependencyNode, bool) {
	node, exists := dg.nodes[addr]
	return node, exists
}

// RemoveNode removes a node and all its dependencies
func (dg *DependencyGraph) RemoveNode(addr CellAddress) bool {
	node, exists := dg.nodes[addr]
	if !exists {
		return false
	}

	// remove this node from all its precedents' dependent lists
	for precedentAddr, precedentNode := range node.CellPrecedents {
		delete(precedentNode.CellDependents, addr)
		// clean up precedent node if it has no dependencies
		dg.cleanupNodeIfEmpty(precedentAddr)
	}

	// remove this node from all its dependents' precedent lists
	for _, dependentNode := range node.CellDependents {
		delete(dependentNode.CellPrecedents, addr)
		// do no cleanup dependent nodes - they might have formulas
		// node will be cleaned up later if it's straight up empty
	}

	// remove from range observers
	for rangeAddr := range node.RangePrecedents {
		if observers, exists := dg.rangeObservers[rangeAddr]; exists {
			delete(observers, addr)
			if len(observers) == 0 {
				delete(dg.rangeObservers, rangeAddr)
			}
		}
	}

	// remove from dirty set
	delete(dg.dirtySet, addr)

	// remove from volatile cells
	delete(dg.volatileCells, addr)

	// remove the node itself
	delete(dg.nodes, addr)

	return true
}

// cleanupNodeIfEmpty removes a node if it has no dependencies or formula
func (dg *DependencyGraph) cleanupNodeIfEmpty(addr CellAddress) {
	node, exists := dg.nodes[addr]
	if !exists {
		return
	}

	// keep node if it has a formula or any dependencies
	if node.Formula != "" ||
		len(node.CellPrecedents) > 0 ||
		len(node.CellDependents) > 0 ||
		len(node.RangePrecedents) > 0 {
		return
	}

	// remove empty node and its dirty flag
	delete(dg.nodes, addr)
	delete(dg.dirtySet, addr)
}

// AddCellDependency adds a cell-to-cell dependency (from depends on to)
func (dg *DependencyGraph) AddCellDependency(from, to CellAddress) {
	fromNode := dg.GetOrCreateNode(from)
	toNode := dg.GetOrCreateNode(to)

	// mark dep
	fromNode.CellPrecedents[to] = toNode
	toNode.CellDependents[from] = fromNode
}

// RemoveCellDependency removes a cell-to-cell dependency
func (dg *DependencyGraph) RemoveCellDependency(from, to CellAddress) bool {
	fromNode, fromExists := dg.nodes[from]
	toNode, toExists := dg.nodes[to]

	if !fromExists || !toExists {
		return false
	}

	// remove the dependency
	delete(fromNode.CellPrecedents, to)
	delete(toNode.CellDependents, from)

	// clean up empty nodes
	dg.cleanupNodeIfEmpty(from)
	dg.cleanupNodeIfEmpty(to)

	return true
}

// AddRangeDependency adds a cell-to-range dependency (from depends on range)
func (dg *DependencyGraph) AddRangeDependency(from CellAddress, rangeAddr RangeAddress) {
	node := dg.GetOrCreateNode(from)

	// add range to node's precedents
	node.RangePrecedents[rangeAddr] = struct{}{}

	// add node to range observers
	if dg.rangeObservers[rangeAddr] == nil {
		dg.rangeObservers[rangeAddr] = make(map[CellAddress]struct{})
	}
	dg.rangeObservers[rangeAddr][from] = struct{}{}
}

// RemoveRangeDependency removes a cell-to-range dependency
func (dg *DependencyGraph) RemoveRangeDependency(from CellAddress, rangeAddr RangeAddress) bool {
	node, exists := dg.nodes[from]
	if !exists {
		return false
	}

	// remove range from node's precedents
	delete(node.RangePrecedents, rangeAddr)

	// remove node from range observers
	if observers, exists := dg.rangeObservers[rangeAddr]; exists {
		delete(observers, from)
		if len(observers) == 0 {
			delete(dg.rangeObservers, rangeAddr)
		}
	}

	// clean up node if empty
	dg.cleanupNodeIfEmpty(from)

	return true
}

// ClearDependencies clears all dependencies for a cell
func (dg *DependencyGraph) ClearDependencies(addr CellAddress) {
	node, exists := dg.nodes[addr]
	if !exists {
		return
	}

	// remove cell dependencies
	for precedentAddr := range node.CellPrecedents {
		dg.RemoveCellDependency(addr, precedentAddr)
	}

	// remove range dependencies
	for rangeAddr := range node.RangePrecedents {
		dg.RemoveRangeDependency(addr, rangeAddr)
	}
}

// MarkDirty marks a cell as needing recalculation
func (dg *DependencyGraph) MarkDirty(addr CellAddress) {
	dg.dirtySet[addr] = struct{}{}

	if node, exists := dg.nodes[addr]; exists {
		node.IsDirty = true
	}
}

// MarkRangeDirty marks all cells depending on a range as dirty
func (dg *DependencyGraph) MarkRangeDirty(rangeAddr RangeAddress) {
	// find all cells observing this range
	if observers, exists := dg.rangeObservers[rangeAddr]; exists {
		for cellAddr := range observers {
			dg.MarkDirty(cellAddr)
		}
	}
}

// MarkCellIfInRangeDirty marks cells dirty if the given cell is within any observed range
func (dg *DependencyGraph) MarkCellIfInRangeDirty(addr CellAddress) {
	// check all observed ranges to see if this cell is within them
	for rangeAddr, observers := range dg.rangeObservers {
		if dg.IsInRange(addr, rangeAddr) {
			// mark all observers of this range as dirty
			for observerAddr := range observers {
				dg.MarkDirty(observerAddr)
			}
		}
	}
}

// IsInRange checks if a cell is within a range
func (dg *DependencyGraph) IsInRange(cell CellAddress, r RangeAddress) bool {
	return cell.WorksheetID == r.WorksheetID &&
		cell.Row >= r.StartRow && cell.Row <= r.EndRow &&
		cell.Column >= r.StartColumn && cell.Column <= r.EndColumn
}

// ClearDirty clears the dirty flag for a cell
func (dg *DependencyGraph) ClearDirty(addr CellAddress) {
	delete(dg.dirtySet, addr)

	if node, exists := dg.nodes[addr]; exists {
		node.IsDirty = false
	}
}

// ClearAllDirty clears all dirty flags
func (dg *DependencyGraph) ClearAllDirty() {
	dg.dirtySet = make(map[CellAddress]struct{})

	for _, node := range dg.nodes {
		node.IsDirty = false
	}
}

// GetDirectDependents returns cells directly depending on this cell
func (dg *DependencyGraph) GetDirectDependents(addr CellAddress) []CellAddress {
	node, exists := dg.nodes[addr]
	if !exists {
		return nil
	}

	result := make([]CellAddress, 0, len(node.CellDependents))
	for dependentAddr := range node.CellDependents {
		result = append(result, dependentAddr)
	}
	return result
}

// GetAllDependents returns all cells affected by this cell (transitive closure)
func (dg *DependencyGraph) GetAllDependents(addr CellAddress) []CellAddress {
	visited := make(map[CellAddress]struct{})
	var result []CellAddress

	dg.collectDependents(addr, visited, &result)
	return result
}

// collectDependents recursively collects all dependents
func (dg *DependencyGraph) collectDependents(addr CellAddress, visited map[CellAddress]struct{}, result *[]CellAddress) {
	if _, alreadyVisited := visited[addr]; alreadyVisited {
		return
	}
	visited[addr] = struct{}{}

	node, exists := dg.nodes[addr]
	if !exists {
		return
	}

	for dependentAddr := range node.CellDependents {
		if _, alreadyVisited := visited[dependentAddr]; !alreadyVisited {
			*result = append(*result, dependentAddr)
			dg.collectDependents(dependentAddr, visited, result)
		}
	}
}

// GetDirectPrecedents returns cells this cell directly depends on
func (dg *DependencyGraph) GetDirectPrecedents(addr CellAddress) []CellAddress {
	node, exists := dg.nodes[addr]
	if !exists {
		return nil
	}

	result := make([]CellAddress, 0, len(node.CellPrecedents))
	for precedentAddr := range node.CellPrecedents {
		result = append(result, precedentAddr)
	}
	return result
}

// GetRangePrecedents returns ranges this cell depends on
func (dg *DependencyGraph) GetRangePrecedents(addr CellAddress) []RangeAddress {
	node, exists := dg.nodes[addr]
	if !exists {
		return nil
	}

	result := make([]RangeAddress, 0, len(node.RangePrecedents))
	for rangeAddr := range node.RangePrecedents {
		result = append(result, rangeAddr)
	}
	return result
}

func (dg *DependencyGraph) GetCalculationOrder() ([]CellAddress, bool) {
	// three states: unvisited (not in map), visiting (false), visited (true)
	state := make(map[CellAddress]bool)
	var order []CellAddress
	hasCycle := false

	var visit func(addr CellAddress) bool
	visit = func(addr CellAddress) bool {
		if completed, exists := state[addr]; exists {
			if !completed {
				// currently visiting - cycle detected
				return true
			}
			// already visited
			return false
		}

		// mark as visiting
		state[addr] = false

		node, exists := dg.nodes[addr]
		if exists {
			// visit all precedents first
			for precedentAddr := range node.CellPrecedents {
				if visit(precedentAddr) {
					hasCycle = true
				}
			}
		}

		// mark as visited
		state[addr] = true
		order = append(order, addr)

		return false
	}

	// visit all nodes
	for addr := range dg.nodes {
		if _, visited := state[addr]; !visited {
			if visit(addr) {
				hasCycle = true
			}
		}
	}

	return order, hasCycle
}

// HasCycle checks if there are circular dependencies
func (dg *DependencyGraph) HasCycle() bool {
	_, hasCycle := dg.GetCalculationOrder()
	return hasCycle
}

// GetAffectedCells returns all cells that need recalculation when a
// cell changes. this includes direct and transitive dependents, plus cells
// observing ranges
func (dg *DependencyGraph) GetAffectedCells(addr CellAddress) []CellAddress {
	affected := make(map[CellAddress]struct{})

	// get all transitive dependents
	dependents := dg.GetAllDependents(addr)
	for _, dep := range dependents {
		affected[dep] = struct{}{}
	}

	// check if this cell is in any observed ranges
	for rangeAddr, observers := range dg.rangeObservers {
		if dg.IsInRange(addr, rangeAddr) {
			for observerAddr := range observers {
				affected[observerAddr] = struct{}{}
				// also get transitive dependents of the observer
				observerDeps := dg.GetAllDependents(observerAddr)
				for _, dep := range observerDeps {
					affected[dep] = struct{}{}
				}
			}
		}
	}

	result := make([]CellAddress, 0, len(affected))
	for affectedAddr := range affected {
		result = append(result, affectedAddr)
	}
	return result
}

// SetFormula sets the formula for a node (creates node if needed)
func (dg *DependencyGraph) SetFormula(addr CellAddress, formula string) {
	node := dg.GetOrCreateNode(addr)
	node.Formula = formula
}

// SetValue sets the cached value for a node
func (dg *DependencyGraph) SetValue(addr CellAddress, value Primitive) {
	if node, exists := dg.nodes[addr]; exists {
		node.Value = value
	}
}

// GetFormula retrieves the formula for a cell
func (dg *DependencyGraph) GetFormula(addr CellAddress) (string, bool) {
	if node, exists := dg.nodes[addr]; exists {
		return node.Formula, true
	}
	return "", false
}

// GetValue retrieves the cached value for a cell
func (dg *DependencyGraph) GetValue(addr CellAddress) (Primitive, bool) {
	if node, exists := dg.nodes[addr]; exists {
		return node.Value, true
	}
	return nil, false
}

// NodeCount returns the number of nodes in the graph
func (dg *DependencyGraph) NodeCount() int {
	return len(dg.nodes)
}

// RangeObserverCount returns the number of observed ranges
func (dg *DependencyGraph) RangeObserverCount() int {
	return len(dg.rangeObservers)
}

// Clear removes all nodes and dependencies from the graph
func (dg *DependencyGraph) Clear() {
	dg.nodes = make(map[CellAddress]*DependencyNode)
	dg.rangeObservers = make(map[RangeAddress]map[CellAddress]struct{})
	dg.dirtySet = make(map[CellAddress]struct{})
	dg.volatileCells = make(map[CellAddress]struct{})
}

// MarkVolatile marks a cell as containing volatile functions
func (dg *DependencyGraph) MarkVolatile(addr CellAddress) {
	dg.volatileCells[addr] = struct{}{}
}

// UnmarkVolatile removes volatile marking from a cell
func (dg *DependencyGraph) UnmarkVolatile(addr CellAddress) {
	delete(dg.volatileCells, addr)
}

// IsVolatile checks if a cell contains volatile functions
func (dg *DependencyGraph) IsVolatile(addr CellAddress) bool {
	_, isVolatile := dg.volatileCells[addr]
	return isVolatile
}

// GetVolatileCells returns all cells marked as volatile
func (dg *DependencyGraph) GetVolatileCells() []CellAddress {
	result := make([]CellAddress, 0, len(dg.volatileCells))
	for addr := range dg.volatileCells {
		result = append(result, addr)
	}
	return result
}

// MarkAllVolatileDirty marks all volatile cells as dirty for recalculation
func (dg *DependencyGraph) MarkAllVolatileDirty() {
	for addr := range dg.volatileCells {
		dg.MarkDirty(addr)
	}
}
