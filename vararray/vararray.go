package vararray

import (
	"fmt"
	"log"
	"math/bits"
)

//VarArray64 Struct for packing uint<64s in []uint64 and also work as bitarray
type VarArray64 struct {
	nodeSize         uint
	Array            []uint64
	Count            uint
	Debug            bool
	cachedbitcount   uint
	cachedarrayindex uint
}

//AddNode increase size if necessary and set node TODO: Handle nodeSize>64
func (varArray *VarArray64) AddNode(val uint64) {
	c := uint(cap(varArray.Array))
	n := (varArray.Count + 2) * varArray.nodeSize //TODO: +1 gives occational error...fix it
	if n > c*64 {
		newArr := make([]uint64, c+1, 2*(c+1))
		copy(newArr, varArray.Array)
		varArray.Array = newArr
		c = uint(cap(varArray.Array))
	}
	l := uint(len(varArray.Array))
	if n > (l * 64) {
		varArray.Array = varArray.Array[:c]
	}

	varArray.SetNode(val, varArray.Count)
	varArray.Count++
}

//Len get number of nodes in vararr
func (varArray *VarArray64) Len() float64 {
	return float64(len(varArray.Array)*64) / float64(varArray.nodeSize)
}

//SetNode set node TODO: Handle nodeSize>64
func (varArray *VarArray64) SetNode(val uint64, index uint) bool {
	arrayStart := index * varArray.nodeSize
	arrayStop := arrayStart + varArray.nodeSize
	arrayStartIndex := arrayStart >> 6
	arrayStopIndex := arrayStop >> 6
	startPos := uint(arrayStart & 63)
	stopPos := uint(arrayStop & 63)
	startMask := uint64(1) << startPos
	stopMask := uint64(1) << stopPos
	startMask--
	stopMask--
	if arrayStartIndex == arrayStopIndex {
		mask := startMask ^ stopMask
		varArray.Array[arrayStartIndex] = (varArray.Array[arrayStartIndex] &^ mask) | (val << startPos)
	} else {
		varArray.Array[arrayStartIndex] = (varArray.Array[arrayStartIndex] & startMask) | val<<startPos
		varArray.Array[arrayStopIndex] = (varArray.Array[arrayStopIndex] &^ stopMask) | (val >> (varArray.nodeSize - stopPos))
	}
	return true
}

//GetNode ... TODO: Handle nodeSize>64
func (varArray *VarArray64) GetNode(index uint) (uint64, bool) {
	arrayStart := index * varArray.nodeSize
	arrayStop := arrayStart + varArray.nodeSize
	arrayStartIndex := arrayStart >> 6
	arrayStopIndex := arrayStop >> 6
	startPos := uint(arrayStart & 63)
	stopPos := uint(arrayStop & 63)
	startMask := uint64(1) << startPos
	stopMask := uint64(1) << stopPos
	if arrayStopIndex >= uint(len(varArray.Array)) {
		if varArray.Debug {
			fmt.Printf("GetNode out of bounds, %d>%d", arrayStopIndex, len(varArray.Array))
		}
		return uint64(0), false
	}
	startMask--
	stopMask--
	if arrayStartIndex == arrayStopIndex {
		mask := startMask ^ stopMask
		return (varArray.Array[arrayStartIndex] & mask) >> startPos, true
	}
	return ((varArray.Array[arrayStartIndex] &^ startMask) >> startPos) | ((varArray.Array[arrayStopIndex] & stopMask) << (varArray.nodeSize - stopPos)), true
}

//SetVal set bit
func (varArray *VarArray64) SetVal(index uint) {
	varArray.Array[index>>6] |= 1 << (index & 63)
}

//GetVal check if bit set
func (varArray *VarArray64) GetVal(index uint) bool {
	return varArray.Array[index>>6]&(1<<(index&63)) > 0
}

//ZeroNode check if any bits set in node
func (varArray *VarArray64) ZeroNode(index uint) bool {
	if varArray.nodeSize > 64 {
		return varArray.ZeroNodeLarge(index)
	}
	return varArray.ZeroNodeSmall(index)
}

//ZeroNodeLarge check if any bits set in node
func (varArray *VarArray64) ZeroNodeLarge(index uint) bool {
	arrayStart := index * varArray.nodeSize
	arrayStop := arrayStart + varArray.nodeSize
	arrayStartIndex := arrayStart >> 6
	arrayStopIndex := arrayStop >> 6
	if arrayStopIndex > uint(len(varArray.Array)) {
		arrayStopIndex = uint(len(varArray.Array))
	}
	for i := arrayStartIndex; i < arrayStopIndex; i++ {
		if varArray.Array[i] > 0 {
			return false
		}
	}
	return true
}

//ZeroNodeSmall check if any bits set in node
func (varArray *VarArray64) ZeroNodeSmall(index uint) bool {
	node, found := varArray.GetNode(index)
	if !found {
		log.Printf("ZeroNodeSmall(%d) oob", index)
	}
	return node == 0
}

//CountFromTo count zero nodes and set bits from start to stop
func (varArray *VarArray64) CountFromTo(start, stop uint) (uint, uint) {
	if varArray.nodeSize > 64 {
		return varArray.CountFromToLarge(start, stop)
	}
	return varArray.CountFromToSmall(start, stop)
}

//CountFromToSmall count zero nodes and set bits from start to stop //TODO: get both in one pass
func (varArray *VarArray64) CountFromToSmall(start, stop uint) (uint, uint) {
	ones := 0
	singles := 0
	for i := start; i < stop; i++ {
		node, found := varArray.GetNode(i)
		if !found {
			log.Printf("CountFromToSmall(%d,%d) oob for %d", start, stop, i)
		}
		if node > 0 {
			ones += bits.OnesCount64(node)
		} else {
			singles++
		}

	}
	return uint(ones), uint(singles)
}

//CountFromToLarge count zero nodes and set bits from start to stop //TODO: get both in one pass
func (varArray *VarArray64) CountFromToLarge(start, stop uint) (uint, uint) {
	ones := 0
	singles := 0
	startIndex := (start * varArray.nodeSize) >> 6
	stopIndex := (stop + varArray.nodeSize) >> 6
	if stopIndex > uint(len(varArray.Array)) {
		stopIndex = uint(len(varArray.Array))
	}
	for i := start; i < stop; i++ {
		if varArray.ZeroNodeLarge(i) {
			singles++
		} else {
			for j := startIndex; j < stopIndex; j++ {
				ones += bits.OnesCount64(varArray.Array[j])
			}
		}
		startIndex = stopIndex
		stopIndex += varArray.nodeSize
		if stopIndex > uint(len(varArray.Array)) {
			stopIndex = uint(len(varArray.Array))
		}

	}
	return uint(ones), uint(singles)
}

//CountToVal count set bits in node up to val for small Node
func (varArray *VarArray64) CountToVal(start, val uint) uint {
	if varArray.nodeSize > 64 {
		return varArray.CountToValLarge(start, val)
	}
	return varArray.CountToValSmall(start, val)
}

//CountToValSmall count set bits in node up to val for small Node
func (varArray *VarArray64) CountToValSmall(start, val uint) uint {
	stopMask := uint64(1) << val
	stopMask--
	node, found := varArray.GetNode(start)
	if !found {
		log.Printf("CountToValSmall(%d,%d) oob", start, val)
	}
	return uint(bits.OnesCount64(node & stopMask))
}

//CountToValLarge count set bits in node up to val for large Node
func (varArray *VarArray64) CountToValLarge(start, val uint) uint {
	count := uint(0)
	arrayStart := start * varArray.nodeSize
	arrayStop := arrayStart + uint(val)
	arrayStartIndex := arrayStart >> 6
	arrayStopIndex := arrayStop >> 6
	stopPos := uint(arrayStop & 63)
	stopMask := uint64(1) << stopPos
	updateCache := false

	stopMask--
	if arrayStartIndex == arrayStopIndex {
		return uint(bits.OnesCount64(varArray.Array[arrayStartIndex] & stopMask))
	}

	if arrayStartIndex <= varArray.cachedarrayindex && varArray.cachedarrayindex < arrayStopIndex {
		count = varArray.cachedbitcount
		arrayStartIndex = varArray.cachedarrayindex
		if count > 0 {
			arrayStartIndex++
		}
		updateCache = true
	}

	for i := arrayStartIndex; i < arrayStopIndex; i++ {
		count += uint(bits.OnesCount64(varArray.Array[i]))
	}
	if updateCache {
		varArray.cachedbitcount = count
		varArray.cachedarrayindex = arrayStopIndex - 1
	}
	return count + uint(bits.OnesCount64(varArray.Array[arrayStopIndex]&stopMask))
}

//GetNodeVal check if bit set
func (varArray *VarArray64) GetNodeVal(index uint, val uint) bool {
	i := index*varArray.nodeSize + val
	if (i >> 6) >= uint(len(varArray.Array)) {
		return false
	}
	return varArray.Array[i>>6]&(1<<(i&63)) > 0
}

//New create vararray to specification
func New(n uint, nodeSize uint) *VarArray64 {
	//
	ret := &VarArray64{nodeSize, make([]uint64, n), 0, false, 0, 0}
	return ret
}

//Uint64ToVarArray64 wrap existing array in vararray
func Uint64ToVarArray64(arr []uint64, nodeSize uint) *VarArray64 {
	return &VarArray64{nodeSize, arr, 0, false, 0, 0}
}

func indexingTest(index uint, testVal uint64, array []uint64) {
	log.Printf("initial:\n%064b%064b\n", array[1], array[0])
	nodeSize := uint(7)
	arrayStart := index * nodeSize
	arrayStop := arrayStart + nodeSize
	arrayStartIndex := arrayStart >> 6
	arrayStopIndex := arrayStop >> 6
	startPos := uint(arrayStart & 63)
	stopPos := uint(arrayStop & 63)
	startMask := uint64(1) << startPos
	stopMask := uint64(1) << stopPos

	startMask--
	stopMask--
	if arrayStartIndex == arrayStopIndex {
		mask := startMask ^ stopMask
		array[arrayStartIndex] = (array[arrayStartIndex] &^ mask) | (testVal << startPos)
	} else {
		array[arrayStartIndex] = (array[arrayStartIndex] & startMask) | testVal<<startPos
		array[arrayStopIndex] = (array[arrayStopIndex] &^ stopMask) | (testVal >> (nodeSize - stopPos))
	}
	log.Printf("added:\n%064b%064b\n", array[1], array[0])
}

func retrievalTest(index uint, array []uint64) uint64 {
	log.Printf("initial:\n%064b%064b\n", array[1], array[0])
	nodeSize := uint(7)
	arrayStart := index * nodeSize
	arrayStop := arrayStart + nodeSize
	arrayStartIndex := arrayStart >> 6
	arrayStopIndex := arrayStop >> 6
	startPos := uint(arrayStart & 63)
	stopPos := uint(arrayStop & 63)
	startMask := uint64(1) << startPos
	stopMask := uint64(1) << stopPos

	startMask--
	stopMask--
	if arrayStartIndex == arrayStopIndex {
		mask := startMask ^ stopMask
		return (array[arrayStartIndex] & mask) >> startPos
	}
	first := (array[arrayStartIndex] &^ startMask) >> startPos
	second := (array[arrayStopIndex] & stopMask) << (nodeSize - stopPos)
	return first | second
}
