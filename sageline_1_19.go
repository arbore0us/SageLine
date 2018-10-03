package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math"
	"math/bits"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"./vararray"
	"github.com/RoaringBitmap/roaring"
	"github.com/shawnsmithdev/zermelo/zuint32"
	"github.com/willf/bitset"
)

func timeTrack(start time.Time, name string) {
	end := time.Now()
	elapsed := end.Sub(start)
	mys := elapsed.Nanoseconds() / 1000
	log.Printf("%s took %s (%d us) (%d %d)", name, elapsed, mys, start.Nanosecond(), end.Nanosecond())
}

func _Pow(a, b int) int {
	p := 1
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

type partition32 struct {
	start uint32 //shouldn't really be needed but keep for now
	end   uint32
	shift uint32
	mask  uint32
	size  uint32
}

//genPartitions32: generate mask/shift etc. for partitioning of 32bit uints
func genPartitions32(parts []int, fragmentSize int) []partition32 {
	ret := make([]partition32, len(parts), len(parts))

	start := 0
	for i, part := range parts {
		ret[i] = partition32{start: uint32(start + 1), end: uint32(start + part), shift: uint32(fragmentSize - start - part), mask: uint32(1)<<uint32(part) - uint32(1), size: uint32(math.Pow(2.0, float64(part)))}
		start = start + part
	}
	return ret
}

/*buildSageline: Build basic sageline, pointer less tree where nodes are bitstrings where 1 denotes precense
e.g.
partition 1 bit 1 bit
01 10 11=>
  11		#first bit
10  11	#second bit
or alternatively partition 2 bits
0111
*/
func buildSageline(fragments []uint32, levels []*bitset.BitSet, partitions []partition32, fragmentSize int) []uint32 {
	defer timeTrack(time.Now(), "buildSageline")
	//keep track of current node for each depth
	nPartitions := len(partitions)
	offsets := make([]uint32, nPartitions, nPartitions)

	//initialize
	fragment := fragments[0]

	for depth, partition := range partitions {
		x := (fragment >> partition.shift) & (partition.mask)
		levels[depth].Set(uint(x))
	}

	lastfragment := fragments[0]
	for _, fragment := range fragments[1:] {
		if fragment != lastfragment {
			//get starting partition
			firstdiff := uint32(bits.LeadingZeros32(fragment ^ lastfragment))
			first := true
			for depth, partition := range partitions {
				if partition.end > firstdiff {
					x := (fragment >> partition.shift) & (partition.mask)
					if first { //last one could be the same at that level
						i := uint(offsets[depth] + x)
						first = false
						levels[depth].Set(i)
					} else {
						offsets[depth] += partition.size
						levels[depth].Set(uint(offsets[depth] + x))
					}
				}
			}
			lastfragment = fragment
		}
	}
	return offsets
}

//TODO: Add handling for last fragment, can be handled by appending appropriate number
/*buildSagelineWithSingles: Similar to basic sageline with special handling for nodes with only one member
e.g. if for 0110011010100010; 01100 is unique, the node at the next depth is zeroed and 11010100010 stored as a single for that depth
*/
func buildSagelineWithSingles(fragments []uint32, levels []*bitset.BitSet, singles []*vararray.VarArray64, partitions []partition32, fragmentSize int) []uint32 {
	defer timeTrack(time.Now(), "buildSagelineWithSingles")
	//keep track of current node for each depth
	nPartitions := len(partitions)
	offsets := make([]uint32, nPartitions, nPartitions)
	mem := make([]uint32, nPartitions, nPartitions) //keep track of last fragment
	//initialize
	fragment := fragments[0]
	for depth, partition := range partitions {
		x := (fragment >> partition.shift) & (partition.mask)
		mem[depth] = x
	}
	levels[0].Set(uint(mem[0]))
	lastfragment := fragments[0]
	lastDepth := 0
	var maxDepth int

	for _, fragment := range fragments[1:] {
		if fragment != lastfragment {
			//get starting partition
			firstdiff := uint32(bits.LeadingZeros32(fragment ^ lastfragment))
			first := true
			for depth, partition := range partitions {
				if partition.end > firstdiff {
					x := (fragment >> partition.shift) & (partition.mask)
					if first {
						//compare depths
						if lastDepth <= depth {
							//set lastDepth:depth for lastfragment
							for d := lastDepth + 1; d <= depth; d++ {
								levels[d].Set(uint(offsets[d] + mem[d]))
							}
							maxDepth = depth

						} else {
							//incr offsets
							for d := depth + 1; d <= lastDepth; d++ {
								offsets[d] += partitions[d].size
							}
							maxDepth = lastDepth
						}
						levels[depth].Set(uint(offsets[depth] + x))

						if maxDepth+1 < nPartitions {
							if maxDepth+2 < nPartitions {
								//virtually zeroing node
								offsets[maxDepth+1] += partitions[maxDepth+1].size

								//monkeypatch to handle trailing zero nodes not increasing length of array
								levels[maxDepth+1].Set(uint(offsets[maxDepth+1]))
								levels[maxDepth+1].Clear(uint(offsets[maxDepth+1]))

								singles[maxDepth+1].AddNode(uint64(lastfragment & (1<<(uint32(fragmentSize)-partitions[maxDepth].end) - 1)))
							} else {
								//only one level below maxDepth so set in level
								levels[maxDepth+1].Set(uint(offsets[maxDepth+1] + mem[maxDepth+1]))
								offsets[maxDepth+1] += partitions[maxDepth+1].size
							}
						}
						first = false

						//either lastfrag has more than one unique partition or last part should be zeroed
						lastDepth = depth
					} else {
						//store since won't know if unique until next iteration
						mem[depth] = x
					}
				}
			}
			lastfragment = fragment
		}
	}
	return offsets
}

//iterFindInSageLine: Find fragment in sageline using cached offsets etc.
func iterFindInSageLine(fragment uint32, fragmentSize int, firstdiff int, tree [][]uint64, lastoffsets []uint32, lastindexes []uint32, lastindexcounts []uint32, partitions []partition32) (int, bool) {
	var offset uint32
	var indexcount uint32
	nPartitions := len(partitions)
	for depth := range partitions {
		partition := &partitions[depth]
		if int(partition.end) > firstdiff {
			if depth > 0 {
				offset = lastoffsets[depth-1] //match at least one which means last offset for level before depth is the same for this fragment
			}
			offset += (fragment >> partition.shift) & partition.mask
			index := offset / 64
			if index >= uint32(len(tree[depth])) { //Needed? could keep max val and exit early?
				return int(partition.end), false
			}
			mask := uint64(1) << (offset % 64)
			comp := tree[depth][index] & mask
			if comp == 0 {
				return int(partition.end), false
			}
			//calculate offset for next depth
			if depth < nPartitions-1 {
				mask2 := uint64(1)<<(offset%64) - uint64(1)
				lastindex := lastindexes[depth]
				if index > lastindex {
					indexcount = lastindexcounts[depth] + uint32(popcountSlice(tree[depth][lastindex:index]))*partitions[depth+1].size
					lastindexcounts[depth] = indexcount
					lastindexes[depth] = index
				} else {
					indexcount = lastindexcounts[depth] //count before last index
				}
				offset = indexcount + uint32(bits.OnesCount64(tree[depth][index]&mask2))*partitions[depth+1].size
				lastoffsets[depth] = offset
			}
		}
	}
	return fragmentSize, true //end of last matched node
}

//batchIterFindInSageLine: Search multiple numbers iteratively against given sageline, queries need to be sorted
func batchIterFindInSageLine(queries []uint32, SageLine [][]uint64, partitions []partition32, fragmentSize int) int { //, int, int) {
	defer timeTrack(time.Now(), "batchIterFindInSageLine")
	nPartitions := len(partitions)
	offsets := make([]uint32, nPartitions, nPartitions)
	indexcounts := make([]uint32, nPartitions, nPartitions)
	indexes := make([]uint32, nPartitions, nPartitions)
	matches := 0
	lastfragment := queries[0]

	//initialize
	lastmatch, match := iterFindInSageLine(lastfragment, fragmentSize, 0, SageLine, offsets, indexes, indexcounts, partitions)
	if match {
		matches++
	}
	for _, fragment := range queries[1:] {
		//compare to last fragment
		firstdiff := bits.LeadingZeros32(fragment ^ lastfragment)
		if firstdiff == fragmentSize { //equal
			//matches if last fragment matched
			if match {
				matches++
			}
			/*
				 	lastmatch == fragmentSize for matches so if lastfragment matched
					current fragment is either the same or branches earlier
					if last fragment didn't match and the firstdiff is deeper than the last
					matching depth for last fragment this fragment won't match
			*/
		} else if firstdiff <= lastmatch {
			lastmatch, match = iterFindInSageLine(fragment, fragmentSize, firstdiff, SageLine, offsets, indexes, indexcounts, partitions)
			if match {
				matches++
			}
		}
		lastfragment = fragment
	}
	return matches
}

//popcountSlice: sum of popcounts for slice
func popcountSlice(slice []uint64) uint32 {
	sum := 0
	for _, aInt := range slice {
		sum += bits.OnesCount64(aInt)
	}
	return uint32(sum)
}

//iterFindInSageLineS: search sageline with singles iteratively
func iterFindInSageLineS(fragment uint32, fragmentSize int, firstdiff int, tree []*vararray.VarArray64, singles []*vararray.VarArray64, nSingles []uint, lastindexes []uint, lastindexcounts []uint, lastcounts []uint, partitions []partition32) (int, bool, bool) {
	var index uint
	nPartitions := len(partitions)

	for depth := range partitions {
		partition := &partitions[depth]
		if int(partition.end) > firstdiff {
			index = uint(lastcounts[depth]) //match at least one which means last offset for level before depth is the same for this fragment
			nodeindex := uint((fragment >> partition.shift) & partition.mask)

			if float64(index) >= tree[depth].Len() { //Needed? could keep max val and exit early?
				return int(partition.end), false, false
			}
			if !tree[depth].GetNodeVal(index, nodeindex) { //hit in node
				if depth == 0 || depth+1 == nPartitions { //no singles at first depth nor last so misses there are actual misses
					return int(partition.end), false, false
				}
				if !tree[depth].ZeroNode(index) { //if the node has hits but not for fragment then fragment is a miss
					return int(partition.end), false, false
				}
				//count forward from lastindex to current index
				lastindex := uint(lastindexes[depth]) //should be same for first node
				if index > lastindex {                //should always happen? if node is zero then it's unique and index can't be lastindex
					ones, singles := tree[depth].CountFromTo(lastindex, index)
					lastindexcounts[depth+1] += ones
					lastindexes[depth] = index
					nSingles[depth] += singles //+1 for current node
				}

				//check single
				single, _ := singles[depth].GetNode(nSingles[depth])
				return int(partitions[depth-1].end), single == uint64(fragment&((1<<(uint32(fragmentSize)-partitions[depth-1].end))-1)), true
			}
			//calculate index for next depth
			if depth < nPartitions-1 {
				lastindex := lastindexes[depth]
				if index > lastindex {
					ones, singles := tree[depth].CountFromTo(lastindex, index)
					lastindexcounts[depth+1] += ones
					lastindexes[depth] = index
					nSingles[depth] += singles //+1 for current node
				}

				count := tree[depth].CountToVal(index, nodeindex)
				lastcounts[depth+1] = lastindexcounts[depth+1] + count
			}
		}
	}
	return fragmentSize, true, false //end of last matched node
}

//batchIterFindInSageLineS: Search multiple numbers iteratively against given sageline, queries need to be sorted, using singles
func batchIterFindInSageLineS(queries []uint32, SageLine []*vararray.VarArray64, singles []*vararray.VarArray64, partitions []partition32, fragmentSize int) int {
	defer timeTrack(time.Now(), "batchIterFindInSageLineS")
	nPartitions := len(partitions)
	nSingles := make([]uint, nPartitions, nPartitions)
	indexcounts := make([]uint, nPartitions, nPartitions)
	indexes := make([]uint, nPartitions, nPartitions)
	counts := make([]uint, nPartitions, nPartitions)
	matches := 0
	lastfragment := queries[0]
	//initialize
	lastmatch, match, single := iterFindInSageLineS(lastfragment, fragmentSize, 0, SageLine, singles, nSingles, indexes, indexcounts, counts, partitions)

	if match {
		matches++
	}
	for _, fragment := range queries[1:] {
		firstdiff := bits.LeadingZeros32(fragment ^ lastfragment)
		if firstdiff == fragmentSize { //equal
			if match {
				matches++
			}
		} else {
			if firstdiff <= lastmatch || single { //branch earlier
				//monkeypatch
				if !single && !match && firstdiff == lastmatch {
					match = false
				} else {
					if single && firstdiff > lastmatch {
						firstdiff = lastmatch
					}
					lastmatch, match, single = iterFindInSageLineS(fragment, fragmentSize, firstdiff, SageLine, singles, nSingles, indexes, indexcounts, counts, partitions)
				}
				if match {
					matches++
				}
			}
		}
		lastfragment = fragment
	}
	return matches
}

/*
collatedFindInSageLine: Same as batchIterFindInSageLine but with iter find inlined and
testing of some minor changes to examine the impact on execution speed
*/
func collatedFindInSageLine(queries []uint32, SageLine [][]uint64, partitions []partition32, fragmentSize int) int { //, int, int) {
	defer timeTrack(time.Now(), "collatedFindInSageLine")
	nPartitions := len(partitions)

	offsets := make([]uint32, nPartitions, nPartitions)
	indexcounts := make([]uint32, nPartitions, nPartitions)
	indexes := make([]uint32, nPartitions, nPartitions)

	matches := 0
	lastfragment := queries[0]
	lastmatch, match := iterFindInSageLine(lastfragment, fragmentSize, 0, SageLine, offsets[0:], indexes[0:], indexcounts[0:], partitions)
	for depth, partition := range partitions {
		if partition.end == uint32(lastmatch) {
			lastmatch = depth
		}
	}

	if match {
		matches++
	}
	var offset uint32
	var indexcount uint32
QUERIES:
	for i := 1; i < len(queries); i++ {
		firstdiff := bits.LeadingZeros32(queries[i] ^ queries[i-1])
		if firstdiff == fragmentSize { //equal
			if match {
				matches++
			}
		} else if firstdiff <= int(partitions[lastmatch].end) { //branch earlier
			match = false
			offset = 0
			for depth := range partitions {
				if int(partitions[depth].end) >= firstdiff {
					if depth > 0 {
						offset = offsets[depth-1] * partitions[depth].size //match at least one which means last offset for level before depth is the same for this fragment
					}
					offset += (queries[i] >> partitions[depth].shift) & partitions[depth].mask
					index := offset >> 6
					if index >= uint32(len(SageLine[depth])) { //Needed? could keep max val and exit early?
						lastmatch = depth
						continue QUERIES
					}
					comp := SageLine[depth][index] & (uint64(1) << (offset & 63))
					if comp == 0 {
						lastmatch = depth
						continue QUERIES
					}
					if int(partitions[depth].end) == fragmentSize {
						break
					}
					lastindex := indexes[depth]
					indexcount = indexcounts[depth]
					if index > lastindex {
						indexcount += uint32(popcountSlice(SageLine[depth][lastindex:index]))
						indexcounts[depth] = indexcount
						indexes[depth] = index
					}
					offset = indexcount + uint32(bits.OnesCount64(SageLine[depth][index]&(uint64(1)<<(offset&63)-uint64(1))))
					offsets[depth] = offset
				}
			}
			matches++
			match = true
			lastmatch = nPartitions - 1 //match end
		}
	}
	return matches
}

func deferredRadix(arr []uint32) {
	defer timeTrack(time.Now(), "Radix")
	zuint32.Sort(arr)
}

func genRandomRef(nInts int) []uint32 {
	defer timeTrack(time.Now(), "GenRandom")
	return genSeededRef(nInts, time.Now().UnixNano())
}

func genSeededRef(nInts int, seed int64) []uint32 {
	defer timeTrack(time.Now(), "GenSeeded")
	r := rand.New(rand.NewSource(seed))
	ref := make([]uint32, nInts, nInts)
	rbytes := make([]byte, nInts*4)
	r.Read(rbytes)
	buf := bytes.NewReader(rbytes)
	binary.Read(buf, binary.LittleEndian, &ref)
	deferredRadix(ref)
	return ref
}

func calcBits(n int, D int) float64 {
	return math.Pow(2.0, float64(D)) * (1.0 - math.Pow(1.0-float64(1.0/math.Pow(2.0, float64(D))), float64(n)))
}

func calcSingles(n uint, D uint) float64 {
	return (float64(n) / math.Pow(2.0, float64(D))) * math.Pow(math.Pow(2.0, float64(D-1))/math.Pow(2.0, float64(D)), float64(n-1))
}

func genLevels(partitions []partition32, n int) []*bitset.BitSet {
	defer timeTrack(time.Now(), "GenLevels")
	levels := []*bitset.BitSet{}
	nNodes := uint32(1)
	for _, partition := range partitions {
		levels = append(levels, bitset.New(uint(nNodes)))
		nNodes = uint32(calcBits(n, int(partition.end)))
	}
	return levels
}

func genSingles(partitions []partition32, n uint, fragmentSize int) []*vararray.VarArray64 {
	defer timeTrack(time.Now(), "GenLevels")
	singles := []*vararray.VarArray64{}
	singles = append(singles, vararray.New(0, 1))
	for _, partition := range partitions[1:] {
		nSingles := uint32(calcSingles(n, uint(partition.end)))
		singles = append(singles, vararray.New(uint(nSingles), uint(fragmentSize-int(partition.start-1))))
	}
	return singles
}

func treeFromLevels(levels []*bitset.BitSet) [][]uint64 {
	defer timeTrack(time.Now(), "GenTreeFromLevels")
	//grab bytes
	n := len(levels)
	tree := make([][]uint64, n)
	for i := 0; i < n; i++ {
		tree[i] = levels[i].Bytes()
	}
	return tree
}

func treeFromLevelS(partitions []partition32, levels []*bitset.BitSet) []*vararray.VarArray64 {
	defer timeTrack(time.Now(), "GenTreeFromLevelS")
	//grab bytes
	n := len(levels)
	tree := make([]*vararray.VarArray64, n, n)
	for i, partition := range partitions {
		tree[i] = vararray.Uint64ToVarArray64(levels[i].Bytes(), uint(partition.size))
	}
	return tree
}

func validateFind(query []uint32, tree [][]uint64, partitions []partition32, fragmentSize int) (int, int) {
	defer timeTrack(time.Now(), "CompVal")
	n1 := batchIterFindInSageLine(query, tree, partitions, fragmentSize)
	n2 := collatedFindInSageLine(query, tree, partitions, fragmentSize)
	return n1, n2
}

func createRoaring(references []uint32) *roaring.Bitmap {
	defer timeTrack(time.Now(), "Creating Roaring")
	rring := roaring.NewBitmap()
	rring.AddMany(references)
	return rring
}

func findInRoaring(queries []uint32, bmref *roaring.Bitmap) uint64 {
	defer timeTrack(time.Now(), "Find in Roaring")
	query := roaring.NewBitmap()
	query.AddMany(queries)
	return roaring.And(query, bmref).GetCardinality()
}

func iterFindInRoaring(queries []uint32, bmref *roaring.Bitmap) int {
	defer timeTrack(time.Now(), "iterFind in Roaring")
	matches := 0
	for _, kmer := range queries {
		if bmref.Contains(kmer) {
			matches++
		}
	}
	return matches
}

const (
	nFragmentSets = 1               //references
	nFragments    = 5 * 1000 * 1000 //250 * 1000 * 1000
	fragmentSize  = 32
	nInt32s       = nFragmentSets * nFragments
	nBytes        = nInt32s * 4
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func printPartitions(items []uint32, partitions []partition32) {
	for _, fragment := range items {
		fmt.Printf("%d\n", fragment)
		for _, partition := range partitions {
			nodeindex := uint((fragment >> partition.shift) & partition.mask)
			single := fragment & (1<<(32-partition.end) - 1)
			fmt.Printf("\t%d\ts:%d\t:", nodeindex, single)
		}
		fmt.Printf("\n")
	}

}

func main() {
	profileRate := 1000
	flag.Parse()

	if *cpuprofile != "" {
		runtime.SetCPUProfileRate(profileRate)
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	ref := genSeededRef(nFragments, 99)
	query := genSeededRef(nFragments*2, 99)
	parts := []int{24, 2, 2, 2, 2}
	partS := []int{23, 2, 3, 4}
	partitions := genPartitions32(parts, fragmentSize)
	partitionS := genPartitions32(partS, fragmentSize)

	levels := genLevels(partitions, nFragments)
	levelS := genLevels(partitionS, nFragments)
	singles := genSingles(partitionS, nFragments, fragmentSize)
	offsets := buildSageline(ref, levels, partitions, fragmentSize)
	offsetS := buildSagelineWithSingles(ref, levelS, singles, partitionS, fragmentSize)
	tree := treeFromLevels(levels)
	treeS := treeFromLevelS(partitionS, levelS)
	rb0 := createRoaring(ref)
	c1 := findInRoaring(query, rb0)
	c2 := iterFindInRoaring(query, rb0)
	fmt.Println("R1:", c1, " R2:", c2)
	n3 := batchIterFindInSageLineS(query, treeS, singles, partitionS, fragmentSize)
	n1, n2 := validateFind(query, tree, partitions, fragmentSize)
	fmt.Println("offsets:", len(offsets), len(levels), len(offsetS), len(treeS))
	fmt.Println("BatchIter:", n1, "\tCollated:", n2, "\tSingles:", n3)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
}
