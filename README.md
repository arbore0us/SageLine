#SageLine
SageLine is a compressed self-index1) with similarities to roaring bitmap, bwt-indexes and judy array. It’s a variable-ary tree of bitmaps that can be used as a suffix tree of arbitrary binary data, randomly distributed binary data of fixed length (random unsigned integers) in the current proof of concept implementation.

##Introduction
The structure is based on partitioning each integer and constructing the SageLine with one partition represented by each depth in the tree. 
e.g. a 3bit integer such as 101 can be partitioned as 3:{101},1-2:{1}{01}, 2-1:{10}{1} and 1-1-1:{1}{0}{1}

Each depth has nodes (bitmaps) of size 2^b bits where b is the number of bits of the partition for that level and a set bit in the node indicates presence. Empty nodes, i.e. all zeroes, are not present in the basic structure.
e.g. the 3 bit integers 000, 001, 100, 101 with a  2-1 partition has the first 2 bits for the first depth 00,00,10,10 so the node for the first depth is 1010, the first 1 for 00, the second for 10.

Each depth except the first has one node for each set bit in the previous depth.
e.g. for the four 3 bit integers
	1010
	11	11			

To search the structure the query is partitioned and
for each partition:
	index the current node with partition and test if that bit is set
	if that bit is set
		set current node (for the next depth) to number of set bits prior to that bit at the 			current depth (counting across all prior as well as the current node)
	 else
		query is not present in the structure

##Construction
For construction integers are presorted and nodes are created for the first integer.
e.g. for the four 3 bit integers
first (and only) node of the first depth is created and the bit for 00 set
the first node of the second depth is created and the bit for 0 set
1000
10

For each subsequent integer it is compared to the previous one and the first partition/depth where they differ determined. The partition for that depth of the current depth is used to set the depth in the current node, if not at the last partition new nodes are created for subsequent partitions.
e.g. 000 and 001 differ at the 3rd bit corresponding to the second partition, the bit for 1 is set in the current node for the second depth
1000
11

001 and 100 differ at the 1st bit corresponding to the first partition, the bit corresponding to 10 is set 
1010
11
a new node is created for the second partition of 100 and the bit for 0 is set
1010
11 10
100 and 101 differ at the 3rd bit corresponding to the second partition, the bit for 1 is set in the current node for the second depth
1010
11 11

##Searching
Searching multiple queries is carried out in a smiliar manner. Queries are sorted, the first one searched and subsequent queries compared to the prior query to determine the correct partition to start the search in. Queries matching the structure will match at the last depth, queries following a query not matching the structure need to differ in a partition no greater than the last partition matched by the previous query. The cost of counting set bits is amortized across queries.


##Constructing partitions.
The efficiency of the structure will be heavily influenced by how integers are partitioned. The expected number of nodes for a depth is a basic ”balls in bins” problem, the same as calculating the expected number of empty locations for a hash, assuming a random distribution.
 The number of nodes for a certain depth will be dependent on the number of bits represented by the previous depths as well as the total number of integers, not being dependent of the how previous depths are partitioned. 
 As such a dynamic algorithm can be used to determine optimal partitioning.


##Extending the structure.

###Singles
In the basic structure full nodes will be required at all depths even if they contain only a single value. Since there are no all zero nodes in the basic structure this can be used to mark a unique hit and the remainder of the integer stored in the structure (not implemented) or external to the structure (implemented).

###Joins
In planning

###Combined sorting and construction, sorting and searching
The sorting and construction as well as sorting and searching could be combined, respectively.

###Combined Depths and Singles
In the current implementation singles are stored external to the structure and depths are stored separately, mainly for ease of implementation. They could instead be stored internally in one 
collated structure, with indexes for depths rather than pointers and/or singles stored adjacent to zero nodes.

###Aux structures
Aux structures could be added e.g. for storing counts for ranges or for enabling using different partitions for different parts of the structure.

###Level compression
If the first depth is set so that it’s completely dense, or close to completely dense, the first depth could be omitted and the first partition used to directly index the second depth. If not completely dense singles might need to be handled differently.
