package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Item struct {
	key   []byte
	value []byte
}

type CustomItem struct {
	key    []byte
	value  []byte
	tstart []byte
	tend   []byte
}

type Node struct {
	// associated transaction
	tx *tx

	pageNum    pgnum
	items      []*Item
	childNodes []pgnum
}

func NewEmptyNode() *Node {
	return &Node{}
}

// NewNodeForSerialization creates a new node only with the properties that are relevant when saving to the disk
func NewNodeForSerialization(items []*Item, childNodes []pgnum) *Node {
	return &Node{
		items:      items,
		childNodes: childNodes,
	}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func newCustomItem(key []byte, value []byte, tstart []byte, tend []byte) *CustomItem {
	return &CustomItem{
		key:    key,
		value:  value,
		tstart: tstart,
		tend:   tend,
	}
}

func isLast(index int, parentNode *Node) bool {
	return index == len(parentNode.items)
}

func isFirst(index int) bool {
	return index == 0
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) createNode(node *Node) *Node {
	return n.tx.createNode(node)
}

func (n *Node) createNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.createNode(node)
	}
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.tx.getNode(pageNum)
}

func (n *Node) isUpperBoundReached() bool {
	return n.tx.Database.isUpperBoundReached(n)
}

func (n *Node) canSpareAnElement() bool {
	splitIndex := n.tx.Database.findBalanceIndex(n)
	if splitIndex == -1 {
		return false
	}
	return true
}

// isLowerBoundReached checks if the node size is smaller than the size of a page.
func (n *Node) isLowerBoundReached() bool {
	return n.tx.Database.isLowerBoundReached(n)
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	// Add page header: isLeaf, key-value pairs count, node num
	// isLeaf
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isLeaf {
			childNode := n.childNodes[i]

			// 8b for each
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		// offset
		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)

		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isLeaf {
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftPos := 0

	isLeaf := uint16(buf[0])

	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	for i := 0; i < itemsCount; i++ {
		if isLeaf == 0 { // False
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, pgnum(pageNum))
		}

		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen
		//fmt.Printf("key is: %s, value is: %s\n", key, value)
		n.items = append(n.items, newItem(key, value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pgnum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}

func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumSize // 8 is the pgnum size
	return size
}

func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.items {
		size += n.elementSize(i)
	}

	// Add last page
	size += pageNumSize // 8 is the pgnum size
	return size
}

// findkeyhelper работает неправильно, фикс
func (n *Node) findKey(key []byte, exact bool) (int, *Node, []int, error) {
	ancestorsIndexes := []int{0} // index of root
	index, node, err := findKeyHelper(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, ancestorsIndexes, nil
}

func findKeyHelper(node *Node, key []byte, exact bool, ancestorsIndexes *[]int) (int, *Node, error) {
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key, exact, ancestorsIndexes)
}

func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.items {
		fmt.Printf("%s , %s\n", existingItem.key, key)
		res := bytes.Compare(existingItem.key, key)
		if res == 0 { // Keys match
			return true, i
		}
		//bigger
		if res == 1 {
			return false, i
		}
	}
	//lower
	return false, len(n.items)
}

func (n *Node) findAllKeys(key []byte, exact bool) ([][]int, []Node) {
	ancestorsIndexes := [][]int{{0}} // index of root
	var nodes []Node
	findAllKeysHelper(n, key, &ancestorsIndexes, &nodes)
	print("All keys found")

	return ancestorsIndexes, nodes
}

func findAllKeysHelper(node *Node, key []byte, ancestorsIndexes *[][]int, nodes *[]Node) {
	for i := 0; i < len(node.childNodes); i++ {
		if !node.isLeaf() {
			nextChild, _ := node.getNode(node.childNodes[i])
			findAllKeysHelper(nextChild, key, ancestorsIndexes, nodes)
		}
		isFound, indexes := node.findAllKeysInNode(key)
		if isFound {
			*ancestorsIndexes = append(*ancestorsIndexes, indexes)
			*nodes = append(*nodes, *node)
		}
	}

	if !node.isLeaf() {
		nextChild, _ := node.getNode(node.childNodes[len(node.childNodes)])
		findAllKeysHelper(nextChild, key, ancestorsIndexes, nodes)
	}
}

func (n *Node) findAllKeysInNode(key []byte) (bool, []int) {
	var arr []int
	for i, existingItem := range n.items {
		temp := existingItem.key[11:]
		res := bytes.Compare(temp, key)
		if res == 0 { // Keys match
			arr = append(arr, i)
		}
	}
	if len(arr) != 0 {
		return true, arr
	}
	return false, arr
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex { // nil or empty slice or after last element
		n.items = append(n.items, item)
		return insertionIndex
	}

	n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
	n.items[insertionIndex] = item
	return insertionIndex
}

// split on position
// used on upper/lower bound
func (n *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {

	splitIndex := nodeToSplit.tx.Database.findBalanceIndex(nodeToSplit)

	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf() {
		newNode = n.createNode(n.tx.newNode(nodeToSplit.items[splitIndex+1:], []pgnum{}))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode = n.createNode(n.tx.newNode(nodeToSplit.items[splitIndex+1:], nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}
	n.addItem(middleItem, nodeToSplitIndex)
	if len(n.childNodes) == nodeToSplitIndex+1 { // If middle of list, then move items forward
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNum
	}

	n.createNodes(n, nodeToSplit)
}

// rebalance on remove
func (n *Node) rebalanceRemove(unbalancedNode *Node, unbalancedNodeIndex int) error {
	pNode := n

	// Right -> rotate right // BRotateRight
	if unbalancedNodeIndex != 0 {
		leftNode, err := n.getNode(pNode.childNodes[unbalancedNodeIndex-1])
		if err != nil {
			return err
		}
		if leftNode.canSpareAnElement() {
			rotateRight(leftNode, pNode, unbalancedNode, unbalancedNodeIndex)
			n.createNodes(leftNode, pNode, unbalancedNode)
			return nil
		}
	}

	// Left -> rotateLeft // BRotateLeft
	if unbalancedNodeIndex != len(pNode.childNodes)-1 {
		rightNode, err := n.getNode(pNode.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return err
		}
		if rightNode.canSpareAnElement() {
			rotateLeft(unbalancedNode, pNode, rightNode, unbalancedNodeIndex)
			n.createNodes(unbalancedNode, pNode, rightNode)
			return nil
		}
	}

	if unbalancedNodeIndex == 0 {
		rightNode, err := n.getNode(n.childNodes[unbalancedNodeIndex+1])
		if err != nil {
			return err
		}

		return pNode.merge(rightNode, unbalancedNodeIndex+1)
	}

	return pNode.merge(unbalancedNode, unbalancedNodeIndex)
}

func (n *Node) removeItemFromLeaf(index int) {
	n.items = append(n.items[:index], n.items[index+1:]...)
	n.createNode(n)
}

func (n *Node) removeItemFromInternal(index int) ([]int, error) {
	//remove largest from left

	affectedNodes := make([]int, 0)
	affectedNodes = append(affectedNodes, index)

	aNode, err := n.getNode(n.childNodes[index])
	if err != nil {
		return nil, err
	}

	for !aNode.isLeaf() {
		traversingIndex := len(n.childNodes) - 1
		aNode, err = aNode.getNode(aNode.childNodes[traversingIndex])
		if err != nil {
			return nil, err
		}
		affectedNodes = append(affectedNodes, traversingIndex)
	}

	n.items[index] = aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]
	n.createNodes(n, aNode)

	return affectedNodes, nil
}

func rotateRight(aNode, pNode, bNode *Node, bNodeIndex int) {

	aNodeItem := aNode.items[len(aNode.items)-1]
	aNode.items = aNode.items[:len(aNode.items)-1]

	pNodeItemIndex := bNodeIndex - 1
	if isFirst(bNodeIndex) {
		pNodeItemIndex = 0
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = aNodeItem

	bNode.items = append([]*Item{pNodeItem}, bNode.items...)

	//bNode.items = append([]*Item{pNodeItem}, bNode.items...)

	if !aNode.isLeaf() {
		childNodeToShift := aNode.childNodes[len(aNode.childNodes)-1]
		aNode.childNodes = aNode.childNodes[:len(aNode.childNodes)-1]
		bNode.childNodes = append([]pgnum{childNodeToShift}, bNode.childNodes...)
	}
}

func rotateLeft(aNode, pNode, bNode *Node, bNodeIndex int) {

	bNodeItem := bNode.items[0]
	bNode.items = bNode.items[1:]

	pNodeItemIndex := bNodeIndex
	if isLast(bNodeIndex, pNode) {
		pNodeItemIndex = len(pNode.items) - 1
	}
	pNodeItem := pNode.items[pNodeItemIndex]
	pNode.items[pNodeItemIndex] = bNodeItem

	// Assign parent item to a and make it last
	aNode.items = append(aNode.items, pNodeItem)

	// If it's an inner leaf then move children as well.
	if !bNode.isLeaf() {
		childNodeToShift := bNode.childNodes[0]
		bNode.childNodes = bNode.childNodes[1:]
		aNode.childNodes = append(aNode.childNodes, childNodeToShift)
	}
}

func (n *Node) merge(bNode *Node, bNodeIndex int) error {
	//simple btree merge
	aNode, err := n.getNode(n.childNodes[bNodeIndex-1])
	if err != nil {
		return err
	}

	pNodeItem := n.items[bNodeIndex-1]
	n.items = append(n.items[:bNodeIndex-1], n.items[bNodeIndex:]...)
	aNode.items = append(aNode.items, pNodeItem)

	aNode.items = append(aNode.items, bNode.items...)
	n.childNodes = append(n.childNodes[:bNodeIndex], n.childNodes[bNodeIndex+1:]...)
	if !aNode.isLeaf() {
		aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
	}
	n.createNodes(aNode, n)
	n.tx.deleteNode(bNode)
	return nil
}
