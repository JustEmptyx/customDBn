package main

import (
	"bytes"
	"encoding/binary"
)

type Collection struct {
	name         []byte
	rootNodePage pgnum
	counter      uint64

	tx *tx
}

func newCollection(name []byte, rootNodePage pgnum) *Collection {
	return &Collection{
		name:         name,
		rootNodePage: rootNodePage,
	}
}

func newEmptyCollection() *Collection {
	return &Collection{}
}

func (c *Collection) ID() uint64 {
	if !c.tx.write {
		return 0
	}

	id := c.counter
	c.counter += 1
	return id
}

func (c *Collection) serialize() *Item {
	b := make([]byte, collectionSize)
	leftPos := 0
	binary.LittleEndian.PutUint64(b[leftPos:], uint64(c.rootNodePage))
	leftPos += pageNumSize
	binary.LittleEndian.PutUint64(b[leftPos:], c.counter)
	leftPos += counterSize
	return newItem(c.name, b)
}

func (c *Collection) deserialize(item *Item) {
	c.name = item.key

	if len(item.value) != 0 {
		leftPos := 0
		c.rootNodePage = pgnum(binary.LittleEndian.Uint64(item.value[leftPos:]))
		leftPos += pageNumSize

		c.counter = binary.LittleEndian.Uint64(item.value[leftPos:])
		leftPos += counterSize
	}
}

//
//func (c *Collection) serializeCustomCollection() *CustomItem {
//	b := make([]byte, collectionSize)
//	leftPos := 0
//	binary.LittleEndian.PutUint64(b[leftPos:], uint64(c.rootNodePage))
//	leftPos += pageNumSize
//	binary.LittleEndian.PutUint64(b[leftPos:], c.counter)
//	leftPos += counterSize
//	return newItem(c.name, b)
//}
//

//func (c *Collection) deserializeCustomCollection(item *CustomItem) {
//	c.name = item.key
//
//	if len(item.value) != 0 {
//		leftPos := 0
//		c.rootNodePage = pgnum(binary.LittleEndian.Uint64(item.value[leftPos:]))
//		leftPos += pageNumSize
//
//		c.counter = binary.LittleEndian.Uint64(item.value[leftPos:])
//		leftPos += counterSize
//	}
//}

func (c *Collection) Put(key []byte, value []byte) error {
	if !c.tx.write {
		return writeInsideReadTxErr
	}

	i := newItem(key, value)

	var rootNodePage *Node
	var err error
	if c.rootNodePage == 0 {
		rootNodePage = c.tx.createNode(c.tx.newNode([]*Item{i}, []pgnum{}))
		c.rootNodePage = rootNodePage.pageNum
		return nil
	} else {
		rootNodePage, err = c.tx.getNode(c.rootNodePage)
		if err != nil {
			return err
		}
	}

	insertionIndex, nodeToInsertIn, ancestorsIndexes, err := rootNodePage.findKey(i.key, false)
	if err != nil {
		return err
	}

	if nodeToInsertIn.items != nil && insertionIndex < len(nodeToInsertIn.items) && bytes.Compare(nodeToInsertIn.items[insertionIndex].key, key) == 0 {
		nodeToInsertIn.items[insertionIndex] = i
	} else {
		nodeToInsertIn.addItem(i, insertionIndex)
	}
	nodeToInsertIn.createNode(nodeToInsertIn)

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return err
	}

	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		nodeIndex := ancestorsIndexes[i+1]
		if node.isUpperBoundReached() {
			pnode.split(node, nodeIndex)
		}
	}

	rootNode := ancestors[0]
	if rootNode.isUpperBoundReached() {
		newRoot := c.tx.newNode([]*Item{}, []pgnum{rootNode.pageNum})
		newRoot.split(rootNode, 0)

		newRoot = c.tx.createNode(newRoot)

		c.rootNodePage = newRoot.pageNum
	}

	return nil
}

func (c *Collection) Find(key []byte) (*Item, error) {
	n, err := c.tx.getNode(c.rootNodePage)
	if err != nil {
		return nil, err
	}

	index, containingNode, _, err := n.findKey(key, true)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, nil
	}
	return containingNode.items[index], nil
}

func (c *Collection) FindAll(key []byte) (*Item, error) {
	n, err := c.tx.getNode(c.rootNodePage)
	if err != nil {
		return nil, err
	}
	indexes, nodes := n.findAllKeys(key, true)
	print(indexes)
	print(nodes)
	if err != nil {

		return nil, err
	}
	return nodes[0].items[0], nil
}

func (c *Collection) Remove(key []byte) error {
	if !c.tx.write {
		return writeInsideReadTxErr
	}

	// Find the path to the node where the deletion should happen
	rootNode, err := c.tx.getNode(c.rootNodePage)
	if err != nil {
		return err
	}

	removeItemIndex, nodeToRemoveFrom, ancestorsIndexes, err := rootNode.findKey(key, true)
	if err != nil {
		return err
	}

	if removeItemIndex == -1 {
		return nil
	}

	if nodeToRemoveFrom.isLeaf() {
		nodeToRemoveFrom.removeItemFromLeaf(removeItemIndex)
	} else {
		affectedNodes, err := nodeToRemoveFrom.removeItemFromInternal(removeItemIndex)
		if err != nil {
			return err
		}
		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors, err := c.getNodes(ancestorsIndexes)
	if err != nil {
		return err
	}

	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode := ancestors[i]
		node := ancestors[i+1]
		if node.isLowerBoundReached() {
			err = pnode.rebalanceRemove(node, ancestorsIndexes[i+1])
			if err != nil {
				return err
			}
		}
	}

	rootNode = ancestors[0]
	// If the root has no items after rebalancing, there's no need to save it because we ignore it.
	if len(rootNode.items) == 0 && len(rootNode.childNodes) > 0 {
		c.rootNodePage = ancestors[1].pageNum
	}

	return nil
}

func (c *Collection) getNodes(indexes []int) ([]*Node, error) {
	rootNodePage, err := c.tx.getNode(c.rootNodePage)
	if err != nil {
		return nil, err
	}

	nodes := []*Node{rootNodePage}
	child := rootNodePage
	for i := 1; i < len(indexes); i++ {
		child, _ = c.tx.getNode(child.childNodes[indexes[i]])
		nodes = append(nodes, child)
	}
	return nodes, nil
}
