package main

import (
	"errors"
	"fmt"
	"os"
)

type pgnum uint64

type Params struct {
	pSize int

	//in percents
	MinStored float32
	MaxStored float32
}

var DefaultParams = &Params{
	MinStored: 0.5,
	MaxStored: 0.95,
}

type page struct {
	num  pgnum
	data []byte
}

type dal struct {
	pSize     int
	minStored float32
	maxStored float32
	file      *os.File

	*meta
	*freelist
}

func fillNewDalObject(Params *Params) *dal {
	dal := &dal{
		meta:      newEmptyMeta(),
		pSize:     Params.pSize,
		minStored: Params.MinStored,
		maxStored: Params.MaxStored,
	}
	return dal
}
func newDal(path string, Params *Params) (*dal, error) {
	dal := fillNewDalObject(Params)

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		meta, err := dal.parseMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.parseFreeList()
		if err != nil {
			return nil, err
		}
		dal.freelist = freelist
		// doesn't exist
	} else if errors.Is(err, os.ErrNotExist) {
		// init freelist
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		dal.freelist = setFreeList()
		dal.freelistPage = dal.AllocateNewPage()
		_, err := dal.updateFreeList()
		if err != nil {
			return nil, err
		}

		// init root
		collectionsNode, err := dal.createNode(NewNodeForSerialization([]*Item{}, []pgnum{}))
		//collectionsNode, err := dal.createNode(NewNodeForSerialization([]*CustomItem{}, []pgnum{}))
		if err != nil {
			return nil, err
		}
		dal.root = collectionsNode.pageNum

		// write meta page
		_, err = dal.updateMeta(dal.meta) // other error
	} else {
		return nil, err
	}
	return dal, nil
}

// used in node to rebalance
func (d *dal) findBalanceIndex(node *Node) int {
	size := 0
	size += nodeHeaderSize
	println("minRange", d.minRange())

	for i := range node.items {
		size += node.elementSize(i)

		// if we have a big enough page size (more than minimum), and didn't reach the last node, which means we can
		// spare an element
		println("size : ", size)
		if float32(size) > d.minRange() && i < len(node.items)-1 {
			return i + 1
		}
	}

	return -1
}

func (d *dal) maxRange() float32 {
	return d.maxStored * float32(d.pSize)
}

func (d *dal) isUpperBoundReached(node *Node) bool {
	println("nodeSize : ", float32(node.nodeSize()))
	println("maxRange : ", d.maxRange())
	return float32(node.nodeSize()) > d.maxRange()
}

func (d *dal) minRange() float32 {
	return d.minStored * float32(d.pSize)
}

func (d *dal) isLowerBoundReached(node *Node) bool {
	return float32(node.nodeSize()) < d.minRange()
}

func (d *dal) close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("can't close db file: %s", err)
		}
		d.file = nil
	}

	return nil
}

func (d *dal) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pSize, d.pSize),
	}
}

func (d *dal) readPage(pageNum pgnum) (*page, error) {
	p := d.allocateEmptyPage()

	offset := int(pageNum) * d.pSize
	_, err := d.file.ReadAt(p.data, int64(offset))
	if err != nil {
		return nil, err
	}
	return p, err
}

func (d *dal) writePage(p *page) error {
	offset := int64(p.num) * int64(d.pSize)
	_, err := d.file.WriteAt(p.data, offset)
	return err
}

func (d *dal) getNode(pageNum pgnum) (*Node, error) {
	p, err := d.readPage(pageNum)
	if err != nil {
		return nil, err
	}
	node := NewEmptyNode()
	node.deserialize(p.data)
	fmt.Printf("items : %s, childNodes: %s", node.items, node.childNodes)
	fmt.Printf("node : %s\n", node)
	node.pageNum = pageNum
	return node, nil
}

func (d *dal) createNode(n *Node) (*Node, error) {
	p := d.allocateEmptyPage()
	if n.pageNum == 0 {
		p.num = d.AllocateNewPage()
		n.pageNum = p.num
	} else {
		p.num = n.pageNum
	}

	p.data = n.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (d *dal) deleteNode(pageNum pgnum) {
	d.deletePage(pageNum)
}

func (d *dal) parseFreeList() (*freelist, error) {
	p, err := d.readPage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freelist := setFreeList()
	freelist.deserialize(p.data)
	return freelist, nil
}

func (d *dal) updateFreeList() (*page, error) {
	p := d.allocateEmptyPage()
	p.num = d.freelistPage
	d.freelist.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	d.freelistPage = p.num
	return p, nil
}

func (d *dal) parseMeta() (*meta, error) {
	p, err := d.readPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data)
	return meta, nil
}

func (d *dal) updateMeta(meta *meta) (*page, error) {
	p := d.allocateEmptyPage()
	p.num = metaPageNum
	meta.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}
