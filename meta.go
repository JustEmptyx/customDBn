package main

import "encoding/binary"

const (
	magicNumber uint32 = 0xABCD1234
	metaPageNum        = 0
)

// meta is the meta page of the db
type meta struct {
	//root page
	//freelist page - пофиксить
	root         pgnum
	freelistPage pgnum
}

func newEmptyMeta() *meta {
	return &meta{}
}

func (m *meta) serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint32(buf[pos:], magicNumber)
	pos += magicNumberSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.root))
	pos += pageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

func (m *meta) deserialize(buf []byte) {
	pos := 0
	magicNumberRes := binary.LittleEndian.Uint32(buf[pos:])
	pos += magicNumberSize

	if magicNumberRes != magicNumber {
		panic("Something goes wrong: wrong db magic number or file")
	}

	m.root = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize

	m.freelistPage = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
