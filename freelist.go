package main

import "encoding/binary"

const metaPage = 0

type freelist struct {
	// maxAllowedPage holds the latest page num allocated. scrapedPages holds all the ids that were released during
	// delete. New page ids are first given from the releasedPageIDs to avoid growing the file. If it's empty, then
	// maxAllowedPage is incremented and a new page is created thus increasing the file size.
	maxAllowedPage pgnum
	scrapedPages   []pgnum
}

func setFreeList() *freelist {
	return &freelist{
		maxAllowedPage: metaPage,
		scrapedPages:   []pgnum{},
	}
}

func (freelist *freelist) AllocateNewPage() pgnum {
	if len(freelist.scrapedPages) != 0 {
		// Take the last element and remove it from the list
		pageID := freelist.scrapedPages[len(freelist.scrapedPages)-1]
		freelist.scrapedPages = freelist.scrapedPages[:len(freelist.scrapedPages)-1]
		return pageID
	}
	freelist.maxAllowedPage += 1
	return freelist.maxAllowedPage
}

func (freelist *freelist) deletePage(page pgnum) {
	freelist.scrapedPages = append(freelist.scrapedPages, page)
}

func (freelist *freelist) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(freelist.maxAllowedPage))
	pos += 2

	// released pages count
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(freelist.scrapedPages)))
	pos += 2

	for _, page := range freelist.scrapedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize

	}
	return buf
}

func (freelist *freelist) deserialize(buf []byte) {
	pos := 0
	freelist.maxAllowedPage = pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	// released pages count
	scrapedPagesCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < scrapedPagesCount; i++ {
		freelist.scrapedPages = append(freelist.scrapedPages, pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}
