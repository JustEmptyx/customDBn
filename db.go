package main

import (
	"os"
	"sync"
)

type Database struct {
	rwlock sync.RWMutex // 1 author - n readers
	*dal
}

func Open(path string, Params *Params) (*Database, error) {
	Params.pSize = os.Getpagesize()
	dal, err := newDal(path, Params)
	if err != nil {
		return nil, err
	}

	Database := &Database{
		sync.RWMutex{},
		dal,
	}

	return Database, nil
}

func (Database *Database) Close() error {
	return Database.close()
}

func (Database *Database) ReadTx() *tx {
	Database.rwlock.RLock()
	return newTx(Database, false)
}

func (Database *Database) WriteTx() *tx {
	Database.rwlock.Lock()
	return newTx(Database, true)
}
