package wal

import "os"

type WAL struct {
	filePath string
}

func NewWal(filepath string) *WAL {

	return &WAL{filePath: filepath}
}
