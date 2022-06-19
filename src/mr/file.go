package mr

import (
	"os"
)

type FileSystem interface {
	Read(filename string) ([]byte, error)
	Write(filename string, data []byte) error
}

var _ FileSystem = &localFileSystem{}

func NewLocalFileSystem() FileSystem {
	return &localFileSystem{}
}

type localFileSystem struct {
}

func (fs *localFileSystem) Read(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func (fs *localFileSystem) Write(filename string, data []byte) error {
	return os.WriteFile(filename, data, os.ModePerm)
}
