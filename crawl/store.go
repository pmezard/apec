package main

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	dir   string
	lock  sync.Mutex
	known map[string]bool
}

func listFiles(dir string) ([]string, error) {
	fp, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	files := []string{}
	for {
		entries, err := fp.Readdir(1024)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for _, fi := range entries {
			if fi.Mode().IsRegular() {
				files = append(files, fi.Name())
			}
		}
	}
	return files, nil
}

func OpenStore(dir string) (*Store, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}
	s := &Store{
		dir:   dir,
		known: map[string]bool{},
	}
	files, err := listFiles(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		s.known[f] = true
	}
	return s, nil
}

func (s *Store) Has(id string) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.known[id]
}

func (s *Store) Write(id string, data []byte) (bool, error) {
	path := filepath.Join(s.dir, id)
	fp, err := ioutil.TempFile(s.dir, "entry-")
	if err != nil {
		return false, err
	}
	defer func() {
		fp.Close()
		os.Remove(fp.Name())
	}()
	_, err = fp.Write(data)
	if err != nil {
		return false, err
	}
	err = fp.Close()
	if err != nil {
		return false, err
	}
	err = os.Rename(fp.Name(), path)
	if err != nil {
		if os.IsExist(err) {
			err = nil
		}
	} else {
		s.lock.Lock()
		s.known[id] = true
		s.lock.Unlock()
	}
	return true, err
}
