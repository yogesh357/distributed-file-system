package main

import (
	"bytes"
	"io/ioutil"
	"testing"
)

// func TestStoreDeleteKey(t *testing.T) {
// 	opts := StoreOpts{
// 		PathTransformFunc: CASPathTransformFunc,
// 	}
// 	s := NewStore(opts)

// 	key := "testkey"
// 	data := []byte("some jpg bytes")
// 	if err := s.writeStream(key, bytes.NewReader(data)); err != nil {
// 		t.Error(err)
// 	}

// 	if err := s.Delete(key); err != nil {
// 		t.Error(err)
// 	}
// }

// func TestPathTransformFunc(t *testing.T) {
// 	key := "fuxkinggoodpics"
// 	pathKey := CASPathTransformFunc(key)

// 	expectedOriginalKey := "61b90/280e3/a1a02/67fc3/696e1/2f4f5/ea61d/caf27"
// 	expectedPathName := "61b90/280e3/a1a02/67fc3/696e1/2f4f5/ea61d/caf27"

// 	if pathKey.FileName != expectedPathName {
// 		t.Errorf("have %s want %s", pathKey.PathName, expectedPathName)
// 	}
// 	if pathKey.FileName != expectedPathName {
// 		t.Errorf("have %s want %s", pathKey.FileName, expectedOriginalKey)
// 	}
// }

func TestStore(t *testing.T) {
	opts := StoreOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	s := NewStore(opts)

	key := "testkey"

	// data := bytes.NewReader([]byte("some jpg bytes "))
	data := []byte("some jpg bytes ")

	if err := s.writeStream(key, bytes.NewReader(data)); err != nil {
		t.Error(err)
	}

	if ok := s.Has(key); !ok {
		t.Errorf("key %s should exist", key)
	}

	r, err := s.Read(key)
	if err != nil {
		t.Error(err)
	}

	b, _ := ioutil.ReadAll(r)
	if string(b) != string(data) {
		t.Errorf("want %s have %s", data, b)

	}
	s.Delete(key)
}
