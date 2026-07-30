package main

import (
	"bytes"
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	key := "fuxkinggoodpics"
	pathKey := CASPathTransformFunc(key)

	expectedOriginalKey := "61b90/280e3/a1a02/67fc3/696e1/2f4f5/ea61d/caf27"
	expectedPathName := "61b90/280e3/a1a02/67fc3/696e1/2f4f5/ea61d/caf27"

	if pathKey.Original != expectedPathName {
		t.Errorf("have %s want %s", pathKey.PathName, expectedPathName)
	}
	if pathKey.Original != expectedPathName {
		t.Errorf("have %s want %s", pathKey.Original, expectedOriginalKey)
	}
}

func TestStore(t *testing.T) {
	opts := StoreOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	s := NewStore(opts)

	data := bytes.NewReader([]byte("some jpg bytes "))
	if err := s.writeStream("testkey", data); err != nil {
		t.Error(err)
	}
}
