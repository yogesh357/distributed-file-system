package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"
)

func newStore() *Store {
	opts := StoreOpts{
		PathTransformFunc: CASPathTransformFunc,
	}
	return NewStore(opts)
}

func teardown(t *testing.T, s *Store) {
	if err := s.Clear(); err != nil {
		t.Error(err)
	}
}

// TODO check this why it is failing
func TestPathTransformFunc(t *testing.T) {
	key := "fuxkinggoodpics"
	pathKey := CASPathTransformFunc(key)

	expectedFileName := "61b90280e3a1a0267fc3696e12f4f5ea61dcaf27"
	expectedPathName := "61b90/280e3/a1a02/67fc3/696e1/2f4f5/ea61d/caf27"

	if pathKey.PathName != expectedPathName {
		t.Errorf("have %s want %s", pathKey.PathName, expectedPathName)
	}
	if pathKey.FileName != expectedFileName {
		t.Errorf("have %s want %s", pathKey.FileName, expectedFileName)
	}
}

func TestStore(t *testing.T) {
	s := newStore()
	defer teardown(t, s)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("testkey%d", i)

		data := []byte(fmt.Sprintf("some jpg bytes %d", i))

		// key := "testkey"

		// data := []byte("some jpg bytes ")

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
		if err := s.Delete(key); err != nil {
			t.Error(err)
		}
		if ok := s.Has(key); ok {
			t.Errorf("key %s should not exist", key)
		}
	}
}
