/*
We don't have Delete that will delete file from peers on the network

*/

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/yogesh/filesystem/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FilerServer {
	rcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(rcpTransportOpts)

	fileServerOpts := FilerServerOpts{
		EncKey:            newEncryptionKey(),
		StorageRoot:       listenAddr + "_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}

	s := NewFileServer(fileServerOpts)

	tcpTransport.OnPeer = s.OnPeer

	return s

}

func main() {
	s1 := makeServer(":3000", "")
	s2 := makeServer(":4000", ":3000")
	s3 := makeServer(":5000", ":3000", ":4000")

	// for this the s1 is blocking the s2 ? why ??
	// go func() {
	// 	log.Fatal(s1.Start())
	// 	time.Sleep(500 * time.Millisecond)
	// 	log.Fatal(s2.Start())
	// }()

	go func() { log.Fatal(s1.Start()) }()
	time.Sleep(time.Millisecond * 400)

	go func() { log.Fatal(s2.Start()) }()
	time.Sleep(time.Millisecond * 400)

	go func() { log.Fatal(s3.Start()) }()
	time.Sleep(time.Second * 2)

	//: LOOP

	// for i := 0; i < 10; i++ {
	// 	key := fmt.Sprintf("myprivatekey_%d", i)
	// 	data := bytes.NewReader([]byte("hello world !! hello world !!"))
	// 	if err := s2.Store(key, data); err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	time.Sleep(5 * time.Millisecond)
	// }

	for i := 0; i < 20; i++ {

		key := fmt.Sprintf("picture_%d.jpg", i)
		data := bytes.NewReader([]byte("MY BIG DATA"))
		if err := s3.Store(key, data); err != nil {
			log.Fatal(err)
		}
		if err := s3.store.Delete(s3.ID, key); err != nil {
			log.Fatal(err)
		}

		r, err := s3.Get(key)
		if err != nil {
			log.Fatal(err)
		}

		b, err := ioutil.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	}

}
