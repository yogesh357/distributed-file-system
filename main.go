package main

import (
	"bytes"
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

	go func() {
		log.Fatal(s1.Start())
	}()
	time.Sleep(time.Second * 4)

	go s2.Start()
	time.Sleep(time.Second * 4)

	data := bytes.NewReader([]byte("hello world"))

	s2.StoreData("myprivatekey", data)

	select {}
}
