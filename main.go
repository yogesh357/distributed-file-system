package main

import (
	"log"

	"github.com/yogesh/filesystem/p2p"
)

func main() {
	tcpOpts := p2p.TCPTransportOpts{
		ListenAddr:    ":8080",
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tr := p2p.NewTCPTransport(tcpOpts)

	if err := tr.ListenAndAccept(); err != nil {
		log.Fatalf("failed to listen and accept: %v", err)
	}

	//? what is this
	select {}
}
