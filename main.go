package main

import (
	"fmt"
	"log"

	"github.com/yogesh/filesystem/p2p"
)

func OnPeer(peer p2p.Peer) error {
	peer.Close()
	// return fmt.Errorf("Failed the onpeer func")
	// fmt.Println("OnPeer function called for new peer connection")
	return nil
}

func main() {
	tcpOpts := p2p.TCPTransportOpts{
		ListenAddr:    ":8080",
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
		OnPeer:        OnPeer,
	}
	tr := p2p.NewTCPTransport(tcpOpts)

	go func() {
		for {
			msg := <-tr.Consume()
			fmt.Printf("Received message from %s: %s\n", msg.From, string(msg.Payload))
		}
	}()

	if err := tr.ListenAndAccept(); err != nil {
		log.Fatalf("failed to listen and accept: %v", err)
	}

	//? what is this
	select {}
}
