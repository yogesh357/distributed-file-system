package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/yogesh/filesystem/p2p"
)

type FilerServerOpts struct {
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FilerServer struct {
	FilerServerOpts

	peerLock sync.Mutex
	peers    map[string]p2p.Peer

	store  *Store
	quitch chan struct{}
}

func NewFileServer(opts FilerServerOpts) *FilerServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	return &FilerServer{
		FilerServerOpts: opts,
		store:           NewStore(storeOpts),
		quitch:          make(chan struct{}),
		peers:           make(map[string]p2p.Peer),
	}
}

type Message struct {
	// From    string
	Payload any
}

// type DataMessage struct {
// 	Key  string
// 	Data []byte
// }

func (s *FilerServer) broadCast(msg *Message) error {
	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	mw := io.MultiWriter(peers...)

	return gob.NewEncoder(mw).Encode(msg)
}

func (s *FilerServer) StoreData(key string, r io.Reader) error {
	//1. store this file to disk
	// 2.2boradcast this file to all the peers in the network

	// buf := new(bytes.Buffer)
	msg := Message{
		Payload: []byte("StorageKey"),
	}

	payloadBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(payloadBuf).Encode(msg); err != nil {
		return err
	}
	rpc := p2p.RPC{
		Payload: payloadBuf.Bytes(),
	}

	wireBuf := new(bytes.Buffer)
	if err := gob.NewEncoder(wireBuf).Encode(rpc); err != nil {
		return err
	}

	for _, peer := range s.peers {
		if err := peer.Send(wireBuf.Bytes()); err != nil {
			return err
		}
	}
	payload := []byte("THIS IS LARGE FILE")
	for _, peer := range s.peers {
		if err := peer.Send(payload); err != nil {
			return err
		}
	}

	return nil
	// buf := new(bytes.Buffer)
	// tee := io.TeeReader(r, buf)
	// if err := s.store.Write(key, tee); err != nil {
	// 	return err
	// }

	// p := &DataMessage{
	// 	Key:  key,
	// 	Data: buf.Bytes(),
	// }

	// // fmt.Println(buf.String())

	// return s.broadCast(&Message{
	// 	From:    "todo",
	// 	Payload: p,
	// })

}

func (s *FilerServer) Stop() {
	close(s.quitch)
}

func (s *FilerServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	s.peers[p.RemoteAddr().String()] = p

	log.Printf("connecte with remote peer %s", p.RemoteAddr())
	return nil
}

func (s *FilerServer) loop() {
	defer func() {
		log.Printf("file server stopped due to user quite action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println(err)
			}
			peer, ok := s.peers[rpc.From]
			if !ok {
				log.Panic("peer not found")
			}
			fmt.Println(peer)

			fmt.Printf("recv: %s", string(msg.Payload.([]byte)))

			// if err := s.handleMessage(&m); err != nil {
			// 	log.Println(err)
			// }
		case <-s.quitch:
			return
		}
	}
}

// func (s *FilerServer) handleMessage(msg *Message) error {
// 	switch v := msg.Payload.(type) {
// 	case *DataMessage:
// 		fmt.Printf("received data : %+v\n", v)
// 	}
// 	return nil
// }

func (s *FilerServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial error :", err)
			}
		}(addr)
	}
	return nil

}

func (s *FilerServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}
	s.bootstrapNetwork()
	s.loop()
	return nil
}

func inti() {
	// gob.Register(DataMessage[])
}
