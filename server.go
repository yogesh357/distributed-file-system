package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

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

func (s *FilerServer) stream(msg *Message) error {
	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	mw := io.MultiWriter(peers...)

	return gob.NewEncoder(mw).Encode(msg)
}

func (s *FilerServer) broadcast(msg *Message) error {

	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	Key  string
	Size int64
}

type MessageGetFile struct {
	Key string
}

func (s *FilerServer) Get(key string) (io.Reader, error) {
	if s.store.Has(key) {
		return s.store.Read(key)
	}
	fmt.Printf("dont have this file (%s) locally , fetching form network ...\n", key)
	msg := Message{
		Payload: MessageGetFile{
			Key: key,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}
	time.Sleep(time.Millisecond * 5)
	for _, peer := range s.peers {
		fmt.Println("receiving stream from ", peer.RemoteAddr())
		fileBuffer := new(bytes.Buffer)
		n, err := io.Copy(fileBuffer, peer)
		if err != nil {
			return nil, err
		}
		fmt.Println("received bytes over the network", n)
		fmt.Println(fileBuffer.String())
	}

	select {}
	return nil, nil
}

func (s *FilerServer) Store(key string, r io.Reader) error {
	//1. store this file to disk
	// 2.2boradcast this file to all the peers in the network
	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	size, err := s.store.Write(key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			Key:  key,
			Size: size,
		},
	}
	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(3 * time.Second)

	for _, peer := range s.peers {
		peer.Send([]byte{p2p.IncomingStream})
		n, err := io.Copy(peer, fileBuffer)
		if err != nil {
			return err
		}
		fmt.Println("received and written bytes to disk ", n)

		// fmt.Printf("[%s] received and written (%d) bytes to disk\n", s.Transport.Addr(), n)

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
		log.Printf("file server stopped due to error or user quite action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error:", err)
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handling message error :", err)
			}

			//: not needed
			// fmt.Printf("%+v\n", msg.Payload)

			// peer, ok := s.peers[rpc.From]
			// if !ok {
			// 	panic("peer not found in peer map")
			// }
			// b := make([]byte, 1000)
			// if _, err := peer.Read(b); err != nil {
			// 	log.Panic(err)
			// }

			// fmt.Printf(" %s \n", string(b))
			// peer.(*p2p.TCPPeer).Wg.Done() //? what the fuck is this syntax

			// if err := s.handleMessage(&m); err != nil {
			// 	log.Println(err)
			// }
			// fmt.Printf("%+v\n", string(m.Data))
		case <-s.quitch:
			return
		}
	}
}

func (s *FilerServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	}
	return nil
}

func (s *FilerServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.Key) {
		return fmt.Errorf("server file (%s) but  does  not exist on the disk\n", msg.Key)
	}

	fmt.Printf("reading the file (%s) from disk and sending it over the network to %s\n", msg.Key, from)

	r, err := s.store.Read(msg.Key)
	if err != nil {
		return err
	}
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("written %d byes over the netwrok to %s\n", n, from)
	return nil
}

func (s *FilerServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}
	n, err := s.store.Write(msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return nil
	}

	fmt.Printf("[%s] written %d bytes to disk\n", s.Transport.Addr(), n)

	peer.(*p2p.TCPPeer).Wg.Done()

	return nil
}

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

// ? what is need for this
func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}
