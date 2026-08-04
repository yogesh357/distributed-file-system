package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/yogesh/filesystem/p2p"
)

type FilerServerOpts struct {
	ID                string
	EncKey            []byte
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

	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}

	return &FilerServer{
		FilerServerOpts: opts,
		store:           NewStore(storeOpts),
		quitch:          make(chan struct{}),
		peers:           make(map[string]p2p.Peer),
	}
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
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}

func (s *FilerServer) Get(key string) (io.Reader, error) {
	if s.store.Has(s.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n", s.Transport.Addr(), key)

		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}
	fmt.Printf("[%s] dont have this file (%s) locally , fetching form network ...\n", s.Transport.Addr(), key)

	msg := Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: hashKey(key),
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 500)

	for _, peer := range s.peers {
		// First read the file size so we can limit amount  of bytes that we read form connection , so it will not keep hanging
		var fileSize int64
		//       connection ,
		binary.Read(peer, binary.BigEndian, &fileSize)

		n, err := s.store.WriteDecrypt(s.EncKey, s.ID, key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}

		fmt.Printf("[%s] received  (%d) bytes over the network from (%s\n", s.Transport.Addr(), n, key)

		peer.CloseStream()
	}
	_, r, err := s.store.Read(s.ID, key)
	return r, err
}

func (s *FilerServer) Store(key string, r io.Reader) error {
	//1. store this file to disk
	// 2.2boradcast this file to all the peers in the network
	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	size, err := s.store.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  hashKey(key),
			Size: size + 16,
		},
	}
	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(3 * time.Second)

	peers := []io.Writer{}

	for _, peer := range s.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})
	n, err := copyEncrypt(s.EncKey, fileBuffer, mw)
	if err != nil {
		return nil
	}
	fmt.Printf("[%s] received and written (%d) bytes to disk\n", s.Transport.Addr(), n)

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
	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] server file (%s) but  does  not exist on the disk\n", s.Transport.Addr(), msg.Key)
	}

	fmt.Printf("[%s] serving file (%s) over the network to %s\n", s.Transport.Addr(), msg.Key, from)

	fileSize, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readCloser")
		defer rc.Close()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	// First send  the "incomingstream" byte to the peer and then we can send file size as int64
	// this will
	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.BigEndian, fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written (%d) byes over the network to %s\n", s.Transport.Addr(), n, from)

	return nil
}

func (s *FilerServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}
	n, err := s.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return nil
	}

	fmt.Printf("[%s] written %d bytes to disk\n", s.Transport.Addr(), n)

	peer.CloseStream()

	return nil
}

func (s *FilerServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			fmt.Printf("[%s] attemping to connect with remote (%s)", s.Transport.Addr(), addr)
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
