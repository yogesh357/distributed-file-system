package p2p

import (
	"fmt"
	"net"
	"sync"
)

// TCPPeer represnts remote node over TCP established connection.
type TCPPeer struct {
	// conn is underlying connection of peer
	conn net.Conn

	// if we dial a connection => outbound == true
	// if we dial a connection => outbound == false
	outbound bool
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		conn:     conn,
		outbound: outbound,
	}
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakerFunc
	Decoder       Decoder
}

type TCPTransport struct {
	TCPTransportOpts
	listener net.Listener

	mu    sync.RWMutex // this will protect peers?
	peers map[net.Addr]Peer
}

func NewTCPTransport(opts TCPTransportOpts) TCPTransport {
	return TCPTransport{
		TCPTransportOpts: opts,
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.ListenAddr)

	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("TCP accept error : %s\n", err)
		}
		fmt.Printf("new incoming connection %+v\n", conn)

		go t.handleConn(conn)

	}

}

type Temp struct{}

func (t *TCPTransport) handleConn(conn net.Conn) {
	peer := NewTCPPeer(conn, true)

	if err := t.HandshakeFunc(peer); err != nil {
		conn.Close()
		fmt.Printf("handshake failed with peer %+v, err: %s\n", peer, err)
		return
	}

	// Read loop
	msg := &Message{}
	for {
		if err := t.Decoder.Decode(conn, msg); err != nil {
			fmt.Printf("TCP error : %s\n", err)
			continue
		}
		msg.From = conn.RemoteAddr()
		fmt.Printf("Message: %+v\n", msg)
	}

}
