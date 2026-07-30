package p2p

import (
	"fmt"
	"net"
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

// Close implements the Peer interface, which will close the underlying connection of peer.
func (p *TCPPeer) Close() error {
	return p.conn.Close()
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakerFunc
	Decoder       Decoder
}

type TCPTransport struct {
	TCPTransportOpts
	listener net.Listener
	rpcch    chan RPC

	// mu    sync.RWMutex // this will protect peers?
	// peers map[net.Addr]Peer
}

func NewTCPTransport(opts TCPTransportOpts) TCPTransport {
	return TCPTransport{
		TCPTransportOpts: opts,
		rpcch:            make(chan RPC),
	}
}

// we can only read from channel, so we can only consume messages from the channel
// Consume implements the Transport interface , which will return read-only channel for readin incoming messages from another peer in network.
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
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
	rpc := RPC{}
	for {
		if err := t.Decoder.Decode(conn, &rpc); err != nil {
			fmt.Printf("TCP error : %s\n", err)
			continue
		}
		rpc.From = conn.RemoteAddr()
		t.rpcch <- rpc
	}

}
