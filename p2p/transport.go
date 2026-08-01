package p2p

import "net"

// Peer is interface that represents the remote node.
type Peer interface {
	net.Conn
	Send([]byte) error
	RemoteAddr() net.Addr
	Close() error
}

/*
Transport is anything that handles communication between nodes in the network.
This can be of the form (TCP , UDP,webscoket).
*/
type Transport interface {
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
	Dial(string) error
	// ListenAddr() string
}
