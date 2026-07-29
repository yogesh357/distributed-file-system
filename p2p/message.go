package p2p

import "net"

// Message represents any arbitary data that is sent between nodes in the network.
type Message struct {
	From    net.Addr
	Payload []byte
}
