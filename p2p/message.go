package p2p

import "net"

// RPC represents any arbitary data that is sent between nodes in the network.
type RPC struct {
	From    net.Addr
	Payload []byte
}
