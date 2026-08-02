package p2p

// RPC represents any arbitary data that is sent between nodes in the network.
type RPC struct {
	From    string
	Payload []byte
}
