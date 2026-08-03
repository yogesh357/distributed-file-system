package p2p

const (
	IncomingMessage = 0x1
	IncomingStream  = 0x2
)

// RPC represents any arbitary data that is sent between nodes in the network.
type RPC struct {
	From    string
	Payload []byte
	Stream  bool
}
