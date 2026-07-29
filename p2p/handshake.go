package p2p

type HandshakerFunc func(Peer) error

// type DefaultHandshaker struct {}

func NOPHandshakeFunc(Peer) error {
	return nil
}
