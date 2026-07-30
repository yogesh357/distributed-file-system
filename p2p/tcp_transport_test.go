package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	opts := TCPTransportOpts{
		ListenAddr:    ":4000",
		HandshakeFunc: nil,
		Decoder:       nil,
	}
	transport := NewTCPTransport(opts)

	assert.Equal(t, opts.ListenAddr, ":4000")

	// server
	assert.Nil(t, transport.ListenAndAccept())
}
