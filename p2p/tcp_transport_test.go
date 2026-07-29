package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	listenAddress := ":4000"
	transport := NewTCPTransport(listenAddress)

	assert.Equal(t, transport.listenAddress, listenAddress)

	// server
	assert.Nil(t, transport.ListenAndAccept())
}
