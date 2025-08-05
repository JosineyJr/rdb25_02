package health

import (
	"net"
	"sync"
)

type HealthBroadcaster struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

func NewHealthBroadcaster() *HealthBroadcaster {
	return &HealthBroadcaster{
		conns: make(map[net.Conn]struct{}),
	}
}

func (b *HealthBroadcaster) Add(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.conns[conn] = struct{}{}
}

func (b *HealthBroadcaster) Remove(conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()

	delete(b.conns, conn)
	conn.Close()
}

func (b *HealthBroadcaster) Broadcast(msg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for conn := range b.conns {
		go func(c net.Conn) {
			if _, err := conn.Write(msg); err != nil {
				b.Remove(c)
			}
		}(conn)
	}
}
