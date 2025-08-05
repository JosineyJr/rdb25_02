package routing

import (
	"errors"
	"net"
	"sync"
)

type UnixConnPool struct {
	mu      sync.Mutex
	conns   chan *net.UnixConn
	factory func() (*net.UnixConn, error)
}

func NewUnixConnPool(
	initialSize, maxSize int,
	factory func() (*net.UnixConn, error),
) (*UnixConnPool, error) {
	if initialSize < 0 || maxSize <= 0 || initialSize > maxSize {
		return nil, errors.New("invalid capacity settings")
	}

	pool := &UnixConnPool{
		conns:   make(chan *net.UnixConn, maxSize),
		factory: factory,
	}

	for range initialSize {
		conn, err := factory()
		if err != nil {
			close(pool.conns)
			for c := range pool.conns {
				c.Close()
			}
			return nil, err
		}
		pool.conns <- conn
	}

	return pool, nil
}

func (p *UnixConnPool) Get() (*net.UnixConn, error) {
	select {
	case conn := <-p.conns:
		if conn == nil {
			return nil, errors.New("connection is nil from pool")
		}
		return conn, nil
	default:
		return p.factory()
	}
}

func (p *UnixConnPool) Put(conn *net.UnixConn) {
	if conn == nil {
		return
	}

	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

func (p *UnixConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conns == nil {
		return
	}

	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
}
