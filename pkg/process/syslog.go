package process

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type SyslogServer struct {
	Listener  *net.UDPConn
	Collector *Collector
	Errors    chan error
}

func (s SyslogServer) Run(wg *sync.WaitGroup, ctx context.Context) error {
	if s.Listener == nil {
		return errors.New("missing UDP handler")
	}
	if wg == nil {
		return errors.New("missing waitgroup")
	}
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		defer s.Listener.Close()

		buf := make([]byte, 1024*64)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
			}
			s.Listener.SetDeadline(time.Now().Add(1e9))
			n, _, err := s.Listener.ReadFromUDP(buf)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue loop
				}
				select {
				case s.Errors <- err:
				default:
				}
			}
			if err := s.Collector.Collect(buf[:n]); err != nil {
				select {
				case s.Errors <- err:
				default:
				}
			}
		}
	}(ctx)
	return nil
}

func NewSyslogServer(fn CollectBulkFullFn) (*SyslogServer, error) {
	if fn == nil {
		return nil, errors.New("missing bulk handler func")
	}
	server := &SyslogServer{}
	addr, err := net.ResolveUDPAddr("udp", "0.0.0.0:514")
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	server.Listener = listener
	server.Collector = &Collector{
		Size:        256 * 1024,
		Ticker:      time.NewTicker(1 * time.Second),
		HandlerFunc: fn,
	}
	server.Errors = make(chan error, 10)
	return server, nil
}
