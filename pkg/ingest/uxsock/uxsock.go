package uxsock

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"go-peek/pkg/models/consumer"
	"go-peek/pkg/models/events"
	"go-peek/pkg/utils"
)

const (
	bufsize = 32 * 1024 * 1024
)

type ErrSocketCreate struct {
	Path string
	Err  error
}

func (e ErrSocketCreate) Error() string {
	return fmt.Sprintf("Socket: %s Error: [%s]", e.Path, e.Err)
}

type Config struct {
	Sockets []string
	MapFunc func(string) events.Atomic
	Ctx     context.Context
	Force   bool
}

func (c *Config) Validate() error {
	if c.Sockets == nil || len(c.Sockets) == 0 {
		return fmt.Errorf("Unix socket input has no paths configured")
	}
	if c.MapFunc == nil {
		c.MapFunc = func(string) events.Atomic {
			return events.SimpleE
		}
	}
	if c.Ctx == nil {
		c.Ctx = context.Background()
	}
	return nil
}

type handle struct {
	path     string
	listener *net.UnixListener
	atomic   events.Atomic
}

type Consumer struct {
	h        []*handle
	tx       chan *consumer.Message
	ctx      context.Context
	stoppers utils.WorkerStoppers
	errs     *utils.ErrChan
	timeouts int
}

func NewConsumer(c *Config) (*Consumer, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	con := &Consumer{
		tx:   make(chan *consumer.Message, 0),
		ctx:  c.Ctx,
		h:    make([]*handle, 0),
		errs: utils.NewErrChan(100, "uxsock consume"),
	}
	for _, f := range c.Sockets {
		if !utils.FileNotExists(f) && c.Force {
			if err := os.Remove(f); err != nil {
				return nil, err
			}
		}
		sock, err := net.Listen("unix", f)
		if err != nil {
			return nil, &ErrSocketCreate{
				Path: f,
				Err:  err,
			}
		}
		con.h = append(con.h, &handle{
			path:     f,
			listener: sock.(*net.UnixListener),
			atomic:   c.MapFunc(f),
		})
	}
	var wg sync.WaitGroup
	con.stoppers = utils.NewWorkerStoppers(len(con.h))
	go func() {
		<-con.ctx.Done()
		log.Trace("unix socket consumer caught cancel signal")
		con.close()
	}()
	go func() {
		defer close(con.tx)
		defer func() { log.Tracef("All %d unix socket consumers exited", len(con.h)) }()
		for i, h := range con.h {
			log.WithFields(log.Fields{
				"id":     i,
				"action": "worker spawn",
				"module": "uxsock",
				"path":   h.path,
			}).Trace()
			wg.Add(1)
			go func(id int, ctx context.Context, h handle) {
				defer h.listener.Close()
				defer socketCleanUp(h.path)
				defer wg.Done()
			loop:
				for {
					select {
					case <-ctx.Done():
						log.Tracef("breaking uxsock worker %d, %s", id, h.path)
						break loop
					default:
						h.listener.SetDeadline(time.Now().Add(1e9))
						c, err := h.listener.Accept()
						if err != nil {
							if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
								continue loop
							}
							con.errs.Send(err)
						}

						scanner := bufio.NewScanner(c)
						buf := make([]byte, 0, bufsize)
						scanner.Buffer(buf, bufsize)
						for scanner.Scan() {
							select {
							case <-ctx.Done():
								log.Tracef("breaking uxsock worker %d, %s", id, h.path)
								break loop
							default:
								con.tx <- &consumer.Message{
									Data:      utils.DeepCopyBytes(scanner.Bytes()),
									Offset:    -1,
									Partition: -1,
									Type:      consumer.UxSock,
									Event:     h.atomic,
									Source:    h.path,
									Key:       "",
									Time:      time.Now(),
								}
							}
						}
					}
				}
			}(i, con.stoppers[i].Ctx, *h)
		}
		wg.Wait()
	}()
	return con, nil
}

func (c Consumer) Messages() <-chan *consumer.Message { return c.tx }
func (c Consumer) Timeouts() int                      { return c.timeouts }

func (c Consumer) close() error {
	if c.stoppers == nil || len(c.stoppers) == 0 {
		return fmt.Errorf("Cannot close unix socket consumer, not properly instanciated")
	}
	c.stoppers.Close()
	return nil
}

func socketCleanUp(p string) {
	_, err := os.Stat(p)
	if err == nil {
		os.Remove(p)
	}
}

/*
func tester() {
	events, _ := net.Listen("unix", "/tmp/test.sock")
}
*/
