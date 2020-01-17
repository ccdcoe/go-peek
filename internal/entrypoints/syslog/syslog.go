package syslog

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	stdLibAtomic "sync/atomic"

	"github.com/ccdcoe/go-peek/internal/engines/shipper"
	"github.com/ccdcoe/go-peek/pkg/models/atomic"
	"github.com/ccdcoe/go-peek/pkg/models/consumer"
	"github.com/ccdcoe/go-peek/pkg/models/fields"
	"github.com/ccdcoe/go-peek/pkg/utils"
	"github.com/influxdata/go-syslog/rfc5424"
	log "github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Stats struct {
	ParseErrs int64
}

func Entrypoint(cmd *cobra.Command, args []string) {
	addr := fmt.Sprintf("0.0.0.0:%d", viper.GetInt("syslog.port"))
	ServerAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	/* Now listen at selected port */
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer ServerConn.Close()
	log.Infof("Spawned syslog server on %s", addr)

	buf := make([]byte, 1024*64)
	rx := make(chan *consumer.Message, 1024)
	tx := make(chan *consumer.Message, 1024)
	stats := &Stats{}

	var wg sync.WaitGroup
	go func() {
		defer close(tx)
		for i := 0; i < viper.GetInt("work.threads"); i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				p := rfc5424.NewParser()
				bestEffort := true
			loop:
				for item := range rx {
					msg, err := p.Parse(item.Data, &bestEffort)
					if err != nil {
						log.WithFields(log.Fields{
							"msg": string(item.Data),
						}).Error(err)
						stdLibAtomic.AddInt64(&stats.ParseErrs, 1)
						continue loop
					}
					s := atomic.Syslog{
						Timestamp: *msg.Timestamp(),
						Host:      *msg.Hostname(),
						Program:   *msg.Appname(),
						Severity:  *msg.SeverityLevel(),
						Facility:  *msg.FacilityLevel(),
						Message:   *msg.Message(),
						IP:        &fields.StringIP{IP: item.Sender},
					}
					// TODO - make parsing optional by flag
					if viper.GetBool("syslog.msg.parse") {
						event, err := atomic.ParseSyslogMessage(s)
						if err != nil {
							log.WithFields(log.Fields{
								"msg": string(item.Data),
								"err": err,
							}).Error("Unable to parse syslog msg to atomic event")
							continue loop
						}
						if viper.GetBool("syslog.msg.drop") {
							s.Message = "parsed"
						}
						if b, err := json.Marshal(&struct {
							Syslog atomic.Syslog
							Event  interface{}
						}{
							Syslog: s,
							Event:  event,
						}); err != nil {
							log.WithFields(log.Fields{
								"msg": string(item.Data),
								"err": err,
							}).Error("Unable to marshal json")
							continue loop
						} else {
							item.Data = b
						}
					} else {
						b, err := json.Marshal(s)
						if err != nil {
							log.WithFields(log.Fields{
								"msg":    string(item.Data),
								"parsed": s,
							}).Error(err)
							continue loop
						}
						item.Data = b

					}
					item.Time = s.Timestamp
					item.Partition = int64(id)
					tx <- item
				}
			}(i)
		}
		wg.Wait()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func(ctx context.Context) {
		defer close(rx)
		var count int64
		statReport := time.NewTicker(3 * time.Second)
	loop:
		for {
			select {
			case <-c:
				log.Debug("Caught SIGINT")
				break loop
			case <-statReport.C:
				log.WithFields(log.Fields{
					"5424 parse errors": stats.ParseErrs,
				}).Info()
			default:
				ServerConn.SetDeadline(time.Now().Add(1e9))
				n, ip, err := ServerConn.ReadFromUDP(buf)
				if err != nil {
					if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
						continue loop
					}
					log.Error(err)
				}
				rx <- &consumer.Message{
					Sender: ip.IP,
					Data:   utils.DeepCopyBytes(buf[0:n]),
					Time:   time.Now(),
					Offset: count,
					Source: addr,
				}
				count++
			}
		}
	}(context.TODO())

	if err := shipper.Send(tx, "output"); err != nil {
		log.Fatal(err)
	}
}
