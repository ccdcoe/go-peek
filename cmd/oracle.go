package cmd

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"go-peek/internal/app"
	kafkaIngest "go-peek/pkg/ingest/kafka"
	"go-peek/pkg/mitremeerkat"
	"go-peek/pkg/oracle"
	"go-peek/pkg/persist"
	"go-peek/pkg/providentia"
	"net/http"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// oracleCmd represents the oracle command
var oracleCmd = &cobra.Command{
	Use:   "oracle",
	Short: "Yellow team know-it-all API",
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		var wg sync.WaitGroup
		// defer wg.Wait()

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		workdir := viper.GetString("work.dir")
		if workdir == "" {
			app.Throw("app init", errors.New("missing working directory"))
		}
		workdir = path.Join(workdir, cmd.Name())
		ctxPersist, cancelPersist := context.WithCancel(context.Background())
		persist, err := persist.NewBadger(persist.Config{
			Directory:     path.Join(workdir, "badger"),
			IntervalGC:    1 * time.Minute,
			RunValueLogGC: true,
			WaitGroup:     &wg,
			Ctx:           ctxPersist,
			Logger:        logger,
		})
		app.Throw("persist setup", err)
		defer persist.Close()
		defer cancelPersist()

		ctxReader, cancelReader := context.WithCancel(context.Background())

		topicMitreMeerkat := viper.GetString(cmd.Name() + ".input.kafka.topic_sid_mitre")
		topicAssets := viper.GetString(cmd.Name() + ".input.kafka.topic_assets")
		topicInOracle := viper.GetString(cmd.Name() + ".input.kafka.topic_oracle")

		logger.Info("Creating kafka consumer for event stream")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics: []string{
				topicMitreMeerkat,
				topicAssets,
				topicInOracle,
			},
			Ctx:        ctxReader,
			OffsetMode: kafkaOffset,
		})
		app.Throw(cmd.Name()+" event stream setup", err)
		defer cancelReader()

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		data := oracle.NewData()

		err = persist.GetSingle(cmd.Name()+"-data", func(b []byte) error {
			buf := bytes.NewBuffer(b)
			return gob.NewDecoder(buf).Decode(data)
		})
		if err != nil && err == badger.ErrKeyNotFound {
			logger.Warn("no persistance to load")
		} else if err != nil {
			app.Throw("persist load", err)
		}

		tickUpdateData := time.NewTicker(5 * time.Second)
		defer tickUpdateData.Stop()

		s := &oracle.Server{}
		if data.IoC != nil {
			for _, item := range data.IoC {
				s.IoC.Add(item, true)
			}
			logger.WithFields(logrus.Fields{
				"offset": s.IoC.Offset(),
				"items":  s.IoC.Len(),
			}).Info("loaded persistent IoC data")
		}

		s.Routes()
		wg.Add(1)
		go func() {
			defer wg.Done()
			port := viper.GetInt(cmd.Name() + ".port")
			logrus.Infof("Starting up REST API on port %d", port)
			logrus.Error(http.ListenAndServe(fmt.Sprintf(":%d", port), s.Router))
		}()

		logger.Info("starting event loop")

	loop:
		for {
			select {
			case <-tickUpdateData.C:
				logger.WithFields(logrus.Fields{
					"assets":          len(data.Assets),
					"sid_map":         len(data.Meerkat),
					"missing_sid_map": len(data.MissingSidMaps),
				}).Info("updating containers")
				s.Assets.Update(data.Assets)
				s.SidMap.Update(data.Meerkat)
				s.MissingSidMaps.Copy(data.MissingSidMaps)
				data.IoC = s.IoC.Extract()
				persist.SetSingle(cmd.Name()+"-data", data)
			case <-chTerminate:
				break loop
			case msg, ok := <-input.Messages():
				if !ok {
					break loop
				}
				switch msg.Source {
				case topicInOracle:
					switch msg.Key {
					case "meerkat_missing_sid_map":
						var obj mitremeerkat.Mappings
						if err := json.Unmarshal(msg.Data, &obj); err != nil {
							logger.WithFields(logrus.Fields{
								"raw":    string(msg.Data),
								"source": msg.Source,
								"err":    err,
							}).Error("unable to parse asset")
							continue loop
						}
						data.MissingSidMaps = obj
					}
				case topicAssets:
					var obj providentia.Record
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						logger.WithFields(logrus.Fields{
							"raw":    string(msg.Data),
							"source": msg.Source,
							"err":    err,
						}).Error("unable to parse asset")
						continue loop
					}
					data.Assets[obj.Addr.String()] = obj
				case topicMitreMeerkat:
					var obj mitremeerkat.Mapping
					if err := json.Unmarshal(msg.Data, &obj); err != nil {
						logger.WithFields(logrus.Fields{
							"raw":    string(msg.Data),
							"source": msg.Source,
							"err":    err,
						}).Error("unable to parse asset")
						continue loop
					}
					data.Meerkat[obj.SID] = obj
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(oracleCmd)

	pFlags := oracleCmd.PersistentFlags()

	pFlags.Int("port", 8085, "API listener port")
	viper.BindPFlag(oracleCmd.Name()+".port", pFlags.Lookup("port"))

	app.RegisterInputKafkaCore(oracleCmd.Name(), pFlags)
	app.RegisterInputKafkaOracle(oracleCmd.Name(), pFlags)
	app.RegisterInputKafkaEnrich(oracleCmd.Name(), pFlags)
}
