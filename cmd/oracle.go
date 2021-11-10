package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"go-peek/internal/app"
	"go-peek/pkg/ingest/kafka"
	kafkaIngest "go-peek/pkg/ingest/kafka"
	"go-peek/pkg/oracle"
	"go-peek/pkg/providentia"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

		ctxReader, cancelReader := context.WithCancel(context.Background())

		topicMitreMeerkat := viper.GetString(cmd.Name() + ".input.kafka.topic_sid_mitre")
		topicAssets := viper.GetString(cmd.Name() + ".input.kafka.topic_assets")

		logger.Info("Creating kafka consumer for event stream")
		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics: []string{
				topicMitreMeerkat,
				topicAssets,
			},
			Ctx:        ctxReader,
			OffsetMode: kafka.OffsetLastCommit,
		})
		app.Throw(cmd.Name()+" event stream setup", err)
		defer cancelReader()

		s := oracle.Server{}
		s.Routes()
		wg.Add(1)
		go func() {
			defer wg.Done()
			port := viper.GetInt(cmd.Name() + ".port")
			logrus.Infof("Starting up REST API on port %d", port)
			logrus.Error(http.ListenAndServe(fmt.Sprintf(":%d", port), s.Router))
		}()

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		data := oracle.NewData()

		tickUpdateData := time.NewTicker(5 * time.Second)
		defer tickUpdateData.Stop()

		logger.Info("starting event loop")

	loop:
		for {
			select {
			case <-tickUpdateData.C:
				logger.WithFields(logrus.Fields{
					"assets": len(data.Assets),
				}).Info("updating containers")
				s.Assets.Update(data.Assets)
			case <-chTerminate:
				break loop
			case msg, ok := <-input.Messages():
				if !ok {
					break loop
				}
				switch msg.Source {
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
	app.RegisterInputKafkaEnrich(oracleCmd.Name(), pFlags)
}
