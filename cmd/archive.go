package cmd

import (
	"context"
	"fmt"
	"go-peek/internal/app"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/outputs/filestorage"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// archiveCmd represents the archive command
var archiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive logs from kafka topics",
	Run: func(cmd *cobra.Command, args []string) {
		start := time.Now()
		logger.WithFields(logrus.Fields{}).Info("archive called")

		defer logger.WithFields(logrus.Fields{"duration": time.Since(start)}).Info("All done!")
		defer app.Catch(logger)

		folder := viper.GetString("archive.output.folder")
		if folder == "" {
			app.Throw("init", fmt.Errorf("Please configure output folder"))
		}

		ctxReader, cancelReader := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		logrus.Info("Creating kafka consumer")
		input, err := kafka.NewConsumer(&kafka.Config{
			Name:          "archive consumer",
			ConsumerGroup: "peek",
			Brokers:       viper.GetStringSlice("archive.input.kafka.brokers"),
			Topics:        viper.GetStringSlice("archive.input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw("archive consumer", err)

		logrus.Debug("creating channels")
		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		logrus.Info("creating writer")
		w, err := filestorage.NewHandle(&filestorage.Config{
			Name:           "archive writer",
			Stream:         tx,
			Dir:            folder,
			RotateEnabled:  true,
			RotateGzip:     true,
			RotateInterval: 5 * time.Minute,
			Timestamp:      true,
		})
		app.Throw("logfile output creation", err)

		ctxWriter, cancelWriter := context.WithCancel(context.Background())

		logrus.Debug("Creating writer workers")
		app.Throw("writer routine create", w.Do(ctxWriter))
		wg.Add(1)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		logrus.Info("Starting main loop")
	loop:
		for {
			select {
			case msg, ok := <-rx:
				if !ok {
					break loop
				}
				tx <- *msg
			case <-chTerminate:
				break loop
			}
		}
		cancelReader()
		cancelWriter()
	},
}

func init() {
	rootCmd.AddCommand(archiveCmd)

	archiveCmd.PersistentFlags().String("output-folder", "", "Output folder for archive")
	viper.BindPFlag("archive.output.folder", archiveCmd.PersistentFlags().Lookup("output-folder"))

	app.RegisterInputKafkaGenericSimple("archive", archiveCmd.PersistentFlags())
}
