package cmd

import (
	"context"
	"fmt"
	"go-peek/internal/app"
	"go-peek/pkg/archive"
	"go-peek/pkg/ingest/kafka"
	"go-peek/pkg/models/consumer"
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
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		folder := viper.GetString(cmd.Name() + ".output.folder")
		if folder == "" {
			app.Throw("init", fmt.Errorf("Please configure output folder"))
		}

		ctxReader, cancelReader := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		logger.Info("Creating kafka consumer")
		input, err := kafka.NewConsumer(&kafka.Config{
			Name:          cmd.Name() + " consumer",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        viper.GetStringSlice(cmd.Name() + ".input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafka.OffsetLastCommit,
		})
		app.Throw("archive consumer", err)

		logger.Debug("creating channels")
		rx := input.Messages()
		tx := make(chan consumer.Message, 0)
		defer close(tx)

		logrus.Info("creating writer")
		arch, err := archive.NewHandle(archive.Config{
			Directory:      viper.GetString(cmd.Name() + ".output.folder"),
			RotateInterval: viper.GetDuration(cmd.Name() + ".output.rotate.interval"),
			Stream:         tx,
			Logger:         logger,
		})
		app.Throw("logfile output creation", err)

		logrus.Debug("starting up writer")
		ctxWriter, cancelWriter := context.WithCancel(context.Background())
		app.Throw("writer routine create", arch.Do(ctxWriter, &wg))

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		logger.Info("Starting main loop")
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
			case err := <-arch.Errors:
				app.Throw("writer error", err)
			}
		}
		cancelReader()
		cancelWriter()
		wg.Wait()
	},
}

func init() {
	rootCmd.AddCommand(archiveCmd)

	archiveCmd.PersistentFlags().String("output-folder", "", "Output folder for archive")
	viper.BindPFlag("archive.output.folder", archiveCmd.PersistentFlags().Lookup("output-folder"))

	archiveCmd.PersistentFlags().Duration("output-rotate-interval", 1*time.Hour, "Interval between file rotations")
	viper.BindPFlag("archive.output.rotate.interval", archiveCmd.PersistentFlags().Lookup("output-rotate-interval"))

	app.RegisterInputKafkaGenericSimple("archive", archiveCmd.PersistentFlags())
}
