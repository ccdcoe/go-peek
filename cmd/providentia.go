package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"go-peek/internal/app"
	"go-peek/pkg/anonymizer"
	"go-peek/pkg/models/consumer"
	"go-peek/pkg/outputs/kafka"
	"go-peek/pkg/persist"
	"go-peek/pkg/providentia"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// providentiaCmd represents the providentia command
var providentiaCmd = &cobra.Command{
	Use:   "providentia",
	Short: "Pull asset data from providentia API",
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ticker := time.NewTicker(viper.GetDuration(cmd.Name() + ".interval"))
		defer ticker.Stop()

		var wg sync.WaitGroup
		// defer wg.Wait()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx := make(chan consumer.Message, 10)
		if viper.GetBool(cmd.Name() + ".output.kafka.enabled") {
			producer, err := kafka.NewProducer(&kafka.Config{
				Brokers: viper.GetStringSlice(cmd.Name() + ".output.kafka.brokers"),
				Logger:  logger,
			})
			app.Throw("Sarama producer init", err, logger)
			topic := viper.GetString(cmd.Name() + ".output.kafka.topic")
			producer.Feed(tx, cmd.Name()+" producer", ctx, func(m consumer.Message) string {
				return topic
			}, &wg)
		}

		workdir := viper.GetString("work.dir")
		if workdir == "" {
			app.Throw("app init", errors.New("missing working directory"), logger)
		}
		workdir = path.Join(workdir, cmd.Name())

		persist, err := persist.NewBadger(persist.Config{
			Directory:     path.Join(workdir, "badger"),
			IntervalGC:    1 * time.Minute,
			RunValueLogGC: true,
			WaitGroup:     &wg,
			Ctx:           context.TODO(),
			Logger:        logger,
		})
		app.Throw("persist setup", err, logger)
		defer persist.Close()

		m, err := anonymizer.NewMapper(
			anonymizer.Config{
				Persist: persist,
				Logger:  logger,
			},
		)
		app.Throw("Anonymizer creation", err, logger)

		chTerminate := make(chan os.Signal, 1)
		signal.Notify(chTerminate, os.Interrupt, syscall.SIGTERM)

		fn := func() {
			targets, err := providentia.Pull(providentia.Params{
				URL:     viper.GetString(cmd.Name() + ".url"),
				Token:   viper.GetString(cmd.Name() + ".token"),
				RawDump: filepath.Join(workdir, "raw-api-result.json"),
			})
			if err != nil {
				logger.WithFields(logrus.Fields{}).Error(err)
				return
			}
			l := logger.WithFields(logrus.Fields{
				"results":  len(targets),
				"url":      viper.GetString(cmd.Name() + ".url"),
				"endpoint": "targets",
			})
			if len(targets) == 0 {
				l.Error("API call done, no inventory extracted")
				return
			} else {
				l.Info("API call done")
			}

			mapped, err := providentia.MapTargets(targets, m)
			app.Throw("target renaming", err)
			app.DumpJSON(filepath.Join(workdir, "mapped_targets.json"), mapped)

			assets := providentia.ExtractAddrs(mapped, logger)
			app.DumpJSON(filepath.Join(workdir, "assets.json"), assets)

			logger.WithFields(logrus.Fields{
				"addrs":             len(assets),
				"rename_new":        m.Misses,
				"rename_cache_hits": m.Hits,
			}).Info("Assets extracted")

			now := time.Now()
			for _, item := range assets {
				encoded, err := json.Marshal(item)
				app.Throw("Output JSON encode", err)
				if viper.GetBool(cmd.Name() + ".output.kafka.enabled") {
					tx <- consumer.Message{
						Data: encoded,
						Time: now,
						Key:  cmd.Name(),
					}
				}
			}
		}

		if viper.GetBool(cmd.Name() + ".oneshot") {
			fn()
			return
		}

		fn()
	loop:
		for {
			select {
			case <-ticker.C:
				fn()
			case <-chTerminate:
				break loop
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(providentiaCmd)

	providentiaCmd.PersistentFlags().String("url", "", "API endpoint")
	viper.BindPFlag(providentiaCmd.Name()+".url", providentiaCmd.PersistentFlags().Lookup("url"))

	providentiaCmd.PersistentFlags().String("token", "", "API token")
	viper.BindPFlag(providentiaCmd.Name()+".token", providentiaCmd.PersistentFlags().Lookup("token"))

	providentiaCmd.PersistentFlags().Duration("interval", 5*time.Minute, "Sleep between API calls")
	viper.BindPFlag(providentiaCmd.Name()+".interval", providentiaCmd.PersistentFlags().Lookup("interval"))

	providentiaCmd.PersistentFlags().Bool("oneshot", false, "Only run once and exit.")
	viper.BindPFlag(providentiaCmd.Name()+".oneshot", providentiaCmd.PersistentFlags().Lookup("oneshot"))

	app.RegisterOutputKafka(providentiaCmd.Name(), providentiaCmd.PersistentFlags())
}
