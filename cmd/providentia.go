package cmd

import (
	"go-peek/internal/app"
	"go-peek/pkg/providentia"
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
		start := time.Now()
		logger.WithFields(logrus.Fields{}).Info("providentia called")

		defer logger.WithFields(logrus.Fields{"duration": time.Since(start)}).Info("All done!")
		defer app.Catch(logger)

		ticker := time.NewTicker(viper.GetDuration("providentia.interval"))
		defer ticker.Stop()

		fn := func() {
			if items, err := providentia.Pull(providentia.Params{
				URL:    viper.GetString("providentia.url"),
				Token:  viper.GetString("providentia.token"),
				Logger: logger,
			}); err != nil {
				logger.WithFields(logrus.Fields{}).Error(err)
			} else {
				logger.WithFields(logrus.Fields{
					"results": len(items),
					"url":     viper.GetString("providentia.url"),
				}).Debug("API call done")
			}
		}

		fn()
		for {
			select {
			case <-ticker.C:
				fn()
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(providentiaCmd)

	providentiaCmd.PersistentFlags().String("url", "", "API endpoint")
	viper.BindPFlag("providentia.url", providentiaCmd.PersistentFlags().Lookup("url"))

	providentiaCmd.PersistentFlags().String("token", "", "API token")
	viper.BindPFlag("providentia.token", providentiaCmd.PersistentFlags().Lookup("token"))

	providentiaCmd.PersistentFlags().Duration("interval", 5*time.Minute, "Sleep between API calls")
	viper.BindPFlag("providentia.interval", providentiaCmd.PersistentFlags().Lookup("interval"))
}
