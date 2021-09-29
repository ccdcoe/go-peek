package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// exampleConfigCmd represents the exampleConfig command
var exampleConfigCmd = &cobra.Command{
	Use:   "exampleConfig",
	Short: "Dump example config to --config",
	Run: func(cmd *cobra.Command, args []string) {
		logger.Infof("Writing config to %s", cfgFile)
		viper.WriteConfigAs(cfgFile)
	},
}

func init() {
	rootCmd.AddCommand(exampleConfigCmd)
}
