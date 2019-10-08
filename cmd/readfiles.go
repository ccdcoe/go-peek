/*
Copyright © 2019 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/ccdcoe/go-peek/internal/helpers"
	"github.com/ccdcoe/go-peek/pkg/ingest/logfile"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// readfilesCmd represents the readfiles command
var readfilesCmd = &cobra.Command{
	Use:   "readfiles",
	Short: "Simply read log files from directory and print output to stdout",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		files := helpers.GetDirListingFromViper()
		reader, err := logfile.NewConsumer(&logfile.Config{
			Paths:          files.Files(),
			StatWorkers:    viper.GetInt("work.threads"),
			ConsumeWorkers: viper.GetInt("work.threads"),
			MapFunc:        files.MapFunc(),
		})
		if err != nil {
			errLogger(err, true)
		}
		for msg := range reader.Messages() {
			fmt.Fprintf(os.Stdout, "%+v\n", msg.Event.String())
		}
	},
}

func init() {
	rootCmd.AddCommand(readfilesCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// readfilesCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// readfilesCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
