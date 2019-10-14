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
	"github.com/ccdcoe/go-peek/internal/entrypoints/syslog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// syslogCmd represents the syslog command
var syslogCmd = &cobra.Command{
	Use:   "syslog",
	Short: "Super simple syslog server",
	Long:  `Spawns a simple server for collecting unstructured UDP syslog messages in BSD format, and optionally normalizing them to format understandable by main run subcommand. Can be used to reduce reliance on external syslog daemons with extensive and complicated normalization configurations.`,
	Run:   syslog.Entrypoint,
}

func init() {
	rootCmd.AddCommand(syslogCmd)

	syslogCmd.PersistentFlags().Int("syslog-port", 10001, "Port to listen incoming syslog messages.")
	viper.BindPFlag("syslog.port", syslogCmd.PersistentFlags().Lookup("syslog-port"))

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// syslogCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// syslogCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
