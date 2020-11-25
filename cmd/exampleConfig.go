/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

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
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// exampleConfigCmd represents the exampleConfig command
var exampleConfigCmd = &cobra.Command{
	Use:   "exampleConfig",
	Short: "Dump example config to --config",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Infof("Writing config to %s", cfgFile)
		viper.WriteConfigAs(cfgFile)
	},
}

func init() {
	rootCmd.AddCommand(exampleConfigCmd)
}
