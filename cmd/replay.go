package cmd

import (
	"fmt"
	"time"

	"github.com/ccdcoe/go-peek/internal/entrypoints/replay"
	events "github.com/ccdcoe/go-peek/pkg/models/events"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const argTsFormat = "2006-01-02 15:04:05"

// replayCmd represents the replay command
var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "Replay events from filesystem store",
	Long: `Replay mode will parse all timestamps from stored logs and will store diffs in a linked list. ` +
		`Logs from specified range will then be read from files containing while sleeping between each message. For example:` +
		`
`,
	Run: replay.Entrypoint,
}

func init() {
	// We need to know first and last timestamps but host clocks may be wrong
	// Configure parser to always prefer the syslog timestamp
	events.AlwaysUseSyslogTimestamp = true

	replay.IgnoreParseErrors = true
	replay.TimeStampFormat = argTsFormat

	// Register subcommand
	rootCmd.AddCommand(replayCmd)

	// Set up subcommand specific config options
	replayCmd.Flags().Bool(
		"cache", false, `Cache parsed timestamp diffs and store in workdir if cache file is missong. `+
			`Otherwise load cache and bypass parsing all the timestamps. `+
			`Specified timestam range with --time-from and --time-to will be ignored if cache is lodaded.`)
	viper.BindPFlag("cache", replayCmd.Flags().Lookup("cache"))

	replayCmd.Flags().String(
		"time-from", time.Now().Add(-24*time.Hour).Format(argTsFormat), `Start replay from this time.`+
			fmt.Sprintf("Format is %s.", argTsFormat)+
			`Defaults to now - 24h.`)
	viper.BindPFlag("time.from", replayCmd.Flags().Lookup("time-from"))
	replayCmd.Flags().String(
		"time-to", time.Now().Format(argTsFormat), `Stop replay at this time.`+
			fmt.Sprintf("Format is %s.", argTsFormat)+
			`Defaults to now.`)
	viper.BindPFlag("time.to", replayCmd.Flags().Lookup("time-to"))

	replayCmd.Flags().Bool(
		"play-outputs-enable", false, "Play messages to configured outputs."+
			" Disabled by default in replay mode, as replay is mostly needed for local development."+
			" Might not be a good idea to spam production databases if someone only wants to pipe logs to another cli tool.")
	viper.BindPFlag("play.outputs.enable", replayCmd.Flags().Lookup("play-outputs-enable"))

}
