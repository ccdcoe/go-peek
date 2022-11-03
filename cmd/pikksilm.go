package cmd

import (
	"context"
	"encoding/json"
	"go-peek/internal/app"
	kafkaIngest "go-peek/pkg/ingest/kafka"
	"time"

	"github.com/go-redis/redis"
	"github.com/markuskont/datamodels"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// pikksilmCmd represents the pikksilm command
var pikksilmCmd = &cobra.Command{
	Use:   "pikksilm",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		start := app.Start(cmd.Name(), logger)

		defer app.Catch(logger)
		defer app.Done(cmd.Name(), start, logger)

		ctxReader, cancelReader := context.WithCancel(context.Background())
		defer cancelReader()

		logger.Info("Creating kafka consumer for event stream")

		input, err := kafkaIngest.NewConsumer(&kafkaIngest.Config{
			Name:          cmd.Name() + " event stream",
			ConsumerGroup: viper.GetString(cmd.Name() + ".input.kafka.consumer_group"),
			Brokers:       viper.GetStringSlice(cmd.Name() + ".input.kafka.brokers"),
			Topics:        viper.GetStringSlice(cmd.Name() + ".input.kafka.topics"),
			Ctx:           ctxReader,
			OffsetMode:    kafkaOffset,
		})
		app.Throw(cmd.Name()+" event stream setup", err, logger)

		rdb := redis.NewClient(&redis.Options{
			Addr: viper.GetString(cmd.Name() + ".output.redis.host"),
			DB:   viper.GetInt(cmd.Name() + ".output.redis.db"), // use default DB
		})
		defer rdb.Close()
		redisKey := viper.GetString(cmd.Name() + ".output.redis.key")

		var count1 int
		var count3 int

		report := time.NewTicker(viper.GetDuration(cmd.Name() + ".log.interval"))
		defer report.Stop()

	loop:
		for {
			select {
			case <-report.C:
				logger.
					WithField("event_id_1", count1).
					WithField("event_id_3", count3).
					WithField("key", redisKey).
					Info("report")
			case msg, ok := <-input.Messages():
				if !ok {
					break loop
				}
				var obj datamodels.Map
				if err := json.Unmarshal(msg.Data, &obj); err != nil {
					logger.Error(err)
					continue loop
				}
				if c, ok := obj.GetString("winlog", "channel"); !ok || c != "Microsoft-Windows-Sysmon/Operational" {
					continue loop
				}
				id, ok := obj.GetString("winlog", "event_id")
				if !ok {
					continue loop
				}
				switch id {
				case "1":
					count1++
				case "3":
					count3++
				default:
					continue loop
				}
				rdb.LPush(redisKey, msg.Data)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(pikksilmCmd)

	pFlags := pikksilmCmd.PersistentFlags()

	pFlags.String("output-redis-host", "localhost:6379", "Redis host")
	viper.BindPFlag(pikksilmCmd.Name()+".output.redis.host", pFlags.Lookup("output-redis-host"))

	pFlags.Int("output-redis-db", 0, "Redis databse")
	viper.BindPFlag(pikksilmCmd.Name()+".output.redis.db", pFlags.Lookup("output-redis-db"))

	pFlags.String("output-redis-key", "pikksilm", "Redis key")
	viper.BindPFlag(pikksilmCmd.Name()+".output.redis.key", pFlags.Lookup("output-redis-key"))

	app.RegisterInputKafkaGenericSimple(pikksilmCmd.Name(), pikksilmCmd.PersistentFlags())
	app.RegisterLogging(pikksilmCmd.Name(), pikksilmCmd.PersistentFlags())
}
