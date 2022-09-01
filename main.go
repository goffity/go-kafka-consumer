package main

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func mapEnv() {
	for _, s := range os.Environ() {
		err := viper.BindEnv(strings.Split(s, "=")...)
		if err != nil {
			os.Exit(1)
		}
	}

	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		zap.S().Warnf("Not found config file: %s \n", err)
	}
}

func init() {
	var logger *zap.Logger
	logger, _ = zap.NewDevelopment()
	defer func(logger *zap.Logger) {
		err := logger.Sync()
		if err != nil {

		}
	}(logger)

	zap.ReplaceGlobals(logger)

	mapEnv()
}

func main() {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": viper.GetString("KAFKA_BOOTSTRAP_SERVERS"),
		"security.protocol": viper.GetString("KAFKA_SECURITY_PROTOCOL"),
		"sasl.mechanisms":   viper.GetString("KAFKA_SASL_MECHANISMS"),
		"sasl.username":     viper.GetString("KAFKA_SASL_USERNAME"),
		"sasl.password":     viper.GetString("KAFKA_SASL_PASSWORD"),
		"group.id":          "test",
		"auto.offset.reset": "earliest"})
	if err != nil {
		zap.S().Errorf("error: %s", err.Error())
	}
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()
		if err != nil {
			panic(err)
		}
	}(consumer)

	err = consumer.SubscribeTopics([]string{"test-topic"}, nil)

	signalChanel := make(chan os.Signal, 1)
	signal.Notify(signalChanel, syscall.SIGINT, syscall.SIGTERM)

	zap.S().Info("start consume.")
	run := true
	for run == true {
		select {
		case sig := <-signalChanel:
			zap.S().Infof("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			zap.S().Infof("Consumed record with key %s and value %s, and updated total count to %d\n", msg.Key, msg.Value, len(msg.Value))
		}
	}
}
