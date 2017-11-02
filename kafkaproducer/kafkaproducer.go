package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	brokerList string
	brokers    []string
	topic      string
	partition  int64
)

func produce(brokers []string, topic string, partition int32) error {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.Return.Successes = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return fmt.Errorf("error while creating new kafka client: %s", err)
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return fmt.Errorf("error while creating new sync producer: %s", err)
	}

	for {
		msg := sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Key:       sarama.StringEncoder("key"),
			Value:     sarama.StringEncoder("value"),
		}

		_, offset, err := producer.SendMessage(&msg)
		if err != nil {
			return fmt.Errorf("error while sending message: %s", err)
		}

		log.Printf("sent message to partition %d with offset %d", partition, offset)
	}
}

func main() {
	flag.StringVar(&brokerList, "brokers", "", "comma separated list of brokers")
	flag.StringVar(&topic, "topic", "", "topic name")
	flag.Int64Var(&partition, "partition", 0, "partition")
	flag.Parse()

	if brokerList == "" {
		log.Fatalf("list of brokers empty")
	}

	brokers = strings.Split(brokerList, ",")

	if topic == "" {
		log.Fatalf("topic name empty")
	}

	if err := produce(brokers, topic, int32(partition)); err != nil {
		log.Fatalf("%s", err)
	}
}
