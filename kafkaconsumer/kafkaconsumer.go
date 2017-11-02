package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	brokerList     string
	brokers        []string
	topic          string
	partition      int64
	startTimestamp int64
)

func consume(brokers []string, topic string, partition int32, startTimestamp int64) error {
	var err error

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return fmt.Errorf("error while creating new kafka client: %s", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return fmt.Errorf("error while creating new consumer: %s", err)
	}

	offset := sarama.OffsetNewest

	if startTimestamp > 0 {
		offset, err = client.GetOffset(topic, partition, startTimestamp)
		if err != nil {
			return fmt.Errorf("could not get offset for timestamp %d: %s", startTimestamp, err)
		}

		log.Printf("offset for timestamp %d is %d", startTimestamp, offset)
	}

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return fmt.Errorf("error while creating new partition consumer: %s", err)
	}

	for {
		select {
		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				return fmt.Errorf("partition consumer closed")
			}
			log.Printf("received msg offset %d, timestamp %v (%v)", msg.Offset, msg.Timestamp, msg.Timestamp.Unix())
		}
	}
}

func main() {
	flag.StringVar(&brokerList, "brokers", "", "comma separated list of brokers")
	flag.StringVar(&topic, "topic", "", "topic name")
	flag.Int64Var(&startTimestamp, "start_timestamp", 0, "start timestamp")
	flag.Int64Var(&partition, "partition", 0, "partition")
	flag.Parse()

	if brokerList == "" {
		log.Fatalf("list of brokers empty")
	}

	brokers = strings.Split(brokerList, ",")

	if topic == "" {
		log.Fatalf("topic name empty")
	}

	if err := consume(brokers, topic, int32(partition), startTimestamp); err != nil {
		log.Fatalf("%s", err)
	}
}
