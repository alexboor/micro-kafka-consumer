package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strings"
)

const (
	CLIENTID = "micro-kafka-consumer"
	BROKERS  = "127.0.0.1:29092"
	TOPIC    = "in-example"
)

func main() {

	master := initConsumer()

	consumer, errors := consume([]string{TOPIC}, master)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	doneChan := make(chan struct{})

	go func() {
		for {
			select {
			case msg := <-consumer:
				processMsg(msg)
			case consumerError := <-errors:
				fmt.Println("recieve consume error", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				doneChan <- struct{}{}
			case <-signals:
				fmt.Println("User interrupt detected")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan
	fmt.Println("Bye!")

}

// Init master client and return it
func initConsumer() sarama.Consumer {
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	config := sarama.NewConfig()
	config.ClientID = CLIENTID
	config.Consumer.Return.Errors = true

	brokers := []string{BROKERS}

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		fmt.Println("error create master consumer: ")
		panic(err)
	}

	return master
}

// Consume messages from given topic with given master client
// Return two channels for successful consumed messages and for errors
func consume(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)

	for _, t := range topics {
		if strings.Contains(t, "__consumer_offsets") {
			continue
		}

		partitions, err := master.Partitions(t)
		if err != nil {
			fmt.Println("error get partitions: ")
			panic(err)
		}

		for k, _ := range partitions {

			c, err := master.ConsumePartition(t, partitions[k], sarama.OffsetOldest)
			if err != nil {
				fmt.Printf("Topic: %d Partition: %d", t, partitions[k])
				panic(err)
			}

			go func(t string, c sarama.PartitionConsumer) {
				for {
					select {
					case consumerError := <-c.Errors():
						errors <- consumerError
					case msg := <-c.Messages():
						consumers <- msg
					}
				}
			}(t, c)

		}
	}

	return consumers, errors
}

// Human readable output of the given message
func processMsg(msg *sarama.ConsumerMessage) {
	var str interface{}
	if err := json.Unmarshal(msg.Value, &str); err != nil {
	}
	s, _ := json.MarshalIndent(str, "", "  ")

	fmt.Printf("Offset: %d Key: %s\n", msg.Offset, msg.Key)
	fmt.Printf("%s\n", s)
	fmt.Printf("-----\n")
}
