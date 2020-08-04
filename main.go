package main

import (
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

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	return master
}

func consume(topics []string, master sarama.Consumer) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)

	for _, t := range topics {
		if strings.Contains(t, "__consumer_offsets") {
			continue
		}

		partitions, _ := master.Partitions(t)

		for k, _ := range partitions {

			c, err := master.ConsumePartition(t, partitions[k], sarama.OffsetOldest)
			if err != nil {
				fmt.Printf("Topic: %d Partition: %d", t, partitions[k])
				fmt.Println(err)
			}
			fmt.Println("consuming messages...")

			go func(t string, c sarama.PartitionConsumer) {
				for {
					select {
					case consumerError := <-c.Errors():
						errors <- consumerError
						fmt.Println("consume error: ", err)
					case msg := <-c.Messages():
						consumers <- msg
						processMsg(msg)
					}
				}
			}(t, c)

		}
	}

	return consumers, errors
}

func processMsg(msg *sarama.ConsumerMessage) {
	fmt.Printf("[%d]: Key: %s\n", msg.Offset, msg.Key)
	fmt.Printf("%+v\n", msg.Value)
	fmt.Printf("-----\n")
}
