package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

const (
	CLIENTID = "micro-kafka-consumer"
	BROKERS = "127.0.01:29092"
	TOPIC = "in-example"
)

func main() {



	c := initConsumer(2)

	consume(TOPIC, c)




}

func initConsumer (o int64) sarama.PartitionConsumer {
	var startOffset int64

	config := sarama.NewConfig()
	config.ClientID = CLIENTID
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient([]string{BROKERS}, config)
	if err != nil {
		fmt.Println("error create client: ", err)
	}

	master, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println("error create consumer: ", err)
	}
	defer func() {
		if err := master.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	partitions, err := master.Partitions(TOPIC)

	offset, err := client.GetOffset(TOPIC, partitions[3], -1)
	if err != nil {
		fmt.Println("error get offset: ", err)
	}

	if o < 0 {
		startOffset = offset + o
	} else {
		startOffset = sarama.OffsetNewest
	}

	fmt.Println("Avaliable partitions: ", partitions)
	fmt.Println("Offset: ", offset, "Starting from: ", startOffset)

	consumer, err := master.ConsumePartition(TOPIC, partitions[3],startOffset)
	if err != nil {
		fmt.Printf("Topic %v Partitions: %v", TOPIC, partitions)
		fmt.Println("error consume messages")
	}

	return consumer
}

func consume(t string, c sarama.PartitionConsumer) {
	fmt.Println("Start consuming topic: ", TOPIC)

	go func(t string, c sarama.PartitionConsumer) {
		for {
			select {
			case consumerError := <- c.Errors():
				fmt.Println("consumer error: ", consumerError)

			case msg := <- c.Messages():
				processMsg(msg)
			}
		}
	}(t, c)
}

func processMsg(msg *sarama.ConsumerMessage) {
	fmt.Printf("[%d]: Key: %s\n", msg.Offset, msg.Key)
	fmt.Printf("%+v\n", msg.Value)
	fmt.Printf("-----\n")
}