package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
)

func startConsumer(brokers []string, offset int64, foption bool, topic string) {
	var partitionList []int32
	var initialOffset int64

	if debug {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		log.Println("kafka error: ", err)
		os.Exit(-1)
	}

	if partitionList, err = consumer.Partitions(topic); err != nil {
		log.Println("kafka error: ", err)
		os.Exit(-1)

	}

	initialOffset = offset
	if offset == 0 {
		initialOffset = sarama.OffsetOldest
	} else if offset == -1 {
		initialOffset = sarama.OffsetNewest

	}

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		// force consumer shutdown
		go func(pc sarama.PartitionConsumer) {
			wg.Add(1)
			<-cConsumerCloser
			pc.AsyncClose()
		}(pc)

		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for {
				if pc == nil {
					time.Sleep(30 * time.Second)
					continue
				}
				for message := range pc.Messages() {
					if message.Value != nil {
						ss := string(message.Value)
						fmt.Printf("%d: %s\n", message.Offset, ss)
						fmt.Println("-------------------")
					}
				}

				// unless -f is not set,
				// exit when messages are drained

				if !foption {
					break
				}

				time.Sleep(30 * time.Second)
			}
		}(pc)
	}

}
