package main

import (
	"bufio"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func startProducer(brokers []string, config *sarama.Config, stdout bool, topic string) {
	var enqueued, errors int
	producer, err := sarama.NewAsyncProducer(brokers, config)

	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Println(err)
			os.Exit(-1)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	// wait for stdin and go

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		go func() {
			strTime := strconv.Itoa(int(time.Now().Unix()))
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(strTime),
				Value: sarama.ByteEncoder(scanner.Bytes()),
			}

			select {
			case producer.Input() <- msg:
				enqueued++
			case err := <-producer.Errors():
				errors++
				log.Println("Failed to produce message:", err)
			case <-signals:
				log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
				return
			}
		}()
	}
}
