package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func startProducer(brokers []string, config *sarama.Config, stdout bool, topic string) {
	producer, err := sarama.NewSyncProducer(brokers, config)

	if err != nil {
		log.Println(err)
		os.Exit(-1)
	}

	//	defer func() {
	//		if err := producer.Close(); err != nil {
	//			log.Println(err)
	//			os.Exit(-1)
	//		}
	//	}()

	//	signals := make(chan os.Signal, 1)
	//	signal.Notify(signals, os.Interrupt)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if stdout {
			fmt.Fprintf(os.Stdout, scanner.Text())
		}
		wg.Add(1)
		go func() {
			strTime := strconv.Itoa(int(time.Now().Unix()))
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(strTime),
				Value: sarama.ByteEncoder(scanner.Bytes()),
			}
			_, _, err := producer.SendMessage(msg)

			if err != nil {
				log.Println(err)
			}
			wg.Done()
		}()
	}
}
