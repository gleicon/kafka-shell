package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

var debug bool
var cConsumerCloser chan (int)
var wg sync.WaitGroup

func help() {
	fmt.Println("Usage: ktail [-s ip:port] [-n <offset] [-f] <topic>")
	fmt.Println("KTee sends stdin to kafka and to stdout (can be disabled)")
	fmt.Println("-s broker(s) separated by comma. Defaults to localhost:9092")
	fmt.Println("-r max retries to send data")
	fmt.Println("-o prints stdin to stdout before sending to kafka")
	fmt.Println("-d debug info")
	os.Exit(1)
}

func main() {
	brokers := flag.String("s", "localhost:9092", "broker")
	stdout := flag.Bool("o", false, "prints to stdout")
	retries := flag.Int("r", 5, "Max retries")
	dd := flag.Bool("d", false, "Debug")
	flag.Usage = help
	flag.Parse()

	args := flag.Args()
	debug = *dd

	if len(args) < 1 {
		fmt.Println("No topic given")
		os.Exit(-1)
	}

	topic := args[0]

	if topic == "" {
		fmt.Println("No topic given")
		os.Exit(-1)
	}

	config := sarama.NewConfig()
	config.Producer.Retry.Max = *retries
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	//	signals := make(chan os.Signal, 1)
	//	signal.Notify(signals, os.Interrupt)
	brokerList := strings.Split(*brokers, ",")

	startProducer(brokerList, config, *stdout, topic)
	wg.Wait()
}
