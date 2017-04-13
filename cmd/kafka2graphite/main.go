package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
)

var debug bool
var cConsumerCloser chan (int)
var wg sync.WaitGroup

func help() {
	fmt.Println("Usage: kafka2graphite [-s ip:port] [-n <offset] <-t topic> <-g graphite_host:port> -m [metric]")
	fmt.Println("kafka2graphite consumes a kafka topic and push an attribute to a graphite metric.")
	fmt.Println("-s broker(s) separated by comma. Defaults to localhost:9092")
	fmt.Println("-n offset. Defaults to newest. -1 sends to newest message, 0 to oldest, any other number is taken as offset")
	fmt.Println("-t kafka topic")
	fmt.Println("-g graphite host:port/topic")
	fmt.Println("-m metric name - defaults to topic name counter")
	fmt.Println("-d debug info")
	os.Exit(1)
}

func main() {
	brokers := flag.String("s", "localhost:9092", "broker")
	offset := flag.Int64("n", -1, "log offset")
	topic := flag.String("t", "", "topic")
	graphiteAddr := flag.String("g", "localhost:2003", "graphite address")
	metricName := flag.String("m", "", "metric name")

	dd := flag.Bool("d", false, "Debug")
	flag.Usage = help
	flag.Parse()

	args := flag.Args()
	debug = *dd

	if topic == "" {
		fmt.Println("No topic given")
		os.Exit(-1)
	}

	if *metricName == "" {
		*metricName = *topic
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	brokerList := strings.Split(*brokers, ",")

	if debug {
		log.Printf("ktail brokers: %s offset: %d topic: %s\n", *brokers, *offset, topic)
	}
	cConsumerCloser = make(chan int)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		log.Println("Stopping consumer")
		close(cConsumerCloser)
	}()

	startConsumer(brokerList, *offset, *foption, topic)

	wg.Wait()

}
