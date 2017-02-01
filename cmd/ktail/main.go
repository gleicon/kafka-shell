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
	fmt.Println("Usage: ktail [-s ip:port] [-n <offset] [-f] <topic>")
	fmt.Println("KTail  consumes and print messages from a kafka topic.")
	fmt.Println("-s broker(s) separated by comma. Defaults to localhost:9092")
	fmt.Println("-n offset. Defaults to newest. -1 sends to newest message, 0 to oldest, any other number is taken as offset")
	fmt.Println("-f that tail-ish thing that keeps waiting for new lines etc")
	fmt.Println("-d debug info")
	os.Exit(1)
}

func main() {
	brokers := flag.String("s", "localhost:9092", "broker")
	offset := flag.Int64("n", -1, "log offset")
	foption := flag.Bool("f", false, "Wait for new messages")
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

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	brokerList := strings.Split(*brokers, ",")

	if debug {
		log.Printf("ktail brokers: %s offset: %d wait for new lines: %b for topic: %s\n", *brokers, *offset, *foption, topic)
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
