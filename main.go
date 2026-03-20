package main

import (
	"fmt"
	"os"
	"github.com/sh4shv4t/GOfka/broker" // my defined package
)

func main() {
	
	newTopic := broker.Topic{
		TopicName:   "test-topic",
		LogFilePath: "data/test.log",
	}

	offset, err := newTopic.Push([]byte("First Message"))
	if err != nil {
		panic(err)
	}

	msg, err := newTopic.Pull(offset)
	if err != nil {
		panic(err)
	}

	fmt.Println("Message printed and received - ", string(msg))

	offset2, err := newTopic.Push([]byte("Second Message"))
	if err != nil {
		panic(err)
	}

	msg2, err := newTopic.Pull(offset2)
	if err != nil {
		panic(err)
	}

	fmt.Println("Message printed and received - ", string(msg2))

	err = os.Truncate("data/test.log", 0)
	if err != nil {
		panic(err)
	}

	fmt.Println("Test successful, log file truncated")
}
