package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// NATS server address
	addr := "nats://127.0.0.1:4222"

	//Min and Max sleep time between publising messages
	min := 5
	max := 11

	// Stream and Subject names
	streamName := "FOO"
	subjectName := "foo.bar"

	// Create a wait group and service readiness to track ongoing operations
	var wg sync.WaitGroup
	var ready atomic.Bool
	ready.Store(true) // Initially, ready to process messages

	// Connect to NATS
	nc, err := nats.Connect(addr)
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	// create jetstream context from nats connection
	jsx, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	// ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	// Create a context with a cancelletion
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Get or Create a stream
	if _, err := jsx.StreamInfo(streamName); err != nil {
		// create a stream
		_, err = jsx.AddStream(&nats.StreamConfig{
			Name:              streamName,
			Subjects:          []string{subjectName}, // support for multiple subjects
			Retention:         nats.InterestPolicy,   // remove acked messages
			Discard:           nats.DiscardOld,       // when the stream is full, discard old messages
			MaxAge:            7 * 24 * time.Hour,    // max age of stored messages is 7 days
			Storage:           nats.FileStorage,      // type of message storage
			MaxMsgsPerSubject: 100_000_000,           // max stored messages per subject
			MaxMsgSize:        4 << 20,               // max single message size is 4 MB
			NoAck:             false,                 // we need the "ack" system for the message queue system
		}, nats.Context(ctx))
		if err != nil {
			log.Fatalf("add stream: %v", err)
		}
	}

	fmt.Printf("Publishing to Stream: %s, Sub: %s\n", streamName, subjectName)

	// Setup signal handling to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Signal handling loop in a goroutine
	go func() {
		<-sigChan // Wait for SIGINT or SIGTERM
		// Initiate graceful shutdown
		fmt.Println("\nGraceful shutdown initiated. Press Ctrl-C again for immediate shutdown...")

		// Prevent processing new messages
		ready.Store(false)

		// Cancel ongoing operations
		cancel()

		// Wait for ongoing operations to complete
		wg.Wait()

		// All ongoing operations completed
		os.Exit(0)

		// Wait for a second SIGINT or SIGTERM for immediate shutdown
		<-sigChan
		fmt.Println("immediate shutdown initiated...")
		os.Exit(1)
	}()

	// Endless Publish
	var i int
	for ready.Load() {
		// sleep for a random time between min and max
		st := time.Duration(rand.Intn(max-min+1)+min) * time.Second
		time.Sleep(st)

		if nc.Status() != nats.CONNECTED {
			fmt.Printf("not connected: %s\n", nc.Status())
			continue
		}

		wg.Add(1) // Indicate a new operation is starting
		payload := fmt.Sprintf("msg %d", i)
		ack, err := jsx.Publish(subjectName, []byte(payload))
		if err != nil {
			wg.Done() // Operation completed or failed, mark as done
			if ctx.Err() != context.Canceled {
				continue
			}
			fmt.Println("Publish error:", err)
		}
		fmt.Printf("(%d) Seq: %d, Payload %s\n", i, ack.Sequence, payload)

		wg.Done() // Operation completed, mark as done
		i++
	}
}
