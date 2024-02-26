package main

// https://natsbyexample.com/examples/jetstream/pull-consumer/go

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	// NATS server address
	addr := "nats://127.0.0.1:4222"

	// Stream, Durable (consumer group) and Subject names
	streamName := "FOO"
	durableName := "FooBiDooBah"
	subjectName := "foo.bar"

	// Create a wait group and service readiness to track ongoing operations
	var wg sync.WaitGroup
	var ready atomic.Bool
	ready.Store(true) // Initially, ready to process messages

	// Connect to NATS
	nc, err := nats.Connect(addr)
	if err != nil {
		log.Fatalf("failed to establish NATS connection: %v", err)
	}
	defer nc.Close()

	// Get jetstream context
	jsx, err := nc.JetStream()
	if err != nil {
		log.Fatalf("failed to retrieve jetstream context: %v", err)
	}

	// Add a stream consumer
	_, err = jsx.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       durableName,            // durable name is the same as consumer group name
		DeliverPolicy: nats.DeliverAllPolicy,  // deliver all messages, even if they were sent before the consumer was created
		AckPolicy:     nats.AckExplicitPolicy, // ack messages manually
		AckWait:       5 * time.Second,        // wait for ack for 5 seconds
		MaxAckPending: -1,                     // unlimited number of pending acks
	})
	if err != nil {
		log.Fatalf("failed to add consumer: %v", err)
	}

	// Create a context with a cancelletion
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a durable pull subscription
	sub, err := jsx.PullSubscribe(
		subjectName,                        // Subject
		durableName,                        // for an ephemeral pull consumer, the "durable" value must be set to an empty string.
		nats.ManualAck(),                   // ack messages manually
		nats.Bind(streamName, durableName), // bind consumer to the stream
		nats.Context(ctx),                  // use context to cancel the subscription
	)
	if err != nil {
		log.Fatalf("failed to create pull subscription: %v", err)
	}

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
		fmt.Println("ongoing operations canceled...")

		// Wait for ongoing Fetch operations to complete
		wg.Wait()
		fmt.Println("all ongoing operations completed")

		// Now safe to delete the consumer
		if err := jsx.DeleteConsumer(streamName, durableName); err != nil {
			log.Fatalf("failed to delete consumer: %v", err)
		}

		os.Exit(0)

		// Wait for a second SIGINT or SIGTERM for immediate shutdown
		<-sigChan
		fmt.Println("immediate shutdown initiated...")
		os.Exit(1)
	}()

	// Continuous message processing loop
	for ready.Load() {
		wg.Add(1) // Indicate a new operation is starting

		// Fetch messages with a timeout
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second)) // Adjust timeout as needed
		if err != nil {
			wg.Done() // Operation completed or failed, mark as done
			// Timeout reached, no messages, loop will continue
			if err == nats.ErrTimeout {
				continue
			}
			// Context canceled, initiate graceful shutdown
			if ctx.Err() == context.Canceled {
				continue
			}
			// Handle other errors (e.g., connection issues)
			fmt.Printf("failed to fetch message: %v\n", err)
			continue
		}

		// Process fetched message
		for _, msg := range msgs {
			fmt.Printf("Stream: %s, Sub: %s, Msg: %s\n", streamName, msg.Subject, string(msg.Data))
			// Acknowledge the message
			msg.Ack()
		}
		wg.Done() // Operation completed, mark as done
	}

}
