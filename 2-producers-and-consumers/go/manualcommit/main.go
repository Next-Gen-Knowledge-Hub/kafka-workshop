// Command manualcommit demonstrates at-least-once processing: the offset for a
// message is only committed *after* its (simulated) side effect has succeeded.
// If you kill the process mid-way and restart it, it will reprocess the last
// uncommitted message rather than silently skip it.
package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"

	"github.com/segmentio/kafka-go"
)

const (
	broker  = "localhost:9092"
	topic   = "page-views"
	groupID = "manual-commit-consumers"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: groupID,
		// CommitInterval defaults to 0 for kafka.Reader, which already means "commit
		// only when CommitMessages is called explicitly" -- set here for clarity.
		CommitInterval: 0,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Println("consuming with manual commits — Ctrl+C to stop")
	for {
		msg, err := reader.FetchMessage(ctx) // fetches WITHOUT advancing the committed offset
		if err != nil {
			log.Println("stopping:", err)
			return
		}

		if err := simulateSideEffect(msg); err != nil {
			// Do NOT commit: on restart, this exact message will be redelivered.
			log.Printf("processing failed, offset %d NOT committed: %v", msg.Offset, err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed for offset %d: %v", msg.Offset, err)
			continue
		}
		log.Printf("processed and committed: partition=%d offset=%d key=%s", msg.Partition, msg.Offset, msg.Key)
	}
}

// simulateSideEffect stands in for "write to a database", "call a payment API", etc.
// It fails ~10% of the time to make the retry-without-commit path visible in the logs.
func simulateSideEffect(msg kafka.Message) error {
	if rand.Intn(10) == 0 {
		return errors.New("simulated downstream failure")
	}
	return nil
}
