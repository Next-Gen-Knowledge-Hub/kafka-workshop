// Command consumer reads from the "greetings" topic from the beginning and prints
// every message it sees. Stop it with Ctrl+C.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:9092"
	topic  = "greetings"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "helloworld-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Println("consuming from", topic, "— Ctrl+C to stop")
	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Println("stopping:", err)
			return
		}
		log.Printf("consumed: partition=%d offset=%d key=%s value=%s",
			msg.Partition, msg.Offset, msg.Key, msg.Value)
	}
}
