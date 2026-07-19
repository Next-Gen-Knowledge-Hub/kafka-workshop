// Command producer sends five greeting messages to the "greetings" topic and exits.
// Run the cluster from 1-setup (docker compose up -d) before running this.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	broker = "localhost:9092"
	topic  = "greetings"
)

func main() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true, // fine for a workshop; explicit creation is covered in stage 4
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		msg := kafka.Message{
			Key:   []byte(fmt.Sprintf("greeting-%d", i)),
			Value: []byte(fmt.Sprintf("hello kafka #%d", i)),
			Time:  time.Now(),
		}
		if err := writer.WriteMessages(ctx, msg); err != nil {
			log.Fatalf("write message %d: %v", i, err)
		}
		log.Printf("produced: key=%s value=%s", msg.Key, msg.Value)
	}

	log.Println("done — run the consumer next: go run ./consumer")
}
