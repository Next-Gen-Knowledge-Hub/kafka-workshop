// Command syncproducer sends page-view events one at a time and blocks until each
// is acknowledged by the broker, checking the error on every single write.
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
	topic  = "page-views"
)

func main() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		// Async defaults to false: WriteMessages blocks until the broker acks.
	}
	defer writer.Close()

	users := []string{"alice", "bob", "carol"}
	pages := []string{"/home", "/pricing", "/docs", "/checkout"}

	start := time.Now()
	sent := 0
	for i := 0; i < 20; i++ {
		user := users[i%len(users)]
		page := pages[i%len(pages)]

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(user),
			Value: []byte(fmt.Sprintf(`{"user":%q,"page":%q,"ts":%d}`, user, page, time.Now().UnixMilli())),
		})
		cancel()

		if err != nil {
			// In a synchronous producer, this is exactly where you'd decide to retry,
			// send to a dead-letter topic, or fail the request that triggered the send.
			log.Fatalf("write %d failed: %v", i, err)
		}
		sent++
		log.Printf("acked: user=%s page=%s", user, page)
	}

	log.Printf("sent %d messages synchronously in %s", sent, time.Since(start))
}
