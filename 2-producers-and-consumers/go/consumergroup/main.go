// Command consumergroup joins the "page-views-consumers" group reading the
// "page-views" topic. Run several copies of this program at once (in separate
// terminals) to watch Kafka split the topic's partitions across them, and watch it
// rebalance when you stop one.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/segmentio/kafka-go"
)

const (
	broker  = "localhost:9092"
	topic   = "page-views"
	groupID = "page-views-consumers"
)

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: groupID, // same GroupID across instances = one shared consumer group
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pid := os.Getpid()
	log.Printf("consumer pid=%d joining group %q — Ctrl+C to leave and trigger a rebalance", pid, groupID)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("pid=%d stopping: %v", pid, err)
			return
		}
		log.Printf("pid=%d got partition=%d offset=%d key=%s", pid, msg.Partition, msg.Offset, msg.Key)
	}
}
