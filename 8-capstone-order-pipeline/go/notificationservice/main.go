// Command notificationservice consumes both orders-reserved and orders-failed
// and simulates notifying the customer. "Sending a notification" fails
// transiently at random here, standing in for a flaky downstream API — the
// service retries a few times with backoff, and if it still can't get the
// notification out, publishes the original payload to orders-dlq (a dead
// letter queue) instead of either dropping it silently or retrying forever.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Next-Gen-Knowledge-Hub/kafka-workshop/8-capstone-order-pipeline/go/internal/events"
)

const (
	broker     = "localhost:9092"
	maxRetries = 3
)

func main() {
	dlq := &kafka.Writer{
		Addr: kafka.TCP(broker), Topic: events.TopicOrdersDLQ,
		Balancer: &kafka.Hash{}, AllowAutoTopicCreation: true, BatchTimeout: 50 * time.Millisecond,
	}
	defer dlq.Close()

	var wg sync.WaitGroup
	for _, topic := range []string{events.TopicOrdersReserved, events.TopicOrdersFailed} {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			consume(topic, dlq)
		}(topic)
	}

	log.Println("notification-service consuming orders-reserved and orders-failed — Ctrl+C to stop")
	wg.Wait()
}

func consume(topic string, dlq *kafka.Writer) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        "notification-service",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0,
	})
	defer reader.Close()

	for {
		ctx := context.Background()
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("[%s] fetch error: %v", topic, err)
			return
		}

		var result events.ReservationResult
		if err := json.Unmarshal(msg.Value, &result); err != nil {
			log.Printf("[%s] skipping unparseable message at offset %d: %v", topic, msg.Offset, err)
			reader.CommitMessages(ctx, msg)
			continue
		}

		if err := notifyWithRetry(topic, result); err != nil {
			log.Printf("[%s] order %s: giving up after %d attempts, sending to DLQ: %v", topic, result.OrderID, maxRetries, err)
			dead := events.DeadLetter{
				OriginalTopic: topic, Payload: string(msg.Value), Error: err.Error(), Attempts: maxRetries,
			}
			v, _ := json.Marshal(dead)
			if pubErr := dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: v}); pubErr != nil {
				log.Printf("[%s] ALSO failed to publish to DLQ, NOT committing: %v", topic, pubErr)
				continue // retry the whole thing next poll
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[%s] commit failed for order %s: %v", topic, result.OrderID, err)
		}
	}
}

// notifyWithRetry simulates calling a flaky notification API (SMS/email/push).
func notifyWithRetry(topic string, result events.ReservationResult) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := sendNotification(result); err != nil {
			lastErr = err
			log.Printf("[%s] order %s: notify attempt %d/%d failed: %v", topic, result.OrderID, attempt, maxRetries, err)
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond) // linear backoff
			continue
		}
		log.Printf("[%s] order %s (%s): customer %s notified", topic, result.OrderID, result.Status, result.CustomerID)
		return nil
	}
	return lastErr
}

func sendNotification(result events.ReservationResult) error {
	if rand.Intn(4) == 0 { // ~25% simulated transient failure rate
		return fmt.Errorf("notification provider timeout for customer %s", result.CustomerID)
	}
	return nil
}
