// Command inventoryservice consumes the "orders" topic as a consumer group,
// checks (and decrements) in-memory stock per SKU, and publishes the outcome
// to orders-reserved or orders-failed. It only commits an order's offset AFTER
// successfully publishing the outcome — the at-least-once pattern from
// 2-producers-and-consumers/go/manualcommit, applied to a real decision instead
// of a simulated one.
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Next-Gen-Knowledge-Hub/kafka-workshop/8-capstone-order-pipeline/go/internal/events"
)

const broker = "localhost:9092"

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          events.TopicOrders,
		GroupID:        "inventory-service",
		MinBytes:       1,
		MaxBytes:       1 << 20,
		CommitInterval: 0, // manual commits only
	})
	defer reader.Close()

	reserved := &kafka.Writer{
		Addr: kafka.TCP(broker), Topic: events.TopicOrdersReserved,
		Balancer: &kafka.Hash{}, AllowAutoTopicCreation: true, BatchTimeout: 50 * time.Millisecond,
	}
	defer reserved.Close()

	failed := &kafka.Writer{
		Addr: kafka.TCP(broker), Topic: events.TopicOrdersFailed,
		Balancer: &kafka.Hash{}, AllowAutoTopicCreation: true, BatchTimeout: 50 * time.Millisecond,
	}
	defer failed.Close()

	var mu sync.Mutex
	stock := map[string]int{
		"widget": 100,
		"gadget": 5, // deliberately low, to demonstrate the "failed" path
		"gizmo":  50,
	}

	log.Println("inventory-service consuming orders — Ctrl+C to stop")
	for {
		ctx := context.Background()
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			return
		}

		var order events.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("skipping unparseable order at offset %d: %v", msg.Offset, err)
			reader.CommitMessages(ctx, msg)
			continue
		}

		result := reserve(&mu, stock, order)

		out := reserved
		if result.Status == "failed" {
			out = failed
		}
		v, _ := json.Marshal(result)
		if err := out.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: v}); err != nil {
			log.Printf("failed to publish reservation result for order %s, NOT committing: %v", order.OrderID, err)
			continue // retry this order next poll; offset intentionally not committed
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed for order %s: %v", order.OrderID, err)
			continue
		}

		log.Printf("order %s: %s (sku=%s qty=%d)", order.OrderID, result.Status, order.SKU, order.Quantity)
	}
}

func reserve(mu *sync.Mutex, stock map[string]int, order events.Order) events.ReservationResult {
	mu.Lock()
	defer mu.Unlock()

	result := events.ReservationResult{
		OrderID: order.OrderID, CustomerID: order.CustomerID,
		SKU: order.SKU, Quantity: order.Quantity, Amount: order.Amount,
	}

	available, known := stock[order.SKU]
	switch {
	case !known:
		result.Status = "failed"
		result.Reason = "unknown SKU"
	case available < order.Quantity:
		result.Status = "failed"
		result.Reason = "insufficient stock"
	default:
		stock[order.SKU] -= order.Quantity
		result.Status = "reserved"
	}
	return result
}
