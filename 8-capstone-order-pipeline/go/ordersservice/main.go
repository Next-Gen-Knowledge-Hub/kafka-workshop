// Command ordersservice is the pipeline's entry point: a small HTTP API that
// takes an order request and produces it to the "orders" topic. Everything
// downstream (inventory-service, notification-service) reacts to that
// production asynchronously — the HTTP handler never talks to the other
// services directly, only to Kafka.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/Next-Gen-Knowledge-Hub/kafka-workshop/8-capstone-order-pipeline/go/internal/events"
)

const (
	broker = "localhost:9092"
	addr   = ":8090"
)

type orderRequest struct {
	CustomerID string  `json:"customerId"`
	SKU        string  `json:"sku"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"amount"`
}

func main() {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  events.TopicOrders,
		Balancer:               &kafka.Hash{}, // partition by order key so per-order history stays ordered
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: true,
		BatchTimeout:           50 * time.Millisecond,
	}
	defer writer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		handleCreateOrder(w, r, writer)
	})

	log.Printf("orders-service listening on %s (POST /orders)", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func handleCreateOrder(w http.ResponseWriter, r *http.Request, writer *kafka.Writer) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req orderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if req.CustomerID == "" || req.SKU == "" || req.Quantity <= 0 {
		http.Error(w, "customerId, sku, and a positive quantity are required", http.StatusBadRequest)
		return
	}

	order := events.Order{
		OrderID:    uuid.NewString(),
		CustomerID: req.CustomerID,
		SKU:        req.SKU,
		Quantity:   req.Quantity,
		Amount:     req.Amount,
	}
	value, err := json.Marshal(order)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(order.OrderID), Value: value}); err != nil {
		log.Printf("failed to produce order %s: %v", order.OrderID, err)
		http.Error(w, "failed to accept order", http.StatusServiceUnavailable)
		return
	}

	log.Printf("accepted order %s: customer=%s sku=%s qty=%d", order.OrderID, order.CustomerID, order.SKU, order.Quantity)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(order)
}
