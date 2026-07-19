// Package events defines the message payloads shared by every service in the
// capstone pipeline. Every topic in this pipeline carries exactly one of these
// types, JSON-encoded — the closest thing this workshop has to a schema
// contract between independently deployable services (see PRODUCERS.md's note
// on schemas; a real system would likely use Avro/Protobuf + a schema registry
// here instead).
package events

const (
	TopicOrders         = "orders"
	TopicOrdersReserved = "orders-reserved"
	TopicOrdersFailed   = "orders-failed"
	TopicOrdersDLQ      = "orders-dlq"
)

// Order is produced by orders-service and consumed by inventory-service.
type Order struct {
	OrderID    string  `json:"orderId"`
	CustomerID string  `json:"customerId"`
	SKU        string  `json:"sku"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"amount"`
}

// ReservationResult is produced by inventory-service, to either
// orders-reserved or orders-failed depending on Status, and consumed by
// notification-service.
type ReservationResult struct {
	OrderID    string  `json:"orderId"`
	CustomerID string  `json:"customerId"`
	SKU        string  `json:"sku"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"` // "reserved" or "failed"
	Reason     string  `json:"reason,omitempty"`
}

// DeadLetter is produced by notification-service after exhausting retries on
// a message it couldn't process — the payload it failed on, plus why.
type DeadLetter struct {
	OriginalTopic string `json:"originalTopic"`
	Payload       string `json:"payload"`
	Error         string `json:"error"`
	Attempts      int    `json:"attempts"`
}
