// Package kafkatest contains an integration test that exercises a real Kafka
// broker: produce a known batch of messages, consume them back, and assert the
// round trip is exact. It skips (not fails) if no broker is reachable, so
// `go test ./...` from the repo root stays green on a machine with Docker not
// running — but does real, meaningful work wherever stage 1's cluster is up.
package kafkatest

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const broker = "localhost:9092"

func requireBroker(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
	if err != nil {
		t.Skipf("no broker reachable at %s — start it with `cd 1-setup && docker compose up -d`: %v", broker, err)
	}
	conn.Close()
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	requireBroker(t)

	topic := fmt.Sprintf("kafkatest-%s", uuid.NewString())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the topic explicitly and wait for it to be produce-ready, rather
	// than relying on auto-creation: the first write to a brand-new topic can
	// otherwise race the partition leader election and fail once with
	// "Leader Not Available" (see 4-cluster-administration/ADMIN.md).
	createTopic(t, topic)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		RequiredAcks: kafka.RequireAll,
	}
	t.Cleanup(func() { writer.Close() })

	const n = 20
	want := make(map[string]string, n)
	records := make([]kafka.Message, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		want[key] = value
		records[i] = kafka.Message{Key: []byte(key), Value: []byte(value)}
	}

	if err := writer.WriteMessages(ctx, records...); err != nil {
		t.Fatalf("produce: %v", err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  "kafkatest-" + uuid.NewString(), // fresh group: always starts at the beginning
		MinBytes: 1,
		MaxBytes: 1 << 20,
	})
	t.Cleanup(func() { reader.Close() })

	got := make(map[string]string, n)
	deadline := time.Now().Add(20 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.FetchMessage(fetchCtx)
		fetchCancel()
		if err != nil {
			continue // transient poll timeout while the group finishes joining
		}
		got[string(msg.Key)] = string(msg.Value)
		reader.CommitMessages(ctx, msg)
	}

	if len(got) != n {
		t.Fatalf("read back %d/%d messages", len(got), n)
	}
	for key, wantValue := range want {
		gotValue, ok := got[key]
		if !ok {
			t.Errorf("missing key %q", key)
			continue
		}
		if gotValue != wantValue {
			t.Errorf("key %q: got value %q, want %q", key, gotValue, wantValue)
		}
	}

	cleanupTopic(t, topic)
}

// createTopic creates topic explicitly via the cluster controller and blocks
// until the partitions are visible with an elected leader, so the produce call
// right after this returns doesn't race the leader election.
func createTopic(t *testing.T, topic string) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("controller lookup: %v", err)
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("create topic %q: %v", topic, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) > 0 {
			allLeaderless := false
			for _, p := range partitions {
				if p.Leader.ID == -1 {
					allLeaderless = true
				}
			}
			if !allLeaderless {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("topic %q never became produce-ready", topic)
}

// cleanupTopic best-effort deletes the throwaway topic this test created.
func cleanupTopic(t *testing.T, topic string) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Logf("cleanup: dial failed, leaving topic %q behind: %v", topic, err)
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Logf("cleanup: controller lookup failed, leaving topic %q behind: %v", topic, err)
		return
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port)))
	if err != nil {
		t.Logf("cleanup: dial controller failed, leaving topic %q behind: %v", topic, err)
		return
	}
	defer controllerConn.Close()

	if err := controllerConn.DeleteTopics(topic); err != nil {
		t.Logf("cleanup: delete topic %q failed: %v", topic, err)
	}
}
