// Command lagmonitor prints, for every consumer group on the cluster, the lag
// (log-end-offset minus committed offset) per topic — the same numbers
// `kafka-consumer-groups --describe` shows, computed via the Admin API instead
// of shelling out. Run it while stage 2's manualcommit or consumergroup example
// is (or was) running to see real numbers.
//
//	go run . [brokers, default localhost:9092]
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := []string{"localhost:9092"}
	if len(os.Args) > 1 {
		brokers = strings.Split(os.Args[1], ",")
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	groups, err := admin.ListGroups(ctx)
	if err != nil {
		log.Fatalf("list groups: %v", err)
	}
	if len(groups) == 0 {
		fmt.Println("no consumer groups found — run one of the stage 2 examples first")
		return
	}

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	lags, err := admin.Lag(ctx, names...)
	if err != nil {
		log.Fatalf("lag: %v", err)
	}

	for _, groupLag := range lags.Sorted() {
		fmt.Printf("\n=== group: %s (state=%s) ===\n", groupLag.Group, groupLag.State)
		if err := groupLag.Error(); err != nil {
			fmt.Printf("  error computing lag: %v\n", err)
			continue
		}
		for _, m := range groupLag.Lag.Sorted() {
			member := "-"
			if m.Member != nil {
				member = m.Member.ClientID
			}
			fmt.Printf("  %-25s partition=%-3d committed=%-8d end=%-8d lag=%-8d member=%s\n",
				m.Topic, m.Partition, m.Commit.At, m.End.Offset, m.Lag, member)
		}
		fmt.Printf("  total lag: %d\n", groupLag.Lag.Total())
	}
}
