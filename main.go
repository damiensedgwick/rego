package main

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
)

type SubscribeReposCommitOperation struct {
	Path   string `json:"path" cbor:"path"`
	Action string `json:"action" cbor:"action"`
	Cid    string `json:"cid,omitempty" cbor:"cid,omitempty"` // Optional CID
}

type SubscribeReposCommit struct {
	Commit     string                          `json:"commit" cbor:"commit"`
	Operations []SubscribeReposCommitOperation `json:"ops" cbor:"ops"`
	Prev       string                          `json:"prev,omitempty" cbor:"prev,omitempty"`
	Rebase     bool                            `json:"rebase" cbor:"rebase"`
	Repo       string                          `json:"repo" cbor:"repo"`
	Sequence   int64                           `json:"seq" cbor:"seq"`
	Time       time.Time                       `json:"time" cbor:"time"`
	TooBig     bool                            `json:"tooBig" cbor:"tooBig"`
}

type SubscribeRepos struct {
	Commit *SubscribeReposCommit `json:"commit,omitempty" cbor:"commit,omitempty"`
}

type MessageCounter struct {
	count int
	mu    sync.Mutex
}

func (mc *MessageCounter) Increment() {
	mc.mu.Lock()
	mc.count++
	mc.mu.Unlock()
}

func (mc *MessageCounter) GetCount() int {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.count
}

func (mc *MessageCounter) Reset() {
	mc.mu.Lock()
	mc.count = 0
	mc.mu.Unlock()
}

func processMessage(message []byte, counter *MessageCounter) error {
	var data SubscribeRepos
	dec := cbor.NewDecoder(bytes.NewReader(message)) // Decode CBOR-encoded data
	err := dec.Decode(&data)
	if err != nil {
		return fmt.Errorf("error decoding message: %v", err)
	}

	if data.Commit != nil {
		commit := data.Commit
		var relevantOps []SubscribeReposCommitOperation
		for _, op := range commit.Operations {
			if strings.HasPrefix(op.Path, "fyi.unravel.frontpage.") {
				relevantOps = append(relevantOps, op)
			}
		}

		if len(relevantOps) > 0 {
			log.Printf("Received commit %s with %d relevant operations", commit.Commit, len(relevantOps))
			for _, op := range relevantOps {
				log.Printf("  %s: %s", op.Action, op.Path)
			}
		} else {
			log.Printf("Received commit %s with no relevant operations", commit.Commit)
		}
	}

	counter.Increment()
	return nil
}

func listenFirehose(counter *MessageCounter) error {
	url := fmt.Sprintf("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", 1515930042)
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return fmt.Errorf("error connecting to WebSocket: %v", err)
	}
	defer c.Close()

	for {
		_, message, err := c.ReadMessage()
		if err != nil {
			return fmt.Errorf("error reading message: %v", err)
		}

		err = processMessage(message, counter)
		if err != nil {
			log.Printf("Error processing message: %v", err)
		}
	}
}

func logAverageMessages(counter *MessageCounter) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		processed := counter.GetCount()
		log.Printf("Processed %.2f messages per second over the last 5 seconds", float64(processed)/5.0)
		counter.Reset()
	}
}

func main() {
	counter := &MessageCounter{}
	go logAverageMessages(counter)

	for {
		err := listenFirehose(counter)
		if err != nil {
			log.Printf("Error with WebSocket connection, retrying: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}
