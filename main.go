package main

import (
	"bytes"
	"log"
	"net/http"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
)

type Header struct {
	Type      string `cbor:"t"`
	Operation uint8  `cbor:"op"`
}

type SubscribeReposCommit struct {
	Blobs      []byte                          `cbor:"blobs"`
	Blocks     []byte                          `cbor:"blocks"`
	Commit     []byte                          `cbor:"commit"`
	Operations []SubscribeReposCommitOperation `cbor:"ops"`
	Prev       *cid.Cid                        `cbor:"prev,omitempty"`
	Rebase     bool                            `cbor:"rebase"`
	Repo       string                          `cbor:"repo"`
	Rev        string                          `cbor:"rev"`
	Sequence   int64                           `cbor:"seq"`
	Since      string                          `cbor:"since"`
	Time       time.Time                       `cbor:"time"`
	TooBig     bool                            `cbor:"tooBig"`
}

type SubscribeReposCommitOperationAction string

type SubscribeReposCommitOperation struct {
	Path   string                              `cbor:"path"`
	Action SubscribeReposCommitOperationAction `cbor:"action"`
	CID    []byte                              `cbor:"cid,omitempty"`
}

const (
	Create SubscribeReposCommitOperationAction = "create"
	Update SubscribeReposCommitOperationAction = "update"
	Delete SubscribeReposCommitOperationAction = "delete"
)

// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor={}"
// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=1537553850"

func main() {
	conn, err := connect("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?curso=1543524352", nil)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()
	log.Println("successfully connected to bluesky firehose")

	messages := make(chan []byte)

	go func() {
		defer close(messages)
		for {
			msg, err := read(conn)
			if err != nil {
				log.Fatalf("failed to read message: %v", err)
			}
			messages <- msg
		}
	}()

	events := make(chan SubscribeReposCommit)

	go func() {
		defer close(events)
		for msg := range messages {
			event, err := parse(msg)
			if err != nil {
				log.Fatalf("failed to parse message: %v", err)
			}
			events <- event
		}
	}()

	for event := range events {
		log.Printf("event: %v", event)
	}
}

func connect(url string, header http.Header) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func read(conn *websocket.Conn) ([]byte, error) {
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func parse(msg []byte) (SubscribeReposCommit, error) {
	var header Header
	decoder := cbor.NewDecoder(bytes.NewReader(msg))
	if err := decoder.Decode(&header); err != nil {
		return SubscribeReposCommit{}, err
	}

	if header.Type != "#commit" {
		return SubscribeReposCommit{}, nil
	}

	var commit SubscribeReposCommit
	if err := decoder.Decode(&commit); err != nil {
		return SubscribeReposCommit{}, err
	}

	return commit, nil
}
