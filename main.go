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

// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#account]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]
// 2024/09/08 17:28:43 rawMap: map[op:1 t:#commit]

type Header struct {
	Type      string `cbor:"t"`
	Operation uint8  `cbor:"op"`
}

type SubscribeReposCommitOperationAction string

const (
	Create SubscribeReposCommitOperationAction = "create"
	Update SubscribeReposCommitOperationAction = "update"
	Delete SubscribeReposCommitOperationAction = "delete"
)

type SubscribeReposCommitOperation struct {
	Path   string                              `cbor:"path"`
	Action SubscribeReposCommitOperationAction `cbor:"action"`
	CID    *cid.Cid                            `cbor:"cid,omitempty"`
}

type SubscribeReposCommit struct {
	Commit     cid.Cid                         `cbor:"commit"`
	Operations []SubscribeReposCommitOperation `cbor:"ops"`
	Prev       *cid.Cid                        `cbor:"prev,omitempty"`
	Rebase     bool                            `cbor:"rebase"`
	Repo       string                          `cbor:"repo"`
	Sequence   int64                           `cbor:"seq"`
	Time       time.Time                       `cbor:"time"`
	TooBig     bool                            `cbor:"tooBig"`
}

type SubscribeReposHandle struct {
	DID      string    `cbor:"did"`
	Handle   string    `cbor:"handle"`
	Sequence int64     `cbor:"seq"`
	Time     time.Time `cbor:"time"`
}

type SubscribeReposTombstone struct {
	DID      string    `cbor:"did"`
	Sequence int64     `cbor:"seq"`
	Time     time.Time `cbor:"time"`
}

type SubscribeReposAccount struct {
	Sequence int64 `cbor:"seq"`
}

type SubscribeReposIdentity struct {
	Sequence int64 `cbor:"seq"`
}

type SubscribeRepos struct {
	Header Header
	Body   interface{}
}

// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor={}"
// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=1537553850"

func main() {
	conn, err := connect("wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?curso=1537553850", nil)
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

	events := make(chan SubscribeRepos)

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
		log.Printf("event: %+v", event)
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

func parse(msg []byte) (SubscribeRepos, error) {
	var header Header
	decoder := cbor.NewDecoder(bytes.NewReader(msg))
	if err := decoder.Decode(&header); err != nil {
		return SubscribeRepos{}, err
	}

	return SubscribeRepos{
		Header: header,
		Body:   nil,
	}, nil
}
