package main

import (
	"bytes"
	"log"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
)

type Event struct {
	Did       string                                  `json:"did" cborgen:"did"`
	TimeUS    int64                                   `json:"time_us" cborgen:"time_us"`
	EventType string                                  `json:"type" cborgen:"type"`
	Commit    *Commit                                 `json:"commit,omitempty" cborgen:"commit,omitempty"`
	Account   *comatproto.SyncSubscribeRepos_Account  `json:"account,omitempty" cborgen:"account,omitempty"`
	Identity  *comatproto.SyncSubscribeRepos_Identity `json:"identity,omitempty" cborgen:"identity,omitempty"`
}

type Commit struct {
	Rev        string          `json:"rev,omitempty" cborgen:"rev"`
	OpType     string          `json:"type" cborgen:"type"`
	Collection string          `json:"collection,omitempty" cborgen:"collection"`
	RKey       string          `json:"rkey,omitempty" cborgen:"rkey"`
	Record     json.RawMessage `json:"record,omitempty" cborgen:"record,omitempty"`
}

var (
	EventCommit   = "com"
	EventAccount  = "acc"
	EventIdentity = "id"

	CommitCreateRecord = "c"
	CommitUpdateRecord = "u"
	CommitDeleteRecord = "d"
)

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

	events := make(chan Event)

	go func() {
		defer close(events)
		for msg := range messages {
			err := parse(msg, events)
			if err != nil {
				log.Fatalf("failed to parse message: %v", err)
			}
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

func parse(msg []byte, ec chan Event) error {
	var event Event

	decoder := cbor.NewDecoder(bytes.NewReader(msg))
	if err := decoder.Decode(&event); err != nil {
		return err
	}

	ec <- event

	return nil
}
