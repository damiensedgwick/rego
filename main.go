package main

import (
	"fmt"

	"github.com/goccy/go-json"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	apibsky "github.com/bluesky-social/indigo/api/bsky"
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

func main() {

}

// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos"
// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor={}"
// "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=1537553850"

func handleEvent(event *Event) error {
	if event.Commit != nil && (event.Commit.OpType == CommitCreateRecord || event.Commit.OpType == CommitUpdateRecord) {
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post apibsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				return fmt.Errorf("failed to unmarshal post: %w", err)
			}
		}
	}

	return nil
}
