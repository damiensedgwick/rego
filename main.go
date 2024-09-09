package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/data"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/bluesky-social/indigo/repo"
	"github.com/gorilla/websocket"
)

type TextPost struct {
	Created string   `json:"createdAt"`
	Langs   []string `json:"langs"`
	Text    string   `json:"text"`
}

func main() {
	ctx := context.Background()

	uri := "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=1573867440"
	con, _, err := websocket.DefaultDialer.Dial(uri, http.Header{})
	if err != nil {
		log.Fatalf("error connecting to firehose: %v", err)
	}

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *atproto.SyncSubscribeRepos_Commit) error {
			for _, op := range evt.Ops {
				if strings.Contains(op.Path, "app.bsky.feed.post") {
					rr, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(evt.Blocks))
					if err != nil {
						fmt.Println("error reading repo from car")
						return nil
					}

					_, recB, err := rr.GetRecordBytes(ctx, op.Path)
					if err != nil {
						fmt.Println("error getting record bytes")
						break
					}

					rec, err := data.UnmarshalCBOR(*recB)
					if err != nil {
						return fmt.Errorf("failed to unmarshal record: %w", err)
					}

					recJSON, err := json.Marshal(rec)
					if err != nil {
						fmt.Println("error marshalling record to json")
						break
					}

					var post TextPost
					err = json.Unmarshal(recJSON, &post)
					if err != nil {
						fmt.Println("error unmarshalling record json")
						break
					}

					for _, lang := range post.Langs {
						if lang == "en" && post.Text != "" {
							fmt.Println(post.Text)
							break
						}
					}
				}

			}

			return nil
		},
	}

	sched := sequential.NewScheduler("myfirehose", rsc.EventHandler)
	events.HandleRepoStream(context.Background(), con, sched)
}
