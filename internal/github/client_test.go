package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"ghfetcher/internal/config"
	"ghfetcher/internal/store"
	"ghfetcher/internal/tokenpool"
)

func TestHybridGraphQLRESTETagAndSecondaryBackoff(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()

	st, err := store.Open(filepath.Join(tmp, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	var gqlHits atomic.Int32
	var restHits atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		n := gqlHits.Add(1)
		// First call triggers simulated secondary rate limit on token #1 -> client should rotate to token #2!
		if n == 1 {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"message":"You have exceeded a secondary rate limit"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{
			"data": {
				"rateLimit": {"limit": 5000, "remaining": 4990, "cost": 1, "resetAt": %q},
				"r0": {
					"databaseId": 101,
					"owner": {"login": "facebook"},
					"name": "react",
					"isFork": false,
					"stargazerCount": 220000,
					"forkCount": 45000,
					"diskUsage": 380000,
					"defaultBranchRef": {
						"name": "main",
						"target": {"oid": "abc1234", "history": {"totalCount": 19500}}
					},
					"issues": {"totalCount": 1200},
					"pullRequests": {"totalCount": 2400}
				}
			}
		}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	})

	mux.HandleFunc("/repos/facebook/react/pulls/comments", func(w http.ResponseWriter, r *http.Request) {
		restHits.Add(1)
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4999")
		if r.Header.Get("If-None-Match") == `"etag-v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"etag-v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{
			"id": 9001,
			"pull_request_review_id": 501,
			"pull_request_url": "https://api.github.com/repos/facebook/react/pulls/42",
			"path": "packages/react/index.js",
			"commit_id": "deadbeef",
			"original_commit_id": "deadbeef",
			"diff_hunk": "@@ -1,3 +1,4 @@",
			"line": 12,
			"side": "RIGHT",
			"body": "Nit: check nullability",
			"created_at": "2026-01-01T10:00:00Z",
			"updated_at": "2026-01-01T10:05:00Z",
			"user": {"id": 7, "login": "dan", "type": "User"},
			"reactions": {"+1": 3, "total_count": 3}
		}]`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	pool := tokenpool.New([]config.TokenEntry{
		{Label: "tok-a", Token: "ghp_aaaaaaaaaaaa1111", Source: "manual"},
		{Label: "tok-b", Token: "ghp_bbbbbbbbbbbb2222", Source: "manual"},
	}, tokenpool.Options{SafetyReserve: 100})

	client := NewClient(pool, st)
	client.SetEndpoints(srv.URL+"/graphql", srv.URL)

	// 1. Verify BatchEstimateRepos rotates past secondary-rate-limited token #1 to token #2 seamlessly
	ests, cost, err := client.BatchEstimateRepos(ctx, [][2]string{{"facebook", "react"}})
	if err != nil {
		t.Fatalf("BatchEstimateRepos failed: %v", err)
	}
	if cost != 1 || len(ests) != 1 || ests[0].CommitsCount != 19500 {
		t.Fatalf("unexpected estimate: cost=%d ests=%+v", cost, ests)
	}

	repoID, err := st.UpsertRepository(ctx, &store.Repository{Owner: "facebook", Name: "react"})
	if err != nil {
		t.Fatal(err)
	}

	// 2. Verify REST review comments sweep + ETag caching
	fetched, err := client.SweepRepoReviewCommentsREST(ctx, st, repoID, "facebook", "react", nil)
	if err != nil {
		t.Fatalf("SweepRepoReviewCommentsREST failed: %v", err)
	}
	if fetched != 1 {
		t.Fatalf("expected 1 review comment, got %d", fetched)
	}

	// Second call to exact same URL with ETag must return 304 Not Modified (0 quota!)
	var dummy []any
	meta, err := client.DoRESTGet(ctx, "/repos/facebook/react/pulls/comments?direction=asc&per_page=100&sort=updated", true, &dummy)
	if err != nil {
		t.Fatalf("second DoRESTGet failed: %v", err)
	}
	if !meta.NotModified || client.Metrics().ETagHits304 != 1 {
		t.Fatalf("expected ETag 304 cache hit, got %+v metrics=%+v", meta, client.Metrics())
	}
}
