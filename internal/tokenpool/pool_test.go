package tokenpool

import (
	"context"
	"testing"
	"time"

	"ghfetcher/internal/config"
)

func TestDualPoolIndependenceAndSafetyReserve(t *testing.T) {
	now := time.Date(2026, 11, 26, 1, 0, 0, 0, time.UTC)
	entries := []config.TokenEntry{
		{Label: "tok-1", Token: "ghp_token1111111111", Source: "manual"},
		{Label: "tok-2", Token: "ghp_token2222222222", Source: "manual"},
	}

	pool := New(entries, Options{
		SafetyReserve:   100,
		MaxGQLInFlight:  2,
		MaxRESTInFlight: 4,
		NowFunc:         func() time.Time { return now },
	})

	ctx := context.Background()

	// Drain tok-1 GraphQL down to safety reserve (100), while REST remains 5000
	l1, err := pool.Acquire(ctx, BucketGraphQL, 1)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	l1.Complete(5000, 100, now.Add(150*time.Millisecond), 1)

	// Next GraphQL request must automatically route to tok-2 because tok-1 is at SafetyReserve
	l2, err := pool.Acquire(ctx, BucketGraphQL, 1)
	if err != nil {
		t.Fatalf("Acquire 2 failed: %v", err)
	}
	if l2.Label != "tok-2" {
		t.Fatalf("expected routing to tok-2, got %s", l2.Label)
	}
	// Drain tok-2 GraphQL down to 100 as well
	l2.Complete(5000, 100, now.Add(120*time.Millisecond), 1)

	// Meanwhile tok-1 REST bucket is completely independent and still immediately available!
	restLease, err := pool.Acquire(ctx, BucketREST, 1)
	if err != nil {
		t.Fatalf("REST Acquire failed while GQL exhausted: %v", err)
	}
	restLease.Complete(5000, 4999, now.Add(time.Hour), 1)

	// Verify hot-reload wakes up a blocked GraphQL Acquire immediately
	acquiredCh := make(chan string, 1)
	go func() {
		l, aerr := pool.Acquire(ctx, BucketGraphQL, 1)
		if aerr == nil {
			acquiredCh <- l.Label
			l.Complete(5000, 4999, now.Add(time.Hour), 1)
		}
	}()

	time.Sleep(30 * time.Millisecond)
	pool.Reload(append(entries, config.TokenEntry{
		Label: "tok-3-hot", Token: "ghp_token3333333333", Source: "manual",
	}))

	select {
	case got := <-acquiredCh:
		if got != "tok-3-hot" {
			t.Fatalf("expected hot-reloaded tok-3-hot, got %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for hot-reload wakeup")
	}
}
