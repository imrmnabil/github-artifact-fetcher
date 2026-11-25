package tokenpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"ghfetcher/internal/config"
)

// BucketKind distinguishes GitHub's two independent primary quota pools per token.
type BucketKind string

const (
	BucketGraphQL BucketKind = "graphql"
	BucketREST    BucketKind = "rest"

	DefaultLimit = 5000
)

var ErrNoTokensConfigured = errors.New("no GitHub tokens configured; run `ghfetcher config add-token` or set GHFETCHER_TOKENS")

// BucketState tracks live rate-limit state for one pool (REST or GraphQL) of a token.
type BucketState struct {
	Limit              int
	Remaining          int
	ResetAt            time.Time
	AbuseCooldownUntil time.Time
	InFlight           int
	MaxInFlight        int
	TotalRequests      int64
	TotalPoints        int64
}

// EffectiveRemaining returns quota remaining accounting for window reset and in-flight leases.
func (b *BucketState) EffectiveRemaining(now time.Time) int {
	rem := b.Remaining
	if !b.ResetAt.IsZero() && !now.Before(b.ResetAt) {
		rem = b.Limit
		if rem <= 0 {
			rem = DefaultLimit
		}
	}
	eff := rem - b.InFlight
	if eff < 0 {
		return 0
	}
	return eff
}

// NextAvailableAt returns zero Time if usable right now, or the earliest wakeup timestamp.
func (b *BucketState) NextAvailableAt(now time.Time, reserve int, cost int) (usable bool, wakeAt time.Time, waitingInFlight bool) {
	if cost <= 0 {
		cost = 1
	}
	if !b.AbuseCooldownUntil.IsZero() && now.Before(b.AbuseCooldownUntil) {
		return false, b.AbuseCooldownUntil, false
	}
	eff := b.EffectiveRemaining(now)
	if eff-cost < reserve {
		if b.InFlight > 0 && (b.Remaining-reserve >= cost) {
			// Quota exists once in-flight requests release/settle
			return false, time.Time{}, true
		}
		if b.ResetAt.IsZero() || !now.Before(b.ResetAt) {
			return false, now.Add(5 * time.Second), false
		}
		return false, b.ResetAt, false
	}
	if b.MaxInFlight > 0 && b.InFlight >= b.MaxInFlight {
		return false, time.Time{}, true
	}
	return true, time.Time{}, false
}

// TokenTelemetry exposes read-only token status for the htop TUI and ETA engine.
type TokenTelemetry struct {
	ID         int        `json:"id"`
	Label      string     `json:"label"`
	Masked     string     `json:"masked"`
	Source     string     `json:"source"`
	GraphQL    BucketSnap `json:"graphql"`
	REST       BucketSnap `json:"rest"`
	Reserve    int        `json:"reserve"`
}

// BucketSnap is a point-in-time snapshot of a single bucket.
type BucketSnap struct {
	Limit              int       `json:"limit"`
	Remaining          int       `json:"remaining"`
	EffectiveRemaining int       `json:"effective_remaining"`
	ResetAt            time.Time `json:"reset_at"`
	CooldownUntil      time.Time `json:"cooldown_until"`
	InFlight           int       `json:"in_flight"`
	MaxInFlight        int       `json:"max_in_flight"`
	TotalRequests      int64     `json:"total_requests"`
	TotalPoints        int64     `json:"total_points"`
	Sleeping           bool      `json:"sleeping"`
}

type managedToken struct {
	id     int
	label  string
	masked string
	raw    string
	source string
	gql    BucketState
	rest   BucketState
}

// Lease represents an acquired right to make 1 API request on a specific token bucket.
type Lease struct {
	pool       *Pool
	tokenIndex int
	kind       BucketKind
	Token      string
	Label      string
	Masked     string
	released   bool
}

// Complete reports observed rate-limit headers/cost and releases the in-flight slot.
func (l *Lease) Complete(observedLimit, observedRemaining int, observedResetAt time.Time, cost int) {
	if l == nil || l.pool == nil {
		return
	}
	l.pool.completeLease(l, observedLimit, observedRemaining, observedResetAt, cost, 0)
}

// CompleteWithCooldown marks a secondary rate limit / Retry-After backoff on this token bucket.
func (l *Lease) CompleteWithCooldown(cooldown time.Duration) {
	if l == nil || l.pool == nil {
		return
	}
	if cooldown <= 0 {
		cooldown = 60 * time.Second
	}
	l.pool.completeLease(l, -1, -1, time.Time{}, 0, cooldown)
}

// Options configures Pool behavior.
type Options struct {
	SafetyReserve   int
	MaxGQLInFlight  int
	MaxRESTInFlight int
	NowFunc         func() time.Time
}

// Pool coordinates multi-token, dual-bucket (GraphQL + REST) load balancing and cooldowns.
type Pool struct {
	mu              sync.Mutex
	notifyCh        chan struct{}
	tokens          []*managedToken
	safetyReserve   int
	maxGQLInFlight  int
	maxRESTInFlight int
	nowFn           func() time.Time
	paused          bool
}

// New creates a multi-token Pool with independent REST and GraphQL rate-limit buckets.
func New(entries []config.TokenEntry, opts Options) *Pool {
	if opts.SafetyReserve < 0 {
		opts.SafetyReserve = 100
	}
	if opts.MaxGQLInFlight <= 0 {
		opts.MaxGQLInFlight = 3
	}
	if opts.MaxRESTInFlight <= 0 {
		opts.MaxRESTInFlight = 8
	}
	if opts.NowFunc == nil {
		opts.NowFunc = time.Now
	}
	p := &Pool{
		notifyCh:        make(chan struct{}),
		safetyReserve:   opts.SafetyReserve,
		maxGQLInFlight:  opts.MaxGQLInFlight,
		maxRESTInFlight: opts.MaxRESTInFlight,
		nowFn:           opts.NowFunc,
	}
	p.Reload(entries)
	return p
}

func (p *Pool) broadcastLocked() {
	close(p.notifyCh)
	p.notifyCh = make(chan struct{})
}

// SetPaused pauses or resumes new lease acquisitions across all workers.
func (p *Pool) SetPaused(paused bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused != paused {
		p.paused = paused
		p.broadcastLocked()
	}
}

// IsPaused reports whether the pool is currently paused by user hotkey/command.
func (p *Pool) IsPaused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// Reload hot-reloads tokens while preserving live rate-limit state for existing tokens.
func (p *Pool) Reload(entries []config.TokenEntry) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.nowFn()
	existing := make(map[string]*managedToken, len(p.tokens))
	for _, t := range p.tokens {
		existing[t.raw] = t
	}

	next := make([]*managedToken, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for i, e := range entries {
		if e.Token == "" {
			continue
		}
		if _, dup := seen[e.Token]; dup {
			continue
		}
		seen[e.Token] = struct{}{}

		if prev, ok := existing[e.Token]; ok {
			prev.id = i + 1
			if e.Label != "" {
				prev.label = e.Label
			}
			if e.Source != "" {
				prev.source = e.Source
			}
			next = append(next, prev)
			continue
		}

		next = append(next, &managedToken{
			id:     i + 1,
			label:  e.Label,
			masked: config.MaskToken(e.Token),
			raw:    e.Token,
			source: e.Source,
			gql: BucketState{
				Limit:       DefaultLimit,
				Remaining:   DefaultLimit,
				ResetAt:     now.Add(time.Hour),
				MaxInFlight: p.maxGQLInFlight,
			},
			rest: BucketState{
				Limit:       DefaultLimit,
				Remaining:   DefaultLimit,
				ResetAt:     now.Add(time.Hour),
				MaxInFlight: p.maxRESTInFlight,
			},
		})
	}
	p.tokens = next
	p.broadcastLocked()
	return len(p.tokens)
}

// TokenCount returns how many tokens are currently loaded.
func (p *Pool) TokenCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.tokens)
}

func getBucket(t *managedToken, kind BucketKind) *BucketState {
	if kind == BucketGraphQL {
		return &t.gql
	}
	return &t.rest
}

// Acquire blocks until a token with sufficient quota above SafetyReserve is available
// (sleeping automatically through rate-limit cooldowns) or ctx is canceled.
func (p *Pool) Acquire(ctx context.Context, kind BucketKind, estimatedCost int) (*Lease, error) {
	if estimatedCost <= 0 {
		estimatedCost = 1
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		p.mu.Lock()
		if len(p.tokens) == 0 {
			p.mu.Unlock()
			return nil, ErrNoTokensConfigured
		}

		if p.paused {
			waitCh := p.notifyCh
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-waitCh:
				continue
			}
		}

		now := p.nowFn()
		bestIdx := -1
		bestScore := -1
		bestReset := time.Time{}

		var earliestWake time.Time
		anyWaitingInFlight := false

		for idx, tok := range p.tokens {
			b := getBucket(tok, kind)
			// Refresh expired window automatically before scoring
			if !b.ResetAt.IsZero() && !now.Before(b.ResetAt) && b.InFlight == 0 {
				if b.Limit <= 0 {
					b.Limit = DefaultLimit
				}
				b.Remaining = b.Limit
				b.ResetAt = now.Add(time.Hour)
			}

			usable, wakeAt, waitingInFlight := b.NextAvailableAt(now, p.safetyReserve, estimatedCost)
			if usable {
				avail := b.EffectiveRemaining(now) - p.safetyReserve
				// Prefer token whose window resets SOONEST when plenty of quota exists,
				// or highest headroom if close, maximizing cluster-wide hourly yield.
				better := false
				if bestIdx == -1 {
					better = true
				} else if !b.ResetAt.IsZero() && !bestReset.IsZero() && avail >= 250 && bestScore >= 250 {
					if b.ResetAt.Before(bestReset) {
						better = true
					} else if b.ResetAt.Equal(bestReset) && avail > bestScore {
						better = true
					}
				} else if avail > bestScore {
					better = true
				}
				if better {
					bestIdx = idx
					bestScore = avail
					bestReset = b.ResetAt
				}
				continue
			}

			if waitingInFlight {
				anyWaitingInFlight = true
			}
			if !wakeAt.IsZero() && (earliestWake.IsZero() || wakeAt.Before(earliestWake)) {
				earliestWake = wakeAt
			}
		}

		if bestIdx >= 0 {
			tok := p.tokens[bestIdx]
			b := getBucket(tok, kind)
			b.InFlight++
			lease := &Lease{
				pool:       p,
				tokenIndex: bestIdx,
				kind:       kind,
				Token:      tok.raw,
				Label:      tok.label,
				Masked:     tok.masked,
			}
			p.mu.Unlock()
			return lease, nil
		}

		waitCh := p.notifyCh
		p.mu.Unlock()

		// Either wait on in-flight slot release or sleep until earliest token reset/cooldown
		if anyWaitingInFlight && earliestWake.IsZero() {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-waitCh:
			}
			continue
		}

		sleepDur := 500 * time.Millisecond
		if !earliestWake.IsZero() {
			if d := earliestWake.Sub(p.nowFn()); d > 0 {
				sleepDur = d + 50*time.Millisecond
			} else {
				sleepDur = 25 * time.Millisecond
			}
		}

		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-waitCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (p *Pool) completeLease(l *Lease, observedLimit, observedRemaining int, observedResetAt time.Time, cost int, cooldown time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if l.released {
		return
	}
	l.released = true

	var target *managedToken
	if l.tokenIndex >= 0 && l.tokenIndex < len(p.tokens) && p.tokens[l.tokenIndex].raw == l.Token {
		target = p.tokens[l.tokenIndex]
	} else {
		for _, t := range p.tokens {
			if t.raw == l.Token {
				target = t
				break
			}
		}
	}
	if target == nil {
		p.broadcastLocked()
		return
	}

	now := p.nowFn()
	b := getBucket(target, l.kind)
	if b.InFlight > 0 {
		b.InFlight--
	}
	b.TotalRequests++
	if cost > 0 {
		b.TotalPoints += int64(cost)
	}

	if cooldown > 0 {
		until := now.Add(cooldown)
		if until.After(b.AbuseCooldownUntil) {
			b.AbuseCooldownUntil = until
		}
	}

	if observedLimit > 0 {
		b.Limit = observedLimit
	}
	if observedRemaining >= 0 {
		b.Remaining = observedRemaining
	} else if cost > 0 && b.Remaining >= cost {
		b.Remaining -= cost
	}
	if !observedResetAt.IsZero() {
		b.ResetAt = observedResetAt
	}

	p.broadcastLocked()
}

// Snapshot returns current telemetry for all tokens and buckets.
func (p *Pool) Snapshot() []TokenTelemetry {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.nowFn()
	out := make([]TokenTelemetry, 0, len(p.tokens))
	for _, t := range p.tokens {
		gqlUsable, _, gqlInFlightWait := t.gql.NextAvailableAt(now, p.safetyReserve, 1)
		restUsable, _, restInFlightWait := t.rest.NextAvailableAt(now, p.safetyReserve, 1)

		out = append(out, TokenTelemetry{
			ID:      t.id,
			Label:   t.label,
			Masked:  t.masked,
			Source:  t.source,
			Reserve: p.safetyReserve,
			GraphQL: BucketSnap{
				Limit:              t.gql.Limit,
				Remaining:          t.gql.Remaining,
				EffectiveRemaining: t.gql.EffectiveRemaining(now),
				ResetAt:            t.gql.ResetAt,
				CooldownUntil:      t.gql.AbuseCooldownUntil,
				InFlight:           t.gql.InFlight,
				MaxInFlight:        t.gql.MaxInFlight,
				TotalRequests:      t.gql.TotalRequests,
				TotalPoints:        t.gql.TotalPoints,
				Sleeping:           !gqlUsable && !gqlInFlightWait,
			},
			REST: BucketSnap{
				Limit:              t.rest.Limit,
				Remaining:          t.rest.Remaining,
				EffectiveRemaining: t.rest.EffectiveRemaining(now),
				ResetAt:            t.rest.ResetAt,
				CooldownUntil:      t.rest.AbuseCooldownUntil,
				InFlight:           t.rest.InFlight,
				MaxInFlight:        t.rest.MaxInFlight,
				TotalRequests:      t.rest.TotalRequests,
				TotalPoints:        t.rest.TotalPoints,
				Sleeping:           !restUsable && !restInFlightWait,
			},
		})
	}
	return out
}

// PooledAvailable returns total immediately usable quota across all tokens for a bucket kind,
// plus the earliest reset timestamp if any token is sleeping.
func (p *Pool) PooledAvailable(kind BucketKind) (usableTotal int, hourlyCapacity int, earliestReset time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.nowFn()
	for _, t := range p.tokens {
		b := getBucket(t, kind)
		capPerHr := b.Limit - p.safetyReserve
		if capPerHr < 0 {
			capPerHr = 0
		}
		hourlyCapacity += capPerHr

		if avail := b.EffectiveRemaining(now) - p.safetyReserve; avail > 0 {
			usableTotal += avail
		}
		if !b.ResetAt.IsZero() && (earliestReset.IsZero() || b.ResetAt.Before(earliestReset)) {
			earliestReset = b.ResetAt
		}
	}
	return usableTotal, hourlyCapacity, earliestReset
}
