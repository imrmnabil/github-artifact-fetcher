package github

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"ghfetcher/internal/store"
	"ghfetcher/internal/tokenpool"
	"ghfetcher/internal/version"
)

const (
	defaultGraphQLURL = "https://api.github.com/graphql"
	defaultRESTBase   = "https://api.github.com"
)

// Client executes rate-limit-aware GraphQL and REST requests across a multi-token Pool.
type Client struct {
	httpClient *http.Client
	pool       *tokenpool.Pool
	store      *store.Store
	gqlURL     string
	restBase   string

	gqlRequests  atomic.Int64
	gqlPoints    atomic.Int64
	restRequests atomic.Int64
	etagHits304  atomic.Int64
	retries      atomic.Int64
}

// NewClient creates a dual-pool GitHub API client.
func NewClient(pool *tokenpool.Pool, st *store.Store) *Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          64,
		MaxIdleConnsPerHost:   24,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   75 * time.Second,
		},
		pool:     pool,
		store:    st,
		gqlURL:   defaultGraphQLURL,
		restBase: defaultRESTBase,
	}
}

// SetEndpoints overrides API base URLs (used in tests or GitHub Enterprise).
func (c *Client) SetEndpoints(gqlURL, restBase string) {
	if gqlURL != "" {
		c.gqlURL = gqlURL
	}
	if restBase != "" {
		c.restBase = strings.TrimRight(restBase, "/")
	}
}

// Metrics returns live request and cache counters.
type Metrics struct {
	GQLRequests  int64 `json:"gql_requests"`
	GQLPoints    int64 `json:"gql_points"`
	RESTRequests int64 `json:"rest_requests"`
	ETagHits304  int64 `json:"etag_hits_304"`
	Retries      int64 `json:"retries"`
}

func (c *Client) Metrics() Metrics {
	return Metrics{
		GQLRequests:  c.gqlRequests.Load(),
		GQLPoints:    c.gqlPoints.Load(),
		RESTRequests: c.restRequests.Load(),
		ETagHits304:  c.etagHits304.Load(),
		Retries:      c.retries.Load(),
	}
}

type gqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type gqlRateLimit struct {
	Limit     int       `json:"limit"`
	Remaining int       `json:"remaining"`
	Cost      int       `json:"cost"`
	ResetAt   time.Time `json:"resetAt"`
}

type gqlError struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type gqlEnvelope struct {
	Data   json.RawMessage `json:"data"`
	Errors []gqlError      `json:"errors"`
}

// DoGraphQL executes a GraphQL query via the GraphQL token bucket with automatic retry & secondary backoff.
func (c *Client) DoGraphQL(ctx context.Context, query string, vars map[string]any, out any) (cost int, err error) {
	payload, err := json.Marshal(gqlRequest{Query: query, Variables: vars})
	if err != nil {
		return 0, err
	}

	const maxAttempts = 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		lease, err := c.pool.Acquire(ctx, tokenpool.BucketGraphQL, 1)
		if err != nil {
			return 0, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.gqlURL, bytes.NewReader(payload))
		if err != nil {
			lease.Complete(-1, -1, time.Time{}, 0)
			return 0, err
		}
		req.Header.Set("Authorization", "Bearer "+lease.Token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", "ghfetcher/"+version.Version+" (MSR-Research)")

		resp, doErr := c.httpClient.Do(req)
		if doErr != nil {
			lease.Complete(-1, -1, time.Time{}, 0)
			if attempt == maxAttempts || ctx.Err() != nil {
				return 0, fmt.Errorf("graphql transport error: %w", doErr)
			}
			c.retries.Add(1)
			sleepContext(ctx, time.Duration(attempt)*time.Second)
			continue
		}

		bodyBytes, readErr := readBodyMaybeGzip(resp)
		_ = resp.Body.Close()
		hdrLimit, hdrRem, hdrReset := parseRateHeaders(resp.Header)

		if isSecondaryRateLimit(resp.StatusCode, resp.Header, bodyBytes) {
			wait := parseRetryAfter(resp.Header, 60*time.Second)
			lease.CompleteWithCooldown(wait)
			c.retries.Add(1)
			continue
		}

		if resp.StatusCode >= 500 {
			lease.Complete(hdrLimit, hdrRem, hdrReset, 0)
			if attempt == maxAttempts {
				return 0, fmt.Errorf("graphql server error HTTP %d: %s", resp.StatusCode, truncate(string(bodyBytes), 240))
			}
			c.retries.Add(1)
			sleepContext(ctx, time.Duration(attempt*2)*time.Second)
			continue
		}

		if readErr != nil {
			lease.Complete(hdrLimit, hdrRem, hdrReset, 1)
			return 0, fmt.Errorf("read graphql body: %w", readErr)
		}

		if resp.StatusCode != http.StatusOK {
			lease.Complete(hdrLimit, hdrRem, hdrReset, 1)
			return 0, fmt.Errorf("graphql HTTP %d: %s", resp.StatusCode, truncate(string(bodyBytes), 280))
		}

		var env gqlEnvelope
		if err := json.Unmarshal(bodyBytes, &env); err != nil {
			lease.Complete(hdrLimit, hdrRem, hdrReset, 1)
			return 0, fmt.Errorf("decode graphql envelope: %w", err)
		}

		// Extract embedded rateLimit node if present
		var rl struct {
			RateLimit *gqlRateLimit `json:"rateLimit"`
		}
		if len(env.Data) > 0 {
			_ = json.Unmarshal(env.Data, &rl)
		}

		actualCost := 1
		if rl.RateLimit != nil {
			if rl.RateLimit.Cost > 0 {
				actualCost = rl.RateLimit.Cost
			}
			if rl.RateLimit.Limit > 0 {
				hdrLimit = rl.RateLimit.Limit
			}
			hdrRem = rl.RateLimit.Remaining
			if !rl.RateLimit.ResetAt.IsZero() {
				hdrReset = rl.RateLimit.ResetAt
			}
		}

		if len(env.Errors) > 0 {
			msg := env.Errors[0].Message
			if strings.Contains(strings.ToLower(msg), "rate limit") {
				lease.CompleteWithCooldown(30 * time.Second)
				c.retries.Add(1)
				continue
			}
			lease.Complete(hdrLimit, hdrRem, hdrReset, actualCost)
			// Only fail if Data is null/empty or critical error
			if len(env.Data) == 0 || string(env.Data) == "null" {
				return actualCost, fmt.Errorf("graphql error (%s): %s", env.Errors[0].Type, msg)
			}
		} else {
			lease.Complete(hdrLimit, hdrRem, hdrReset, actualCost)
		}

		c.gqlRequests.Add(1)
		c.gqlPoints.Add(int64(actualCost))

		if out != nil && len(env.Data) > 0 {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return actualCost, fmt.Errorf("decode graphql data: %w", err)
			}
		}
		return actualCost, nil
	}
	return 0, fmt.Errorf("graphql request exceeded %d retry attempts", maxAttempts)
}

// RESTResponse wraps decoded REST response status, pagination Link next URL, and 304 cache hit flag.
type RESTResponse struct {
	NotModified bool
	NextURL     string
	StatusCode  int
}

// DoRESTGet performs a GET on the REST bucket with automatic ETag `If-None-Match` conditional caching.
func (c *Client) DoRESTGet(ctx context.Context, pathOrURL string, useETag bool, out any) (*RESTResponse, error) {
	fullURL := pathOrURL
	if !strings.HasPrefix(pathOrURL, "http://") && !strings.HasPrefix(pathOrURL, "https://") {
		if !strings.HasPrefix(pathOrURL, "/") {
			pathOrURL = "/" + pathOrURL
		}
		fullURL = c.restBase + pathOrURL
	}

	const maxAttempts = 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		lease, err := c.pool.Acquire(ctx, tokenpool.BucketREST, 1)
		if err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			lease.Complete(-1, -1, time.Time{}, 0)
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+lease.Token)
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		req.Header.Set("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", "ghfetcher/"+version.Version+" (MSR-Research)")

		if useETag && c.store != nil {
			if etag := c.store.GetETag(ctx, fullURL); etag != "" {
				req.Header.Set("If-None-Match", etag)
			}
		}

		resp, doErr := c.httpClient.Do(req)
		if doErr != nil {
			lease.Complete(-1, -1, time.Time{}, 0)
			if attempt == maxAttempts || ctx.Err() != nil {
				return nil, fmt.Errorf("rest transport error: %w", doErr)
			}
			c.retries.Add(1)
			sleepContext(ctx, time.Duration(attempt)*time.Second)
			continue
		}

		bodyBytes, readErr := readBodyMaybeGzip(resp)
		_ = resp.Body.Close()
		hdrLimit, hdrRem, hdrReset := parseRateHeaders(resp.Header)

		if resp.StatusCode == http.StatusNotModified {
			// 304 Not Modified costs 0 REST quota!
			lease.Complete(hdrLimit, hdrRem, hdrReset, 0)
			c.etagHits304.Add(1)
			return &RESTResponse{NotModified: true, StatusCode: http.StatusNotModified}, nil
		}

		if isSecondaryRateLimit(resp.StatusCode, resp.Header, bodyBytes) {
			wait := parseRetryAfter(resp.Header, 60*time.Second)
			lease.CompleteWithCooldown(wait)
			c.retries.Add(1)
			continue
		}

		if resp.StatusCode >= 500 {
			lease.Complete(hdrLimit, hdrRem, hdrReset, 0)
			if attempt == maxAttempts {
				return nil, fmt.Errorf("rest server error HTTP %d: %s", resp.StatusCode, truncate(string(bodyBytes), 240))
			}
			c.retries.Add(1)
			sleepContext(ctx, time.Duration(attempt*2)*time.Second)
			continue
		}

		lease.Complete(hdrLimit, hdrRem, hdrReset, 1)
		c.restRequests.Add(1)

		if readErr != nil {
			return nil, fmt.Errorf("read rest body: %w", readErr)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("rest HTTP %d on %s: %s", resp.StatusCode, fullURL, truncate(string(bodyBytes), 240))
		}

		if useETag && c.store != nil {
			if newETag := resp.Header.Get("ETag"); newETag != "" {
				c.store.SetETag(ctx, fullURL, newETag)
			}
		}

		if out != nil && len(bodyBytes) > 0 {
			if err := json.Unmarshal(bodyBytes, out); err != nil {
				return nil, fmt.Errorf("decode rest json: %w", err)
			}
		}

		return &RESTResponse{
			StatusCode: resp.StatusCode,
			NextURL:    parseNextLink(resp.Header.Get("Link")),
		}, nil
	}

	return nil, fmt.Errorf("rest request exceeded %d retry attempts", maxAttempts)
}

func readBodyMaybeGzip(resp *http.Response) ([]byte, error) {
	var reader io.Reader = resp.Body
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	}
	return io.ReadAll(reader)
}

func parseRateHeaders(h http.Header) (limit int, remaining int, resetAt time.Time) {
	limit = -1
	remaining = -1
	if v := h.Get("X-RateLimit-Limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if v := h.Get("X-RateLimit-Remaining"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			remaining = n
		}
	}
	if v := h.Get("X-RateLimit-Reset"); v != "" {
		if sec, err := strconv.ParseInt(v, 10, 64); err == nil && sec > 0 {
			resetAt = time.Unix(sec, 0).UTC()
		}
	}
	return limit, remaining, resetAt
}

func isSecondaryRateLimit(status int, h http.Header, body []byte) bool {
	if status != http.StatusForbidden && status != http.StatusTooManyRequests {
		return false
	}
	if h.Get("Retry-After") != "" {
		return true
	}
	lower := strings.ToLower(string(body))
	return strings.Contains(lower, "secondary rate limit") ||
		strings.Contains(lower, "abuse detection") ||
		(status == http.StatusTooManyRequests)
}

func parseRetryAfter(h http.Header, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(h.Get("Retry-After")); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return fallback
}

func parseNextLink(linkHeader string) string {
	if linkHeader == "" {
		return ""
	}
	parts := strings.Split(linkHeader, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if strings.Contains(p, `rel="next"`) {
			start := strings.IndexByte(p, '<')
			end := strings.IndexByte(p, '>')
			if start >= 0 && end > start {
				return p[start+1 : end]
			}
		}
	}
	return ""
}

func sleepContext(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
