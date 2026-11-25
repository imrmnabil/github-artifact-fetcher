package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"ghfetcher/internal/store"
)

type restReviewComment struct {
	ID                  int64     `json:"id"`
	PullRequestReviewID *int64    `json:"pull_request_review_id"`
	InReplyToID         *int64    `json:"in_reply_to_id"`
	PullRequestURL      string    `json:"pull_request_url"`
	DiffHunk            string    `json:"diff_hunk"`
	Path                string    `json:"path"`
	CommitID            string    `json:"commit_id"`
	OriginalCommitID    string    `json:"original_commit_id"`
	Line                *int      `json:"line"`
	StartLine           *int      `json:"start_line"`
	Side                string    `json:"side"`
	Body                string    `json:"body"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	User                *struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
		Type  string `json:"type"`
	} `json:"user"`
	Reactions map[string]any `json:"reactions"`
}

// SweepRepoReviewCommentsREST uses the independent REST token pool to bulk-fetch ALL inline PR diff comments
// across the entire repository at 100 comments/request (instead of N per-PR requests), supporting ETag 304s and incremental `since`.
func (c *Client) SweepRepoReviewCommentsREST(
	ctx context.Context,
	st *store.Store,
	repoID int64,
	owner, name string,
	onProgress func(fetched int),
) (totalFetched int, err error) {
	const streamName = "rest_pr_review_comments"
	cp, err := st.GetCheckpoint(ctx, repoID, streamName)
	if err != nil {
		return 0, err
	}

	var nextURL string
	var watermark string
	var doneCount int64

	if cp != nil {
		watermark = cp.Watermark
		doneCount = cp.ItemsDone
		if !cp.Completed && cp.Cursor != "" {
			nextURL = cp.Cursor
		}
	}

	if nextURL == "" {
		q := url.Values{}
		q.Set("sort", "updated")
		q.Set("direction", "asc")
		q.Set("per_page", "100")
		if watermark != "" {
			q.Set("since", watermark)
		}
		nextURL = fmt.Sprintf("/repos/%s/%s/pulls/comments?%s", owner, name, q.Encode())
	}

	maxUpdated := parseRFC3339(watermark)

	for nextURL != "" {
		if err := ctx.Err(); err != nil {
			return totalFetched, err
		}

		var items []restReviewComment
		meta, err := c.DoRESTGet(ctx, nextURL, true, &items)
		if err != nil {
			return totalFetched, err
		}
		if meta.NotModified {
			break
		}

		var batch store.ArtifactBatch
		userSeen := make(map[string]struct{})

		for _, it := range items {
			if it.UpdatedAt.After(maxUpdated) {
				maxUpdated = it.UpdatedAt
			}
			author := ""
			if it.User != nil && it.User.Login != "" {
				author = it.User.Login
				if _, ok := userSeen[author]; !ok {
					userSeen[author] = struct{}{}
					batch.Users = append(batch.Users, store.User{
						GitHubID: it.User.ID,
						Login:    author,
						Type:     it.User.Type,
					})
				}
			}

			var revID, replyID int64
			var line, startLine int
			if it.PullRequestReviewID != nil {
				revID = *it.PullRequestReviewID
			}
			if it.InReplyToID != nil {
				replyID = *it.InReplyToID
			}
			if it.Line != nil {
				line = *it.Line
			}
			if it.StartLine != nil {
				startLine = *it.StartLine
			}

			prNum := extractTrailingNumber(it.PullRequestURL)
			reactionsJSON := encodeRESTReactions(it.Reactions)

			batch.ReviewComments = append(batch.ReviewComments, store.PRReviewComment{
				RepoID:      repoID,
				CommentID:   it.ID,
				PRNumber:    prNum,
				ReviewID:    revID,
				InReplyToID: replyID,
				AuthorLogin: author,
				Path:        it.Path,
				CommitSHA:   it.CommitID,
				OriginalSHA: it.OriginalCommitID,
				DiffHunk:    it.DiffHunk,
				Line:        line,
				StartLine:   startLine,
				Side:        it.Side,
				Body:        it.Body,
				Reactions:   reactionsJSON,
				CreatedAt:   it.CreatedAt,
				UpdatedAt:   it.UpdatedAt,
			})
		}

		totalFetched += len(items)
		doneCount += int64(len(items))
		hasMore := meta.NextURL != ""

		wm := watermark
		if !maxUpdated.IsZero() {
			wm = maxUpdated.UTC().Format(time.RFC3339)
		}
		cursorToSave := ""
		if hasMore {
			cursorToSave = meta.NextURL
		}

		batch.Checkpoint = &store.SyncCheckpoint{
			RepoID:     repoID,
			StreamName: streamName,
			Cursor:     cursorToSave,
			Watermark:  wm,
			ItemsDone:  doneCount,
			Completed:  !hasMore,
		}

		if err := st.SaveArtifactBatch(ctx, batch); err != nil {
			return totalFetched, err
		}

		if onProgress != nil && len(items) > 0 {
			onProgress(totalFetched)
		}
		nextURL = meta.NextURL
	}

	return totalFetched, nil
}

func extractTrailingNumber(u string) int {
	idx := strings.LastIndexByte(u, '/')
	if idx < 0 || idx+1 >= len(u) {
		return 0
	}
	n, _ := strconv.Atoi(u[idx+1:])
	return n
}

func encodeRESTReactions(m map[string]any) string {
	if len(m) == 0 {
		return "{}"
	}
	clean := make(map[string]int)
	for k, v := range m {
		if k == "url" || k == "total_count" {
			continue
		}
		if num, ok := v.(float64); ok && num > 0 {
			clean[k] = int(num)
		}
	}
	if len(clean) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(clean)
	return string(b)
}

func parseRFC3339(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, s)
	return t
}
