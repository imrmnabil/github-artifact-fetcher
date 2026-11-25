package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"ghfetcher/internal/store"
)

// RepoEstimate holds 1-point GraphQL pre-flight sizing & cost projections for a repository.
type RepoEstimate struct {
	Owner          string
	Name           string
	FullName       string
	GitHubID       int64
	DefaultBranch  string
	HeadSHA        string
	IsFork         bool
	Stars          int
	Forks          int
	DiskKB         int
	CommitsCount   int
	IssuesCount    int
	PRsCount       int
	EstGQLPoints   int
	EstRESTCalls   int
	EstAPISaved    int
}

// BatchEstimateRepos queries up to 35 repos per 1-point GraphQL request to get exact counts & disk size.
func (c *Client) BatchEstimateRepos(ctx context.Context, specs [][2]string) ([]RepoEstimate, int, error) {
	if len(specs) == 0 {
		return nil, 0, nil
	}

	var all []RepoEstimate
	totalCost := 0
	const chunkSize = 35

	for start := 0; start < len(specs); start += chunkSize {
		end := start + chunkSize
		if end > len(specs) {
			end = len(specs)
		}
		chunk := specs[start:end]

		var sb strings.Builder
		sb.WriteString("query {\n  rateLimit { limit remaining cost resetAt }\n")
		for idx, sp := range chunk {
			fmt.Fprintf(&sb, `  r%d: repository(owner: %q, name: %q) {
    databaseId
    owner { login }
    name
    isFork
    stargazerCount
    forkCount
    diskUsage
    defaultBranchRef {
      name
      target {
        ... on Commit {
          oid
          history { totalCount }
        }
      }
    }
    issues { totalCount }
    pullRequests { totalCount }
  }
`, idx, sp[0], sp[1])
		}
		sb.WriteString("}\n")

		rawMap := make(map[string]json.RawMessage)
		cost, err := c.DoGraphQL(ctx, sb.String(), nil, &rawMap)
		totalCost += cost
		if err != nil {
			return nil, totalCost, err
		}

		for idx, sp := range chunk {
			key := fmt.Sprintf("r%d", idx)
			raw, ok := rawMap[key]
			if !ok || len(raw) == 0 || string(raw) == "null" {
				return nil, totalCost, fmt.Errorf("repository %s/%s not found or inaccessible", sp[0], sp[1])
			}
			var node struct {
				DatabaseID       int64 `json:"databaseId"`
				Owner            struct{ Login string `json:"login"` } `json:"owner"`
				Name             string `json:"name"`
				IsFork           bool   `json:"isFork"`
				StargazerCount   int    `json:"stargazerCount"`
				ForkCount        int    `json:"forkCount"`
				DiskUsage        int    `json:"diskUsage"`
				DefaultBranchRef *struct {
					Name   string `json:"name"`
					Target *struct {
						OID     string `json:"oid"`
						History struct {
							TotalCount int `json:"totalCount"`
						} `json:"history"`
					} `json:"target"`
				} `json:"defaultBranchRef"`
				Issues       struct{ TotalCount int `json:"totalCount"` } `json:"issues"`
				PullRequests struct{ TotalCount int `json:"totalCount"` } `json:"pullRequests"`
			}
			if err := json.Unmarshal(raw, &node); err != nil {
				return nil, totalCost, err
			}

			defBranch := "main"
			headSHA := ""
			commits := 0
			if node.DefaultBranchRef != nil {
				if node.DefaultBranchRef.Name != "" {
					defBranch = node.DefaultBranchRef.Name
				}
				if node.DefaultBranchRef.Target != nil {
					headSHA = node.DefaultBranchRef.Target.OID
					commits = node.DefaultBranchRef.Target.History.TotalCount
				}
			}

			// Project hybrid GQL + REST cost:
			// Issues: 30/page (~1 pt/page) + PRs: 25/page (~1 pt/page) + ~5% overflow buffer
			issuePages := (node.Issues.TotalCount + 29) / 30
			prPages := (node.PullRequests.TotalCount + 24) / 25
			estGQL := issuePages + prPages + ((issuePages + prPages) / 15) + 1
			// REST review comments sweep (~1.5 inline diff comments per PR, 100/page)
			estREST := ((node.PullRequests.TotalCount*3)/2 + 99) / 100
			if node.PullRequests.TotalCount > 0 && estREST == 0 {
				estREST = 1
			}
			// Naive crawler cost vs hybrid+bare-git:
			naiveCalls := commits + node.PullRequests.TotalCount*3 + node.Issues.TotalCount*2
			saved := naiveCalls - (estGQL + estREST)
			if saved < 0 {
				saved = 0
			}

			all = append(all, RepoEstimate{
				Owner:         node.Owner.Login,
				Name:          node.Name,
				FullName:      fmt.Sprintf("%s/%s", node.Owner.Login, node.Name),
				GitHubID:      node.DatabaseID,
				DefaultBranch: defBranch,
				HeadSHA:       headSHA,
				IsFork:        node.IsFork,
				Stars:         node.StargazerCount,
				Forks:         node.ForkCount,
				DiskKB:        node.DiskUsage,
				CommitsCount:  commits,
				IssuesCount:   node.Issues.TotalCount,
				PRsCount:      node.PullRequests.TotalCount,
				EstGQLPoints:  estGQL,
				EstRESTCalls:  estREST,
				EstAPISaved:   saved,
			})
		}
	}

	return all, totalCost, nil
}

// OverflowTask represents pagination continuation for an exceptionally large Issue or PR.
type OverflowTask struct {
	Owner      string
	Name       string
	RepoID     int64
	Number     int
	IsPR       bool
	Kind       string // "comments", "reviews", "timeline"
	EndCursor  string
}

type gqlReactionGroup struct {
	Content  string `json:"content"`
	Reactors struct {
		TotalCount int `json:"totalCount"`
	} `json:"reactors"`
}

func encodeReactionGroups(groups []gqlReactionGroup) string {
	m := make(map[string]int)
	for _, g := range groups {
		if g.Reactors.TotalCount > 0 {
			m[g.Content] = g.Reactors.TotalCount
		}
	}
	if len(m) == 0 {
		return "{}"
	}
	b, _ := json.Marshal(m)
	return string(b)
}

type gqlActor struct {
	TypeName   string `json:"__typename"`
	Login      string `json:"login"`
	DatabaseID int64  `json:"databaseId"`
	Name       string `json:"name"`
	Email      string `json:"email"`
}

const issuesPageQuery = `
query($owner: String!, $name: String!, $cursor: String, $since: DateTime, $includeComments: Boolean!, $includeTimeline: Boolean!) {
  rateLimit { limit remaining cost resetAt }
  repository(owner: $owner, name: $name) {
    issues(first: 30, after: $cursor, filterBy: {since: $since}, orderBy: {field: UPDATED_AT, direction: ASC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        databaseId
        state
        stateReason
        title
        body
        createdAt
        updatedAt
        closedAt
        author {
          __typename
          login
          ... on User { databaseId name email }
          ... on Bot { databaseId }
        }
        milestone { title }
        labels(first: 20) { nodes { name } }
        assignees(first: 10) { nodes { login } }
        reactionGroups { content reactors { totalCount } }
        comments(first: 20) @include(if: $includeComments) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            databaseId
            body
            createdAt
            updatedAt
            author { login }
            reactionGroups { content reactors { totalCount } }
          }
        }
        timelineItems(first: 25, itemTypes: [LABELED_EVENT, UNLABELED_EVENT, ASSIGNED_EVENT, UNASSIGNED_EVENT, CLOSED_EVENT, REOPENED_EVENT, MILESTONED_EVENT, RENAMED_TITLE_EVENT, CROSS_REFERENCED_EVENT]) @include(if: $includeTimeline) {
          pageInfo { hasNextPage endCursor }
          nodes {
            __typename
            ... on LabeledEvent { id createdAt actor { login } label { name } }
            ... on UnlabeledEvent { id createdAt actor { login } label { name } }
            ... on AssignedEvent { id createdAt actor { login } }
            ... on UnassignedEvent { id createdAt actor { login } }
            ... on ClosedEvent { id createdAt actor { login } stateReason }
            ... on ReopenedEvent { id createdAt actor { login } }
            ... on MilestonedEvent { id createdAt actor { login } milestoneTitle }
            ... on RenamedTitleEvent { id createdAt actor { login } previousTitle currentTitle }
            ... on CrossReferencedEvent {
              id
              createdAt
              actor { login }
              source {
                __typename
                ... on Issue { number }
                ... on PullRequest { number }
              }
            }
          }
        }
      }
    }
  }
}
`

// IssuesPageResult contains parsed artifacts and pagination state for one GraphQL issues page.
type IssuesPageResult struct {
	Batch         store.ArtifactBatch
	HasNextPage   bool
	EndCursor     string
	MaxUpdatedAt  time.Time
	OverflowTasks []OverflowTask
	ItemsCount    int
	APISaved      int
}

func (c *Client) FetchIssuesPage(
	ctx context.Context,
	repoID int64,
	owner, name, cursor, sinceISO string,
	includeComments, includeTimeline bool,
) (*IssuesPageResult, error) {
	vars := map[string]any{
		"owner":           owner,
		"name":            name,
		"includeComments": includeComments,
		"includeTimeline": includeTimeline,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}
	if sinceISO != "" {
		vars["since"] = sinceISO
	}

	var resp struct {
		Repository struct {
			Issues struct {
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
				Nodes []struct {
					Number      int       `json:"number"`
					DatabaseID  int64     `json:"databaseId"`
					State       string    `json:"state"`
					StateReason string    `json:"stateReason"`
					Title       string    `json:"title"`
					Body        string    `json:"body"`
					CreatedAt   time.Time `json:"createdAt"`
					UpdatedAt   time.Time `json:"updatedAt"`
					ClosedAt    time.Time `json:"closedAt"`
					Author      *gqlActor `json:"author"`
					Milestone   *struct {
						Title string `json:"title"`
					} `json:"milestone"`
					Labels struct {
						Nodes []struct{ Name string `json:"name"` } `json:"nodes"`
					} `json:"labels"`
					Assignees struct {
						Nodes []struct{ Login string `json:"login"` } `json:"nodes"`
					} `json:"assignees"`
					ReactionGroups []gqlReactionGroup `json:"reactionGroups"`
					Comments       *struct {
						TotalCount int `json:"totalCount"`
						PageInfo   struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
						Nodes []struct {
							DatabaseID     int64              `json:"databaseId"`
							Body           string             `json:"body"`
							CreatedAt      time.Time          `json:"createdAt"`
							UpdatedAt      time.Time          `json:"updatedAt"`
							Author         *struct{ Login string `json:"login"` } `json:"author"`
							ReactionGroups []gqlReactionGroup `json:"reactionGroups"`
						} `json:"nodes"`
					} `json:"comments"`
					TimelineItems *struct {
						PageInfo struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
						Nodes []json.RawMessage `json:"nodes"`
					} `json:"timelineItems"`
				} `json:"nodes"`
			} `json:"issues"`
		} `json:"repository"`
	}

	if _, err := c.DoGraphQL(ctx, issuesPageQuery, vars, &resp); err != nil {
		return nil, err
	}

	res := &IssuesPageResult{
		HasNextPage: resp.Repository.Issues.PageInfo.HasNextPage,
		EndCursor:   resp.Repository.Issues.PageInfo.EndCursor,
	}
	userSeen := make(map[string]struct{})

	for _, n := range resp.Repository.Issues.Nodes {
		res.ItemsCount++
		if n.UpdatedAt.After(res.MaxUpdatedAt) {
			res.MaxUpdatedAt = n.UpdatedAt
		}

		authorLogin := ""
		if n.Author != nil && n.Author.Login != "" {
			authorLogin = n.Author.Login
			if _, ok := userSeen[authorLogin]; !ok {
				userSeen[authorLogin] = struct{}{}
				res.Batch.Users = append(res.Batch.Users, store.User{
					GitHubID: n.Author.DatabaseID,
					Login:    authorLogin,
					Name:     n.Author.Name,
					Email:    n.Author.Email,
					Type:     n.Author.TypeName,
				})
			}
		}

		milestone := ""
		if n.Milestone != nil {
			milestone = n.Milestone.Title
		}
		var labels []string
		for _, l := range n.Labels.Nodes {
			labels = append(labels, l.Name)
		}
		var assignees []string
		for _, a := range n.Assignees.Nodes {
			assignees = append(assignees, a.Login)
		}

		commentCount := 0
		if n.Comments != nil {
			commentCount = n.Comments.TotalCount
			for _, cm := range n.Comments.Nodes {
				cAuthor := ""
				if cm.Author != nil {
					cAuthor = cm.Author.Login
				}
				res.Batch.IssueComments = append(res.Batch.IssueComments, store.IssueComment{
					RepoID:      repoID,
					CommentID:   cm.DatabaseID,
					ItemNumber:  n.Number,
					IsPR:        false,
					AuthorLogin: cAuthor,
					Body:        cm.Body,
					Reactions:   encodeReactionGroups(cm.ReactionGroups),
					CreatedAt:   cm.CreatedAt,
					UpdatedAt:   cm.UpdatedAt,
				})
			}
			if n.Comments.PageInfo.HasNextPage {
				res.OverflowTasks = append(res.OverflowTasks, OverflowTask{
					Owner: owner, Name: name, RepoID: repoID, Number: n.Number, IsPR: false,
					Kind: "comments", EndCursor: n.Comments.PageInfo.EndCursor,
				})
			}
			res.APISaved++ // Avoided per-issue /comments REST call
		}

		if n.TimelineItems != nil {
			for _, rawEv := range n.TimelineItems.Nodes {
				if ev, ok := parseTimelineNode(repoID, n.Number, false, rawEv); ok {
					res.Batch.TimelineEvents = append(res.Batch.TimelineEvents, ev)
				}
			}
			if n.TimelineItems.PageInfo.HasNextPage {
				res.OverflowTasks = append(res.OverflowTasks, OverflowTask{
					Owner: owner, Name: name, RepoID: repoID, Number: n.Number, IsPR: false,
					Kind: "timeline", EndCursor: n.TimelineItems.PageInfo.EndCursor,
				})
			}
			res.APISaved++ // Avoided per-issue /timeline REST call
		}

		res.Batch.Issues = append(res.Batch.Issues, store.Issue{
			RepoID:       repoID,
			Number:       n.Number,
			GitHubID:     n.DatabaseID,
			State:        n.State,
			StateReason:  n.StateReason,
			Title:        n.Title,
			Body:         n.Body,
			AuthorLogin:  authorLogin,
			Milestone:    milestone,
			Labels:       labels,
			Assignees:    assignees,
			CommentCount: commentCount,
			Reactions:    encodeReactionGroups(n.ReactionGroups),
			CreatedAt:    n.CreatedAt,
			UpdatedAt:    n.UpdatedAt,
			ClosedAt:     n.ClosedAt,
		})
	}

	return res, nil
}

const prsPageQueryTemplate = `
query($owner: String!, $name: String!, $cursor: String, $includeComments: Boolean!, $includeReviews: Boolean!, $includeTimeline: Boolean!) {
  rateLimit { limit remaining cost resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: 25, after: $cursor, orderBy: {field: UPDATED_AT, direction: %s}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        number
        databaseId
        state
        isDraft
        merged
        title
        body
        createdAt
        updatedAt
        closedAt
        mergedAt
        author {
          __typename
          login
          ... on User { databaseId name email }
          ... on Bot { databaseId }
        }
        mergedBy { login }
        baseRefName
        baseRefOid
        headRefName
        headRefOid
        mergeCommit { oid }
        additions
        deletions
        changedFiles
        labels(first: 15) { nodes { name } }
        assignees(first: 10) { nodes { login } }
        closingIssuesReferences(first: 10) { nodes { number } }
        reactionGroups { content reactors { totalCount } }
        comments(first: 20) @include(if: $includeComments) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            databaseId
            body
            createdAt
            updatedAt
            author { login }
            reactionGroups { content reactors { totalCount } }
          }
        }
        reviews(first: 20) @include(if: $includeReviews) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            databaseId
            state
            body
            submittedAt
            author { login }
            commit { oid }
          }
        }
        timelineItems(first: 25, itemTypes: [LABELED_EVENT, UNLABELED_EVENT, ASSIGNED_EVENT, CLOSED_EVENT, REOPENED_EVENT, MERGED_EVENT, REVIEW_REQUESTED_EVENT, HEAD_REF_FORCE_PUSHED_EVENT, RENAMED_TITLE_EVENT, CROSS_REFERENCED_EVENT]) @include(if: $includeTimeline) {
          pageInfo { hasNextPage endCursor }
          nodes {
            __typename
            ... on LabeledEvent { id createdAt actor { login } label { name } }
            ... on UnlabeledEvent { id createdAt actor { login } label { name } }
            ... on AssignedEvent { id createdAt actor { login } }
            ... on ClosedEvent { id createdAt actor { login } stateReason }
            ... on ReopenedEvent { id createdAt actor { login } }
            ... on MergedEvent { id createdAt actor { login } mergeRefName commit { oid } }
            ... on ReviewRequestedEvent { id createdAt actor { login } }
            ... on HeadRefForcePushedEvent { id createdAt actor { login } beforeCommit { oid } afterCommit { oid } }
            ... on RenamedTitleEvent { id createdAt actor { login } previousTitle currentTitle }
            ... on CrossReferencedEvent {
              id
              createdAt
              actor { login }
              source {
                __typename
                ... on Issue { number }
                ... on PullRequest { number }
              }
            }
          }
        }
      }
    }
  }
}
`

// PRsPageResult contains parsed PRs, reviews, comments, timeline events, and incremental cutoff flag.
type PRsPageResult struct {
	Batch          store.ArtifactBatch
	HasNextPage    bool
	EndCursor      string
	MaxUpdatedAt   time.Time
	ReachedCutoff  bool
	OverflowTasks  []OverflowTask
	ItemsCount     int
	APISaved       int
}

func (c *Client) FetchPRsPage(
	ctx context.Context,
	repoID int64,
	owner, name, cursor string,
	descending bool,
	cutoff time.Time,
	includeComments, includeReviews, includeTimeline bool,
) (*PRsPageResult, error) {
	dir := "ASC"
	if descending {
		dir = "DESC"
	}
	query := fmt.Sprintf(prsPageQueryTemplate, dir)
	vars := map[string]any{
		"owner":           owner,
		"name":            name,
		"includeComments": includeComments,
		"includeReviews":  includeReviews,
		"includeTimeline": includeTimeline,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	var resp struct {
		Repository struct {
			PullRequests struct {
				PageInfo struct {
					HasNextPage bool   `json:"hasNextPage"`
					EndCursor   string `json:"endCursor"`
				} `json:"pageInfo"`
				Nodes []struct {
					Number      int       `json:"number"`
					DatabaseID  int64     `json:"databaseId"`
					State       string    `json:"state"`
					IsDraft     bool      `json:"isDraft"`
					Merged      bool      `json:"merged"`
					Title       string    `json:"title"`
					Body        string    `json:"body"`
					CreatedAt   time.Time `json:"createdAt"`
					UpdatedAt   time.Time `json:"updatedAt"`
					ClosedAt    time.Time `json:"closedAt"`
					MergedAt    time.Time `json:"mergedAt"`
					Author      *gqlActor `json:"author"`
					MergedBy    *struct{ Login string `json:"login"` } `json:"mergedBy"`
					BaseRefName string    `json:"baseRefName"`
					BaseRefOid  string    `json:"baseRefOid"`
					HeadRefName string    `json:"headRefName"`
					HeadRefOid  string    `json:"headRefOid"`
					MergeCommit *struct{ OID string `json:"oid"` } `json:"mergeCommit"`
					Additions    int `json:"additions"`
					Deletions    int `json:"deletions"`
					ChangedFiles int `json:"changedFiles"`
					Labels       struct {
						Nodes []struct{ Name string `json:"name"` } `json:"nodes"`
					} `json:"labels"`
					Assignees struct {
						Nodes []struct{ Login string `json:"login"` } `json:"nodes"`
					} `json:"assignees"`
					ClosingIssuesReferences struct {
						Nodes []struct{ Number int `json:"number"` } `json:"nodes"`
					} `json:"closingIssuesReferences"`
					ReactionGroups []gqlReactionGroup `json:"reactionGroups"`
					Comments       *struct {
						TotalCount int `json:"totalCount"`
						PageInfo   struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
						Nodes []struct {
							DatabaseID     int64              `json:"databaseId"`
							Body           string             `json:"body"`
							CreatedAt      time.Time          `json:"createdAt"`
							UpdatedAt      time.Time          `json:"updatedAt"`
							Author         *struct{ Login string `json:"login"` } `json:"author"`
							ReactionGroups []gqlReactionGroup `json:"reactionGroups"`
						} `json:"nodes"`
					} `json:"comments"`
					Reviews *struct {
						TotalCount int `json:"totalCount"`
						PageInfo   struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
						Nodes []struct {
							DatabaseID  int64     `json:"databaseId"`
							State       string    `json:"state"`
							Body        string    `json:"body"`
							SubmittedAt time.Time `json:"submittedAt"`
							Author      *struct{ Login string `json:"login"` } `json:"author"`
							Commit      *struct{ OID string `json:"oid"` } `json:"commit"`
						} `json:"nodes"`
					} `json:"reviews"`
					TimelineItems *struct {
						PageInfo struct {
							HasNextPage bool   `json:"hasNextPage"`
							EndCursor   string `json:"endCursor"`
						} `json:"pageInfo"`
						Nodes []json.RawMessage `json:"nodes"`
					} `json:"timelineItems"`
				} `json:"nodes"`
			} `json:"pullRequests"`
		} `json:"repository"`
	}

	if _, err := c.DoGraphQL(ctx, query, vars, &resp); err != nil {
		return nil, err
	}

	res := &PRsPageResult{
		HasNextPage: resp.Repository.PullRequests.PageInfo.HasNextPage,
		EndCursor:   resp.Repository.PullRequests.PageInfo.EndCursor,
	}
	userSeen := make(map[string]struct{})

	for _, n := range resp.Repository.PullRequests.Nodes {
		if descending && !cutoff.IsZero() && n.UpdatedAt.Before(cutoff) {
			res.ReachedCutoff = true
			break
		}

		res.ItemsCount++
		if n.UpdatedAt.After(res.MaxUpdatedAt) {
			res.MaxUpdatedAt = n.UpdatedAt
		}

		authorLogin := ""
		if n.Author != nil && n.Author.Login != "" {
			authorLogin = n.Author.Login
			if _, ok := userSeen[authorLogin]; !ok {
				userSeen[authorLogin] = struct{}{}
				res.Batch.Users = append(res.Batch.Users, store.User{
					GitHubID: n.Author.DatabaseID,
					Login:    authorLogin,
					Name:     n.Author.Name,
					Email:    n.Author.Email,
					Type:     n.Author.TypeName,
				})
			}
		}

		mergedBy := ""
		if n.MergedBy != nil {
			mergedBy = n.MergedBy.Login
		}
		mergeSHA := ""
		if n.MergeCommit != nil {
			mergeSHA = n.MergeCommit.OID
		}

		var labels, assignees []string
		var closingIssues []int
		for _, l := range n.Labels.Nodes {
			labels = append(labels, l.Name)
		}
		for _, a := range n.Assignees.Nodes {
			assignees = append(assignees, a.Login)
		}
		for _, ci := range n.ClosingIssuesReferences.Nodes {
			closingIssues = append(closingIssues, ci.Number)
		}

		commentCount := 0
		if n.Comments != nil {
			commentCount = n.Comments.TotalCount
			for _, cm := range n.Comments.Nodes {
				cAuthor := ""
				if cm.Author != nil {
					cAuthor = cm.Author.Login
				}
				res.Batch.IssueComments = append(res.Batch.IssueComments, store.IssueComment{
					RepoID:      repoID,
					CommentID:   cm.DatabaseID,
					ItemNumber:  n.Number,
					IsPR:        true,
					AuthorLogin: cAuthor,
					Body:        cm.Body,
					Reactions:   encodeReactionGroups(cm.ReactionGroups),
					CreatedAt:   cm.CreatedAt,
					UpdatedAt:   cm.UpdatedAt,
				})
			}
			if n.Comments.PageInfo.HasNextPage {
				res.OverflowTasks = append(res.OverflowTasks, OverflowTask{
					Owner: owner, Name: name, RepoID: repoID, Number: n.Number, IsPR: true,
					Kind: "comments", EndCursor: n.Comments.PageInfo.EndCursor,
				})
			}
			res.APISaved++
		}

		reviewCount := 0
		if n.Reviews != nil {
			reviewCount = n.Reviews.TotalCount
			for _, rv := range n.Reviews.Nodes {
				rAuthor := ""
				if rv.Author != nil {
					rAuthor = rv.Author.Login
				}
				rCommit := ""
				if rv.Commit != nil {
					rCommit = rv.Commit.OID
				}
				res.Batch.PRReviews = append(res.Batch.PRReviews, store.PRReview{
					RepoID:      repoID,
					ReviewID:    rv.DatabaseID,
					PRNumber:    n.Number,
					AuthorLogin: rAuthor,
					State:       rv.State,
					CommitSHA:   rCommit,
					Body:        rv.Body,
					SubmittedAt: rv.SubmittedAt,
				})
			}
			if n.Reviews.PageInfo.HasNextPage {
				res.OverflowTasks = append(res.OverflowTasks, OverflowTask{
					Owner: owner, Name: name, RepoID: repoID, Number: n.Number, IsPR: true,
					Kind: "reviews", EndCursor: n.Reviews.PageInfo.EndCursor,
				})
			}
			res.APISaved++
		}

		if n.TimelineItems != nil {
			for _, rawEv := range n.TimelineItems.Nodes {
				if ev, ok := parseTimelineNode(repoID, n.Number, true, rawEv); ok {
					res.Batch.TimelineEvents = append(res.Batch.TimelineEvents, ev)
				}
			}
			if n.TimelineItems.PageInfo.HasNextPage {
				res.OverflowTasks = append(res.OverflowTasks, OverflowTask{
					Owner: owner, Name: name, RepoID: repoID, Number: n.Number, IsPR: true,
					Kind: "timeline", EndCursor: n.TimelineItems.PageInfo.EndCursor,
				})
			}
			res.APISaved++
		}

		res.Batch.PullRequests = append(res.Batch.PullRequests, store.PullRequest{
			RepoID:        repoID,
			Number:        n.Number,
			GitHubID:      n.DatabaseID,
			State:         n.State,
			IsDraft:       n.IsDraft,
			Merged:        n.Merged,
			Title:         n.Title,
			Body:          n.Body,
			AuthorLogin:   authorLogin,
			MergedByLogin: mergedBy,
			BaseRef:       n.BaseRefName,
			BaseSHA:       n.BaseRefOid,
			HeadRef:       n.HeadRefName,
			HeadSHA:       n.HeadRefOid,
			MergeSHA:      mergeSHA,
			Additions:     n.Additions,
			Deletions:     n.Deletions,
			ChangedFiles:  n.ChangedFiles,
			CommentCount:  commentCount,
			ReviewCount:   reviewCount,
			Labels:        labels,
			Assignees:     assignees,
			ClosingIssues: closingIssues,
			Reactions:     encodeReactionGroups(n.ReactionGroups),
			CreatedAt:     n.CreatedAt,
			UpdatedAt:     n.UpdatedAt,
			ClosedAt:      n.ClosedAt,
			MergedAt:      n.MergedAt,
		})
	}

	return res, nil
}

// DrainOverflowTask paginates remaining comments, reviews, or timeline events for mega-Issues/PRs.
func (c *Client) DrainOverflowTask(ctx context.Context, st *store.Store, task OverflowTask) error {
	cursor := task.EndCursor
	itemField := "issue"
	if task.IsPR {
		itemField = "pullRequest"
	}

	for cursor != "" {
		if err := ctx.Err(); err != nil {
			return err
		}

		var batch store.ArtifactBatch
		var hasNext bool
		var nextCursor string

		switch task.Kind {
		case "comments":
			q := fmt.Sprintf(`query($owner:String!, $name:String!, $num:Int!, $cursor:String) {
				rateLimit { limit remaining cost resetAt }
				repository(owner:$owner, name:$name) {
					item: %s(number:$num) {
						comments(first:100, after:$cursor) {
							pageInfo { hasNextPage endCursor }
							nodes { databaseId body createdAt updatedAt author { login } reactionGroups { content reactors { totalCount } } }
						}
					}
				}
			}`, itemField)
			var resp struct {
				Repository struct {
					Item struct {
						Comments struct {
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
							Nodes []struct {
								DatabaseID     int64              `json:"databaseId"`
								Body           string             `json:"body"`
								CreatedAt      time.Time          `json:"createdAt"`
								UpdatedAt      time.Time          `json:"updatedAt"`
								Author         *struct{ Login string `json:"login"` } `json:"author"`
								ReactionGroups []gqlReactionGroup `json:"reactionGroups"`
							} `json:"nodes"`
						} `json:"comments"`
					} `json:"item"`
				} `json:"repository"`
			}
			if _, err := c.DoGraphQL(ctx, q, map[string]any{
				"owner": task.Owner, "name": task.Name, "num": task.Number, "cursor": cursor,
			}, &resp); err != nil {
				return err
			}
			for _, cm := range resp.Repository.Item.Comments.Nodes {
				author := ""
				if cm.Author != nil {
					author = cm.Author.Login
				}
				batch.IssueComments = append(batch.IssueComments, store.IssueComment{
					RepoID: task.RepoID, CommentID: cm.DatabaseID, ItemNumber: task.Number, IsPR: task.IsPR,
					AuthorLogin: author, Body: cm.Body, Reactions: encodeReactionGroups(cm.ReactionGroups),
					CreatedAt: cm.CreatedAt, UpdatedAt: cm.UpdatedAt,
				})
			}
			hasNext = resp.Repository.Item.Comments.PageInfo.HasNextPage
			nextCursor = resp.Repository.Item.Comments.PageInfo.EndCursor

		case "reviews":
			q := `query($owner:String!, $name:String!, $num:Int!, $cursor:String) {
				rateLimit { limit remaining cost resetAt }
				repository(owner:$owner, name:$name) {
					pullRequest(number:$num) {
						reviews(first:100, after:$cursor) {
							pageInfo { hasNextPage endCursor }
							nodes { databaseId state body submittedAt author { login } commit { oid } }
						}
					}
				}
			}`
			var resp struct {
				Repository struct {
					PullRequest struct {
						Reviews struct {
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
							Nodes []struct {
								DatabaseID  int64     `json:"databaseId"`
								State       string    `json:"state"`
								Body        string    `json:"body"`
								SubmittedAt time.Time `json:"submittedAt"`
								Author      *struct{ Login string `json:"login"` } `json:"author"`
								Commit      *struct{ OID string `json:"oid"` } `json:"commit"`
							} `json:"nodes"`
						} `json:"reviews"`
					} `json:"pullRequest"`
				} `json:"repository"`
			}
			if _, err := c.DoGraphQL(ctx, q, map[string]any{
				"owner": task.Owner, "name": task.Name, "num": task.Number, "cursor": cursor,
			}, &resp); err != nil {
				return err
			}
			for _, rv := range resp.Repository.PullRequest.Reviews.Nodes {
				author := ""
				if rv.Author != nil {
					author = rv.Author.Login
				}
				sha := ""
				if rv.Commit != nil {
					sha = rv.Commit.OID
				}
				batch.PRReviews = append(batch.PRReviews, store.PRReview{
					RepoID: task.RepoID, ReviewID: rv.DatabaseID, PRNumber: task.Number,
					AuthorLogin: author, State: rv.State, CommitSHA: sha, Body: rv.Body, SubmittedAt: rv.SubmittedAt,
				})
			}
			hasNext = resp.Repository.PullRequest.Reviews.PageInfo.HasNextPage
			nextCursor = resp.Repository.PullRequest.Reviews.PageInfo.EndCursor

		case "timeline":
			q := fmt.Sprintf(`query($owner:String!, $name:String!, $num:Int!, $cursor:String) {
				rateLimit { limit remaining cost resetAt }
				repository(owner:$owner, name:$name) {
					item: %s(number:$num) {
						timelineItems(first:100, after:$cursor, itemTypes: [LABELED_EVENT, UNLABELED_EVENT, ASSIGNED_EVENT, CLOSED_EVENT, REOPENED_EVENT, RENAMED_TITLE_EVENT, CROSS_REFERENCED_EVENT]) {
							pageInfo { hasNextPage endCursor }
							nodes {
								__typename
								... on LabeledEvent { id createdAt actor { login } label { name } }
								... on UnlabeledEvent { id createdAt actor { login } label { name } }
								... on AssignedEvent { id createdAt actor { login } }
								... on ClosedEvent { id createdAt actor { login } stateReason }
								... on ReopenedEvent { id createdAt actor { login } }
								... on RenamedTitleEvent { id createdAt actor { login } previousTitle currentTitle }
								... on CrossReferencedEvent { id createdAt actor { login } source { __typename ... on Issue { number } ... on PullRequest { number } } }
							}
						}
					}
				}
			}`, itemField)
			var resp struct {
				Repository struct {
					Item struct {
						TimelineItems struct {
							PageInfo struct {
								HasNextPage bool   `json:"hasNextPage"`
								EndCursor   string `json:"endCursor"`
							} `json:"pageInfo"`
							Nodes []json.RawMessage `json:"nodes"`
						} `json:"timelineItems"`
					} `json:"item"`
				} `json:"repository"`
			}
			if _, err := c.DoGraphQL(ctx, q, map[string]any{
				"owner": task.Owner, "name": task.Name, "num": task.Number, "cursor": cursor,
			}, &resp); err != nil {
				return err
			}
			for _, rawEv := range resp.Repository.Item.TimelineItems.Nodes {
				if ev, ok := parseTimelineNode(task.RepoID, task.Number, task.IsPR, rawEv); ok {
					batch.TimelineEvents = append(batch.TimelineEvents, ev)
				}
			}
			hasNext = resp.Repository.Item.TimelineItems.PageInfo.HasNextPage
			nextCursor = resp.Repository.Item.TimelineItems.PageInfo.EndCursor
		}

		if err := st.SaveArtifactBatch(ctx, batch); err != nil {
			return err
		}
		if !hasNext {
			break
		}
		cursor = nextCursor
	}
	return nil
}

func parseTimelineNode(repoID int64, itemNum int, isPR bool, raw json.RawMessage) (store.TimelineEvent, bool) {
	if len(raw) == 0 || string(raw) == "null" || string(raw) == "{}" {
		return store.TimelineEvent{}, false
	}
	var base struct {
		TypeName  string    `json:"__typename"`
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"createdAt"`
		Actor     *struct {
			Login string `json:"login"`
		} `json:"actor"`
	}
	if err := json.Unmarshal(raw, &base); err != nil || base.TypeName == "" || base.ID == "" {
		return store.TimelineEvent{}, false
	}
	actor := ""
	if base.Actor != nil {
		actor = base.Actor.Login
	}
	return store.TimelineEvent{
		RepoID:     repoID,
		EventKey:   base.ID,
		ItemNumber: itemNum,
		IsPR:       isPR,
		EventType:  base.TypeName,
		ActorLogin: actor,
		Payload:    string(raw),
		CreatedAt:  base.CreatedAt,
	}, true
}
