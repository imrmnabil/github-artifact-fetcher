package store

import "time"

type Repository struct {
	ID              int64     `json:"id"`
	GitHubID        int64     `json:"github_id"`
	Owner           string    `json:"owner"`
	Name            string    `json:"name"`
	FullName        string    `json:"full_name"`
	DefaultBranch   string    `json:"default_branch"`
	IsFork          bool      `json:"is_fork"`
	Stars           int       `json:"stars"`
	Forks           int       `json:"forks"`
	DiskKB          int       `json:"disk_kb"`
	TotalCommitsEst int       `json:"total_commits_est"`
	TotalIssuesEst  int       `json:"total_issues_est"`
	TotalPRsEst     int       `json:"total_prs_est"`
	ClonedHeadSHA   string    `json:"cloned_head_sha"`
	LastCommitAt    time.Time `json:"last_commit_at"`
	LastSyncedAt    time.Time `json:"last_synced_at"`
}

type User struct {
	GitHubID int64  `json:"github_id"`
	Login    string `json:"login"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Type     string `json:"type"`
}

type Commit struct {
	RepoID         int64        `json:"repo_id"`
	SHA            string       `json:"sha"`
	AuthorName     string       `json:"author_name"`
	AuthorEmail    string       `json:"author_email"`
	AuthoredAt     time.Time    `json:"authored_at"`
	CommitterName  string       `json:"committer_name"`
	CommitterEmail string       `json:"committer_email"`
	CommittedAt    time.Time    `json:"committed_at"`
	Message        string       `json:"message"`
	ParentSHAs     []string     `json:"parent_shas"`
	FilesChanged   int          `json:"files_changed"`
	Additions      int          `json:"additions"`
	Deletions      int          `json:"deletions"`
	Files          []CommitFile `json:"files,omitempty"`
}

type CommitFile struct {
	RepoID     int64  `json:"repo_id"`
	CommitSHA  string `json:"commit_sha"`
	FilePath   string `json:"file_path"`
	OldPath    string `json:"old_path"`
	ChangeType string `json:"change_type"`
	Additions  int    `json:"additions"`
	Deletions  int    `json:"deletions"`
	Patch      string `json:"patch,omitempty"`
}

type GitRef struct {
	RepoID    int64  `json:"repo_id"`
	RefName   string `json:"ref_name"`
	TargetSHA string `json:"target_sha"`
	RefType   string `json:"ref_type"`
}

type PRCommit struct {
	RepoID    int64  `json:"repo_id"`
	PRNumber  int    `json:"pr_number"`
	CommitSHA string `json:"commit_sha"`
	Ordinal   int    `json:"ordinal"`
}

type Issue struct {
	RepoID       int64     `json:"repo_id"`
	Number       int       `json:"number"`
	GitHubID     int64     `json:"github_id"`
	State        string    `json:"state"`
	StateReason  string    `json:"state_reason"`
	Title        string    `json:"title"`
	Body         string    `json:"body"`
	AuthorLogin  string    `json:"author_login"`
	Milestone    string    `json:"milestone"`
	Labels       []string  `json:"labels"`
	Assignees    []string  `json:"assignees"`
	CommentCount int       `json:"comment_count"`
	Reactions    string    `json:"reactions"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
	ClosedAt     time.Time `json:"closed_at"`
}

type PullRequest struct {
	RepoID        int64     `json:"repo_id"`
	Number        int       `json:"number"`
	GitHubID      int64     `json:"github_id"`
	State         string    `json:"state"`
	IsDraft       bool      `json:"is_draft"`
	Merged        bool      `json:"merged"`
	Title         string    `json:"title"`
	Body          string    `json:"body"`
	AuthorLogin   string    `json:"author_login"`
	MergedByLogin string    `json:"merged_by_login"`
	BaseRef       string    `json:"base_ref"`
	BaseSHA       string    `json:"base_sha"`
	HeadRef       string    `json:"head_ref"`
	HeadSHA       string    `json:"head_sha"`
	MergeSHA      string    `json:"merge_commit_sha"`
	Additions     int       `json:"additions"`
	Deletions     int       `json:"deletions"`
	ChangedFiles  int       `json:"changed_files"`
	CommentCount  int       `json:"comment_count"`
	ReviewCount   int       `json:"review_count"`
	Labels        []string  `json:"labels"`
	Assignees     []string  `json:"assignees"`
	ClosingIssues []int     `json:"closing_issues"`
	Reactions     string    `json:"reactions"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	ClosedAt      time.Time `json:"closed_at"`
	MergedAt      time.Time `json:"merged_at"`
}

type IssueComment struct {
	RepoID      int64     `json:"repo_id"`
	CommentID   int64     `json:"comment_id"`
	ItemNumber  int       `json:"item_number"`
	IsPR        bool      `json:"is_pr"`
	AuthorLogin string    `json:"author_login"`
	Body        string    `json:"body"`
	Reactions   string    `json:"reactions"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type PRReview struct {
	RepoID      int64     `json:"repo_id"`
	ReviewID    int64     `json:"review_id"`
	PRNumber    int       `json:"pr_number"`
	AuthorLogin string    `json:"author_login"`
	State       string    `json:"state"`
	CommitSHA   string    `json:"commit_sha"`
	Body        string    `json:"body"`
	SubmittedAt time.Time `json:"submitted_at"`
}

type PRReviewComment struct {
	RepoID          int64     `json:"repo_id"`
	CommentID       int64     `json:"comment_id"`
	PRNumber        int       `json:"pr_number"`
	ReviewID        int64     `json:"review_id"`
	InReplyToID     int64     `json:"in_reply_to_id"`
	AuthorLogin     string    `json:"author_login"`
	Path            string    `json:"path"`
	CommitSHA       string    `json:"commit_sha"`
	OriginalSHA     string    `json:"original_commit_sha"`
	DiffHunk        string    `json:"diff_hunk"`
	Line            int       `json:"line"`
	StartLine       int       `json:"start_line"`
	Side            string    `json:"side"`
	Body            string    `json:"body"`
	Reactions       string    `json:"reactions"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type TimelineEvent struct {
	RepoID     int64     `json:"repo_id"`
	EventKey   string    `json:"event_key"`
	ItemNumber int       `json:"item_number"`
	IsPR       bool      `json:"is_pr"`
	EventType  string    `json:"event_type"`
	ActorLogin string    `json:"actor_login"`
	Payload    string    `json:"payload_json"`
	CreatedAt  time.Time `json:"created_at"`
}

type SyncCheckpoint struct {
	RepoID     int64     `json:"repo_id"`
	StreamName string    `json:"stream_name"`
	Cursor     string    `json:"cursor"`
	Watermark  string    `json:"watermark"`
	ItemsDone  int64     `json:"items_done"`
	Completed  bool      `json:"completed"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type RuntimeCommand struct {
	ID        int64     `json:"id"`
	Command   string    `json:"command"`
	Arg       string    `json:"arg"`
	CreatedAt time.Time `json:"created_at"`
}
