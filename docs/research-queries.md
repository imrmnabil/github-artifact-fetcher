# Research Query Recipes (SQL, DuckDB & Python)

Once `ghfetcher` finishes (or even *while* it is still running, thanks to SQLite WAL mode), you can query the dataset directly in SQLite or export to Parquet for DuckDB, Polars, Pandas, or R.

---

## 1. PR Review Turnaround Time & Code Review Depth (DuckDB + Parquet)

```python
import duckdb

con = duckdb.connect()
df = con.execute("""
    WITH first_review AS (
        SELECT repo_id, pr_number, MIN(CAST(submitted_at AS TIMESTAMP)) AS first_reviewed_at,
               COUNT(*) AS total_reviews
        FROM 'study_parquet/pr_reviews.parquet'
        WHERE submitted_at != ''
        GROUP BY repo_id, pr_number
    ),
    inline_comments AS (
        SELECT repo_id, pr_number, COUNT(*) AS diff_comments
        FROM 'study_parquet/pr_review_comments.parquet'
        GROUP BY repo_id, pr_number
    )
    SELECT
        r.full_name,
        COUNT(*) AS merged_prs,
        MEDIAN(DATE_DIFF('hour', CAST(pr.created_at AS TIMESTAMP), fr.first_reviewed_at)) AS median_hrs_to_first_review,
        AVG(COALESCE(ic.diff_comments, 0)) AS avg_inline_comments,
        MEDIAN(pr.additions + pr.deletions) AS median_loc_churn
    FROM 'study_parquet/pull_requests.parquet' pr
    JOIN 'study_parquet/repositories.parquet' r ON r.id = pr.repo_id
    LEFT JOIN first_review fr ON fr.repo_id = pr.repo_id AND fr.pr_number = pr.number
    LEFT JOIN inline_comments ic ON ic.repo_id = pr.repo_id AND ic.pr_number = pr.number
    WHERE pr.merged = 1
    GROUP BY r.full_name
""").df()
print(df)
```

---

## 2. Join PRs to Their Local Git Commits & File Churn (Zero-API `pr_commits`)

```sql
SELECT
    pr.number,
    pr.title,
    pr.author_login,
    COUNT(DISTINCT pc.commit_sha) AS commit_count,
    SUM(c.additions) AS git_additions,
    SUM(c.deletions) AS git_deletions
FROM pull_requests pr
JOIN pr_commits pc
  ON pc.repo_id = pr.repo_id AND pc.pr_number = pr.number
JOIN commits c
  ON c.repo_id = pc.repo_id AND c.sha = pc.commit_sha
WHERE pr.merged = 1
GROUP BY pr.repo_id, pr.number
ORDER BY git_additions DESC
LIMIT 20;
```

---

## 3. Hotspot Files: Most Frequently Churned & Reviewed Files

```sql
SELECT
    cf.file_path,
    COUNT(DISTINCT cf.commit_sha) AS commits_touching,
    SUM(cf.additions + cf.deletions) AS total_churn,
    COUNT(DISTINCT rc.comment_id) AS inline_review_comments
FROM commit_files cf
LEFT JOIN pr_review_comments rc
  ON rc.repo_id = cf.repo_id AND rc.path = cf.file_path
GROUP BY cf.file_path
ORDER BY inline_review_comments DESC, total_churn DESC
LIMIT 25;
```

---

## 4. Force-Push & Rework Frequency on Pull Requests (`timeline_events`)

```sql
SELECT
    r.full_name,
    COUNT(DISTINCT te.item_number) AS prs_with_force_push,
    COUNT(*) AS total_force_push_events
FROM timeline_events te
JOIN repositories r ON r.id = te.repo_id
WHERE te.is_pr = 1
  AND te.event_type = 'HeadRefForcePushedEvent'
GROUP BY r.full_name;
```
