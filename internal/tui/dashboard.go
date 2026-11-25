package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"ghfetcher/internal/pipeline"
	"ghfetcher/internal/store"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#00E5FF"))

	badgeRunStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#000000")).
			Background(lipgloss.Color("#00E676")).
			Padding(0, 1)

	badgePauseStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#000000")).
			Background(lipgloss.Color("#FFB300")).
			Padding(0, 1)

	badgeDoneStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#000000")).
			Background(lipgloss.Color("#29B6F6")).
			Padding(0, 1)

	savedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#69F0AE"))

	dimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#78909C"))

	warnStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFD54F"))

	errStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FF5252"))

	headerRowStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#B0BEC5")).
			BorderStyle(lipgloss.NormalBorder()).
			BorderBottom(true).
			BorderForeground(lipgloss.Color("#37474F"))

	selectedRowStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(lipgloss.Color("#263238"))

	boxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#37474F")).
			Padding(0, 1)

	barFullStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#00E676"))
	barWarnStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFB300"))
	barEmptyStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#37474F"))
)

// Controller abstracts either an in-process Orchestrator or a remote SQLite-attached background run.
type Controller interface {
	FetchSnapshot() (pipeline.TelemetrySnapshot, error)
	TogglePause()
	AdjustWorkers(delta int)
	ReloadTokens()
	Stop()
	IsRemote() bool
}

// LocalController drives an in-process Orchestrator directly.
type LocalController struct {
	Orch *pipeline.Orchestrator
}

func (l *LocalController) FetchSnapshot() (pipeline.TelemetrySnapshot, error) {
	return l.Orch.Snapshot(), nil
}
func (l *LocalController) TogglePause()            { l.Orch.TogglePause() }
func (l *LocalController) AdjustWorkers(delta int) { l.Orch.AdjustWorkers(delta) }
func (l *LocalController) ReloadTokens()           { l.Orch.ReloadTokens() }
func (l *LocalController) Stop()                   { l.Orch.Stop() }
func (l *LocalController) IsRemote() bool          { return false }

// RemoteAttachController attaches to a headless `--no-tui` background job via SQLite WAL telemetry & commands.
type RemoteAttachController struct {
	Store *store.Store
}

func (r *RemoteAttachController) FetchSnapshot() (pipeline.TelemetrySnapshot, error) {
	_, _, payload, err := r.Store.ReadTelemetry(context.Background())
	if err != nil {
		return pipeline.TelemetrySnapshot{}, err
	}
	if payload == "" {
		return pipeline.TelemetrySnapshot{}, fmt.Errorf("no telemetry found in %s yet", r.Store.Path())
	}
	var snap pipeline.TelemetrySnapshot
	if err := json.Unmarshal([]byte(payload), &snap); err != nil {
		return pipeline.TelemetrySnapshot{}, err
	}
	return snap, nil
}
func (r *RemoteAttachController) TogglePause() {
	_ = r.Store.EnqueueCommand(context.Background(), "toggle-pause", "")
}
func (r *RemoteAttachController) AdjustWorkers(delta int) {
	cmd := "workers-inc"
	if delta < 0 {
		cmd = "workers-dec"
	}
	_ = r.Store.EnqueueCommand(context.Background(), cmd, "")
}
func (r *RemoteAttachController) ReloadTokens() {
	_ = r.Store.EnqueueCommand(context.Background(), "reload-tokens", "")
}
func (r *RemoteAttachController) Stop() {
	_ = r.Store.EnqueueCommand(context.Background(), "stop", "")
}
func (r *RemoteAttachController) IsRemote() bool { return true }

type tickMsg time.Time
type doneMsg struct{ err error }

// Model implements the Bubble Tea htop-style interactive TUI.
type Model struct {
	ctrl       Controller
	doneCh     <-chan error
	snap       pipeline.TelemetrySnapshot
	snapErr    error
	width      int
	height     int
	cursor     int
	showLogs   bool
	quitting   bool
	runErr     error
}

// RunDashboard launches the interactive htop TUI and blocks until completion or user quit.
func RunDashboard(ctrl Controller, doneCh <-chan error) error {
	m := Model{
		ctrl:   ctrl,
		doneCh: doneCh,
		width:  110,
		height: 30,
	}
	if s, err := ctrl.FetchSnapshot(); err == nil {
		m.snap = s
	}

	prog := tea.NewProgram(m, tea.WithAltScreen())
	finalModel, err := prog.Run()
	if err != nil {
		return err
	}
	if fm, ok := finalModel.(Model); ok && fm.runErr != nil {
		return fm.runErr
	}
	return nil
}

func (m Model) Init() tea.Cmd {
	cmds := []tea.Cmd{tickCmd()}
	if m.doneCh != nil {
		cmds = append(cmds, waitForDoneCmd(m.doneCh))
	}
	return tea.Batch(cmds...)
}

func tickCmd() tea.Cmd {
	return tea.Tick(200*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func waitForDoneCmd(ch <-chan error) tea.Cmd {
	return func() tea.Msg {
		err := <-ch
		return doneMsg{err: err}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tickMsg:
		if s, err := m.ctrl.FetchSnapshot(); err == nil {
			m.snap = s
			m.snapErr = nil
			if m.cursor >= len(m.snap.Repos) && len(m.snap.Repos) > 0 {
				m.cursor = len(m.snap.Repos) - 1
			}
		} else {
			m.snapErr = err
		}
		if m.ctrl.IsRemote() && m.snap.Finished {
			return m, tea.Quit
		}
		return m, tickCmd()

	case doneMsg:
		m.runErr = msg.err
		if s, err := m.ctrl.FetchSnapshot(); err == nil {
			m.snap = s
		}
		return m, tea.Quit

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			if m.quitting {
				// Second press exits immediately without waiting
				return m, tea.Quit
			}
			m.quitting = true
			if !m.ctrl.IsRemote() {
				m.ctrl.Stop()
				if m.doneCh != nil {
					return m, tea.Tick(4*time.Second, func(time.Time) tea.Msg {
						return doneMsg{err: nil}
					})
				}
			}
			return m, tea.Quit

		case "X":
			// Remote stop key when attached
			if m.ctrl.IsRemote() {
				m.ctrl.Stop()
			}
			return m, nil

		case " ", "p":
			m.ctrl.TogglePause()
			return m, nil

		case "+", "=":
			m.ctrl.AdjustWorkers(1)
			return m, nil

		case "-", "_":
			m.ctrl.AdjustWorkers(-1)
			return m, nil

		case "t":
			m.ctrl.ReloadTokens()
			return m, nil

		case "l", "enter", "tab":
			m.showLogs = !m.showLogs
			return m, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
			return m, nil

		case "down", "j":
			if m.cursor+1 < len(m.snap.Repos) {
				m.cursor++
			}
			return m, nil
		}
	}
	return m, nil
}

func (m Model) View() string {
	if m.snapErr != nil && len(m.snap.Repos) == 0 {
		return fmt.Sprintf("\n  Waiting for telemetry: %v\n  Press q to detach.\n", m.snapErr)
	}

	w := m.width - 4
	if w < 76 {
		w = 76
	}

	// 1. Top Status Bar
	statusBadge := badgeRunStyle.Render("RUNNING")
	if m.quitting {
		statusBadge = badgePauseStyle.Render("CHECKPOINTING...")
	} else if m.snap.Finished {
		statusBadge = badgeDoneStyle.Render("COMPLETED")
	} else if m.snap.Paused {
		statusBadge = badgePauseStyle.Render("PAUSED")
	}

	modeTag := ""
	if m.ctrl.IsRemote() {
		modeTag = warnStyle.Render(" [ATTACHED PID " + fmt.Sprintf("%d", m.snap.PID) + "]")
	}

	topLine := fmt.Sprintf(
		"%s %s%s  DB: %s (%s)  %s  Total ETA: %s",
		titleStyle.Render("ghfetcher"),
		statusBadge,
		modeTag,
		m.snap.DBPath,
		fmtBytes(m.snap.DBSizeBytes),
		savedStyle.Render(fmt.Sprintf("Saved: ~%s API calls", fmtInt(m.snap.TotalAPISaved))),
		fmtDurationSec(m.snap.TotalETASeconds),
	)
	if m.snap.CooldownNote != "" {
		topLine += "  " + warnStyle.Render("⏳ "+m.snap.CooldownNote)
	}

	// 2. htop Token Pool & Worker Resource Bars
	var gaugeLines []string
	if len(m.snap.Tokens) == 0 {
		gaugeLines = append(gaugeLines, dimStyle.Render("TOKENS   No API tokens configured (Zero-API Local Git Mode)"))
	} else {
		maxTokShow := len(m.snap.Tokens)
		if maxTokShow > 4 {
			maxTokShow = 4
		}
		for i := 0; i < maxTokShow; i++ {
			tk := m.snap.Tokens[i]
			gqlBar := renderQuotaBar(tk.GraphQL.EffectiveRemaining, tk.GraphQL.Limit, 14, tk.GraphQL.Sleeping)
			restBar := renderQuotaBar(tk.REST.EffectiveRemaining, tk.REST.Limit, 14, tk.REST.Sleeping)

			gqlStatus := fmt.Sprintf("%4d/%d (rst %s)", tk.GraphQL.EffectiveRemaining, tk.GraphQL.Limit, fmtUntil(tk.GraphQL.ResetAt))
			if tk.GraphQL.Sleeping {
				gqlStatus = warnStyle.Render(fmt.Sprintf("SLEEP %s (rsv %d)", fmtUntil(tk.GraphQL.ResetAt), tk.Reserve))
			}
			restStatus := fmt.Sprintf("%4d/%d", tk.REST.EffectiveRemaining, tk.REST.Limit)
			if tk.REST.Sleeping {
				restStatus = warnStyle.Render("SLEEP " + fmtUntil(tk.REST.ResetAt))
			}

			gaugeLines = append(gaugeLines, fmt.Sprintf(
				"TOK #%d %-11s GQL %s %-23s REST %s %s",
				tk.ID, "("+tk.Masked+")", gqlBar, gqlStatus, restBar, restStatus,
			))
		}
		if len(m.snap.Tokens) > maxTokShow {
			gaugeLines = append(gaugeLines, dimStyle.Render(fmt.Sprintf("... +%d additional pooled tokens active", len(m.snap.Tokens)-maxTokShow)))
		}
	}

	gitBar := renderSimpleBar(m.snap.GitWorkersBusy, max1(m.snap.GitWorkersMax), 6)
	apiBar := renderSimpleBar(m.snap.APIWorkersBusy, max1(m.snap.APIWorkersMax), 8)
	gaugeLines = append(gaugeLines, fmt.Sprintf(
		"WORKERS  RepoCap:%2d  GIT %s %d/%d  API %s %d/%d  SQLite: %6.0f rows/s  304-ETags: %d  Retries: %d",
		m.snap.TargetWorkers,
		gitBar, m.snap.GitWorkersBusy, m.snap.GitWorkersMax,
		apiBar, m.snap.APIWorkersBusy, m.snap.APIWorkersMax,
		m.snap.RowsPerSec,
		m.snap.APIMetrics.ETagHits304,
		m.snap.APIMetrics.Retries,
	))

	headerPanel := boxStyle.Width(w).Render(topLine + "\n" + strings.Join(gaugeLines, "\n"))

	// 3. Main Repository Process Table
	colHdr := fmt.Sprintf(
		"  %-26s %-23s %-19s %-16s %-9s %-8s",
		"REPOSITORY", "STAGE", "PROGRESS", "ARTIFACTS", "RATE", "ETA",
	)
	var tableRows []string
	tableRows = append(tableRows, headerRowStyle.Width(w-2).Render(colHdr))

	availRows := m.height - len(gaugeLines) - 14
	if availRows < 4 {
		availRows = 4
	}
	startIdx := 0
	if m.cursor >= availRows {
		startIdx = m.cursor - availRows + 1
	}
	endIdx := startIdx + availRows
	if endIdx > len(m.snap.Repos) {
		endIdx = len(m.snap.Repos)
	}

	for i := startIdx; i < endIdx; i++ {
		r := m.snap.Repos[i]
		pointer := "  "
		if i == m.cursor {
			pointer = "▶ "
		}
		pBar := renderPctBar(r.ProgressPct, 12, r.State)
		artSummary := fmt.Sprintf("%sk cm / %d pr", fmtCompact(r.CommitsDone), r.PRsDone+r.IssuesDone)
		rateStr := "—"
		if r.ItemsPerSec > 0 && r.State == "active" {
			rateStr = fmt.Sprintf("%.0f/s", r.ItemsPerSec)
		}
		etaStr := "—"
		if r.State == "done" {
			etaStr = "done"
		} else if r.ETASeconds > 0 {
			etaStr = fmtDurationSec(r.ETASeconds)
		}

		rowText := fmt.Sprintf(
			"%s%-26s %-23s %s %4.0f%% %-16s %-9s %-8s",
			pointer,
			padOrTrunc(r.FullName, 26),
			padOrTrunc(r.Stage, 23),
			pBar,
			r.ProgressPct,
			padOrTrunc(artSummary, 16),
			rateStr,
			etaStr,
		)
		if i == m.cursor {
			rowText = selectedRowStyle.Width(w - 2).Render(rowText)
		} else if r.State == "error" {
			rowText = errStyle.Render(rowText)
		}
		tableRows = append(tableRows, rowText)
	}

	tablePanel := boxStyle.Width(w).Render(strings.Join(tableRows, "\n"))

	// 4. Bottom Inspector / Log Pane
	var bottomContent string
	if m.showLogs {
		logs := m.snap.RecentLogs
		if len(logs) > 4 {
			logs = logs[len(logs)-4:]
		}
		if len(logs) == 0 {
			bottomContent = dimStyle.Render("EVENT LOG: (no events yet)")
		} else {
			bottomContent = titleStyle.Render("EVENT LOG:") + "\n" + strings.Join(logs, "\n")
		}
	} else if len(m.snap.Repos) > 0 && m.cursor < len(m.snap.Repos) {
		sel := m.snap.Repos[m.cursor]
		line1 := fmt.Sprintf(
			"%s %s  │  Commits: %s/%s (0 API)  │  Issues: %d/%d  │  PRs: %d/%d  │  Inline Diff Comments: %d  │  Overflow: %d/%d",
			titleStyle.Render("INSPECTOR:"),
			sel.FullName,
			fmtInt(int64(sel.CommitsDone)), fmtInt(int64(sel.CommitsTotal)),
			sel.IssuesDone, sel.IssuesTotal,
			sel.PRsDone, sel.PRsTotal,
			sel.RevCommDone,
			sel.OverflowDone, sel.OverflowQueued,
		)
		line2 := dimStyle.Render(fmt.Sprintf(
			"API Calls Avoided on Repo: ~%s  │  Total SQLite Rows Written: %s",
			fmtInt(sel.APISaved), fmtInt(m.snap.RowsWritten),
		))
		if sel.LastError != "" {
			line2 = errStyle.Render("Last Error: " + padOrTrunc(sel.LastError, w-18))
		}
		bottomContent = line1 + "\n" + line2
	}
	bottomPanel := boxStyle.Width(w).Render(bottomContent)

	// 5. Keybar Footer
	quitHint := "[q] Checkpoint & Quit"
	if m.ctrl.IsRemote() {
		quitHint = "[q] Detach TUI  [X] Stop Remote Job"
	}
	footer := dimStyle.Render(fmt.Sprintf(
		" [Space/p] Pause/Resume  [↑/↓] Select Repo  [Enter/l] Toggle Logs  [+/-] Workers  [t] Reload Tokens  %s",
		quitHint,
	))

	return strings.Join([]string{headerPanel, tablePanel, bottomPanel, footer}, "\n")
}

func renderQuotaBar(rem, limit, width int, sleeping bool) string {
	if limit <= 0 {
		limit = 5000
	}
	ratio := float64(rem) / float64(limit)
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	filled := int(ratio * float64(width))
	style := barFullStyle
	if sleeping || ratio < 0.15 {
		style = barWarnStyle
	}
	return "[" + style.Render(strings.Repeat("█", filled)) + barEmptyStyle.Render(strings.Repeat("░", width-filled)) + "]"
}

func renderSimpleBar(busy, maxW, width int) string {
	ratio := float64(busy) / float64(maxW)
	if ratio > 1 {
		ratio = 1
	}
	filled := int(ratio * float64(width))
	return "[" + barFullStyle.Render(strings.Repeat("█", filled)) + barEmptyStyle.Render(strings.Repeat("░", width-filled)) + "]"
}

func renderPctBar(pct float64, width int, state string) string {
	ratio := pct / 100.0
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	filled := int(ratio * float64(width))
	style := barFullStyle
	if state == "error" {
		style = errStyle
	}
	return "[" + style.Render(strings.Repeat("█", filled)) + barEmptyStyle.Render(strings.Repeat("░", width-filled)) + "]"
}

func padOrTrunc(s string, width int) string {
	runes := []rune(s)
	if len(runes) > width {
		if width <= 2 {
			return string(runes[:width])
		}
		return string(runes[:width-2]) + ".."
	}
	return s + strings.Repeat(" ", width-len(runes))
}

func fmtBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func fmtDurationSec(sec int64) string {
	if sec <= 0 {
		return "00:00:00"
	}
	h := sec / 3600
	m := (sec % 3600) / 60
	s := sec % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func fmtUntil(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := time.Until(t)
	if d <= 0 {
		return "now"
	}
	mins := int(d.Minutes())
	if mins >= 60 {
		return fmt.Sprintf("%dh%02dm", mins/60, mins%60)
	}
	if mins > 0 {
		return fmt.Sprintf("%dm", mins)
	}
	return fmt.Sprintf("%ds", int(d.Seconds()))
}

func fmtInt(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	rem := len(s) % 3
	if rem > 0 {
		out = append(out, s[:rem]...)
		if len(s) > rem {
			out = append(out, ',')
		}
	}
	for i := rem; i < len(s); i += 3 {
		out = append(out, s[i:i+3]...)
		if i+3 < len(s) {
			out = append(out, ',')
		}
	}
	return string(out)
}

func fmtCompact(n int) string {
	if n >= 1000 {
		return fmt.Sprintf("%.1f", float64(n)/1000.0)
	}
	return fmt.Sprintf("%.1f", float64(n)/1000.0)
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
