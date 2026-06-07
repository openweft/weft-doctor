package output

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v68/github"

	"github.com/openweft/weft-doctor/classify"
)

// GitHubSink maintains ONE long-lived issue per target repo : the
// "Cluster Diagnosis Dashboard". On each Publish call the issue's
// body is regenerated with the most recent N diagnoses, deduped by
// PatternHash so a recurring pattern is updated in place rather
// than appended N times.
//
// Why a single dashboard issue (vs one issue per diagnosis) : avoids
// notification spam when a single bug fires 50 patterns over a week.
// Same UX as Renovate's Dependency Dashboard.
type GitHubSink struct {
	client       *github.Client
	owner        string
	repo         string
	title        string
	maxRecent    int
	log          *slog.Logger

	mu        sync.Mutex
	state     map[string]classify.Diagnosis // pattern_hash → latest
	issueNum  int                           // 0 = not yet looked up
}

// GitHubOptions configures the sink.
type GitHubOptions struct {
	// Token is a Personal Access Token with repo:issues scope. The
	// CLI reads it from $WEFT_DOCTOR_GH_PAT — never store on disk.
	Token string
	// Owner / Repo identify the target repository.
	Owner string
	Repo  string
	// Title is the dashboard issue title (matches an existing
	// issue if found, else opens new). Default :
	// "Cluster Diagnosis Dashboard".
	Title string
	// MaxRecent caps the number of diagnoses kept in the body.
	// Older ones drop off (LRU by LastSeen). Default 20.
	MaxRecent int
	// Logger is the slog logger. Default slog.Default().
	Logger *slog.Logger
}

// NewGitHubSink builds a sink. Network calls happen on the first
// Publish — the constructor itself just wires the client.
func NewGitHubSink(opts GitHubOptions) (*GitHubSink, error) {
	if opts.Token == "" {
		return nil, fmt.Errorf("output/github: token required (set WEFT_DOCTOR_GH_PAT)")
	}
	if opts.Owner == "" || opts.Repo == "" {
		return nil, fmt.Errorf("output/github: owner + repo required")
	}
	if opts.Title == "" {
		opts.Title = "Cluster Diagnosis Dashboard"
	}
	if opts.MaxRecent == 0 {
		opts.MaxRecent = 20
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	client := github.NewClient(nil).WithAuthToken(opts.Token)
	return &GitHubSink{
		client:    client,
		owner:     opts.Owner,
		repo:      opts.Repo,
		title:     opts.Title,
		maxRecent: opts.MaxRecent,
		log:       opts.Logger,
		state:     map[string]classify.Diagnosis{},
	}, nil
}

func (s *GitHubSink) Name() string { return "github" }

// Publish merges incoming diagnoses into the in-memory state (dedup
// by PatternHash, keep the latest) then PATCHes the dashboard issue
// body. On first call it locates or opens the issue.
func (s *GitHubSink) Publish(ctx context.Context, diags []classify.Diagnosis) error {
	s.mu.Lock()
	for _, d := range diags {
		s.state[d.PatternHash] = d
	}
	body := s.renderBody()
	s.mu.Unlock()

	if s.issueNum == 0 {
		num, err := s.findOrCreateIssue(ctx)
		if err != nil {
			return err
		}
		s.issueNum = num
	}
	_, _, err := s.client.Issues.Edit(ctx, s.owner, s.repo, s.issueNum, &github.IssueRequest{
		Body: github.Ptr(body),
	})
	if err != nil {
		return fmt.Errorf("edit issue #%d: %w", s.issueNum, err)
	}
	s.log.Info("dashboard updated", "owner", s.owner, "repo", s.repo, "issue", s.issueNum, "patterns", len(s.state))
	return nil
}

// findOrCreateIssue searches the repo's open issues for one with
// our title ; opens a new one if not found.
func (s *GitHubSink) findOrCreateIssue(ctx context.Context) (int, error) {
	listOpts := &github.IssueListByRepoOptions{
		State:       "open",
		Creator:     "", // any creator — we may match a manually-renamed issue
		ListOptions: github.ListOptions{PerPage: 100},
	}
	for {
		issues, resp, err := s.client.Issues.ListByRepo(ctx, s.owner, s.repo, listOpts)
		if err != nil {
			return 0, fmt.Errorf("list issues: %w", err)
		}
		for _, iss := range issues {
			if iss.GetTitle() == s.title && iss.PullRequestLinks == nil {
				return iss.GetNumber(), nil
			}
		}
		if resp.NextPage == 0 {
			break
		}
		listOpts.Page = resp.NextPage
	}
	// Open a new one.
	body := s.renderBody()
	iss, _, err := s.client.Issues.Create(ctx, s.owner, s.repo, &github.IssueRequest{
		Title:  github.Ptr(s.title),
		Body:   github.Ptr(body),
		Labels: &[]string{"weft-doctor", "diagnosis"},
	})
	if err != nil {
		return 0, fmt.Errorf("create issue: %w", err)
	}
	s.log.Info("dashboard issue opened", "owner", s.owner, "repo", s.repo, "issue", iss.GetNumber())
	return iss.GetNumber(), nil
}

// renderBody composes the Markdown body. Called under lock. The body
// has 3 sections : header (timestamp + brief), active diagnoses
// (sorted by severity desc), and a footer (link + format note).
func (s *GitHubSink) renderBody() string {
	// Materialise to slice, sort by severity then occurrences desc.
	type entry struct {
		d classify.Diagnosis
	}
	all := make([]entry, 0, len(s.state))
	for _, d := range s.state {
		all = append(all, entry{d: d})
	}
	sort.Slice(all, func(i, j int) bool {
		si, sj := severityRank(all[i].d.Severity), severityRank(all[j].d.Severity)
		if si != sj {
			return si < sj
		}
		return all[i].d.Occurrences > all[j].d.Occurrences
	})
	if len(all) > s.maxRecent {
		all = all[:s.maxRecent]
	}

	var b strings.Builder
	fmt.Fprintf(&b, "# Cluster Diagnosis Dashboard\n\n")
	fmt.Fprintf(&b, "Auto-maintained by [`weft-doctor`](https://github.com/openweft/weft-doctor). "+
		"Last update : %s UTC. Tracking %d active patterns.\n\n",
		time.Now().UTC().Format(time.RFC3339), len(s.state))
	if len(all) == 0 {
		b.WriteString("_No active diagnoses. All quiet._\n")
		return b.String()
	}
	for _, e := range all {
		writeEntry(&b, e.d)
	}
	b.WriteString("\n---\n\n")
	b.WriteString("Entries are auto-deduplicated by pattern hash and sorted by severity then frequency. ")
	b.WriteString("Older patterns drop off after the configured cap. ")
	b.WriteString("Mark an entry resolved by closing this issue ; weft-doctor will reopen with fresh state on the next burst.\n")
	return b.String()
}

func writeEntry(b *strings.Builder, d classify.Diagnosis) {
	fmt.Fprintf(b, "## %s %s\n\n", severityBadge(d.Severity), d.Title)
	if d.RootCause != "" {
		fmt.Fprintf(b, "**Root cause** : %s\n\n", d.RootCause)
	}
	if d.SuggestedAction != "" {
		fmt.Fprintf(b, "**Suggested action** : %s\n\n", d.SuggestedAction)
	}
	if d.FileLocation != "" {
		fmt.Fprintf(b, "**Likely location** : `%s`\n\n", d.FileLocation)
	}
	fmt.Fprintf(b, "**Pattern** : `%s` · **Occurrences** : %d · ", d.PatternHash, d.Occurrences)
	if !d.FirstSeen.IsZero() {
		fmt.Fprintf(b, "**First seen** : %s · ", d.FirstSeen.UTC().Format(time.RFC3339))
	}
	if !d.LastSeen.IsZero() {
		fmt.Fprintf(b, "**Last seen** : %s", d.LastSeen.UTC().Format(time.RFC3339))
	}
	b.WriteString("\n\n")
	if len(d.Examples) > 0 {
		b.WriteString("<details><summary>Example events</summary>\n\n")
		b.WriteString("```\n")
		for i, ex := range d.Examples {
			if i > 0 {
				b.WriteString("\n")
			}
			fmt.Fprintf(b, "[%s] %s", ex.Level, ex.Msg)
			if ex.Source != "" {
				fmt.Fprintf(b, " (subject: %s)", ex.Source)
			}
			b.WriteString("\n")
		}
		b.WriteString("```\n\n")
		b.WriteString("</details>\n\n")
	}
}

func severityRank(s classify.Severity) int {
	switch s {
	case classify.SeverityCritical:
		return 0
	case classify.SeverityHigh:
		return 1
	case classify.SeverityMedium:
		return 2
	case classify.SeverityLow:
		return 3
	}
	return 4
}

func severityBadge(s classify.Severity) string {
	switch s {
	case classify.SeverityCritical:
		return "🔴 [critical]"
	case classify.SeverityHigh:
		return "🟠 [high]"
	case classify.SeverityMedium:
		return "🟡 [medium]"
	case classify.SeverityLow:
		return "🟢 [low]"
	}
	return "[unknown]"
}
