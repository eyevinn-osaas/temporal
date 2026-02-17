package flakereport

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/headers"
)

const (
	minFlakyFailures    = 3
	defaultMaxLinks     = 3
	defaultLookbackDays = 7
	defaultWorkflowID   = 80591745
	defaultRepository   = "temporalio/temporal"
	defaultBranch       = "main"
	defaultConcurrency  = 20
)

// NewCliApp instantiates a new instance of the CLI application
func NewCliApp() *cli.App {
	app := cli.NewApp()
	app.Name = "flakesreport"
	app.Usage = "Generate flaky test reports"
	app.Version = headers.ServerVersion

	app.Commands = []*cli.Command{
		{
			Name:  "generate",
			Usage: "Generate flaky test reports from GitHub Actions",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "repository",
					Value: defaultRepository,
					Usage: "GitHub repository (owner/repo)",
				},
				&cli.StringFlag{
					Name:  "branch",
					Value: defaultBranch,
					Usage: "Git branch to analyze",
				},
				&cli.IntFlag{
					Name:  "days",
					Value: defaultLookbackDays,
					Usage: "Number of days to look back",
				},
				&cli.Int64Flag{
					Name:  "workflow-id",
					Value: defaultWorkflowID,
					Usage: "GitHub Actions workflow ID",
				},
				&cli.IntFlag{
					Name:  "max-links",
					Value: defaultMaxLinks,
					Usage: "Maximum failure links per test",
				},
				&cli.IntFlag{
					Name:  "concurrency",
					Value: defaultConcurrency,
					Usage: "Number of parallel workers for artifact processing",
				},
				&cli.StringFlag{
					Name:  "output-dir",
					Value: "out",
					Usage: "Output directory for report files",
				},
				&cli.BoolFlag{
					Name:  "github-summary",
					Usage: "Generate GitHub Actions step summary",
				},
				&cli.StringFlag{
					Name:    "slack-webhook",
					Usage:   "Slack webhook URL for notifications",
					EnvVars: []string{"SLACK_WEBHOOK"},
				},
				&cli.StringFlag{
					Name:  "run-id",
					Usage: "GitHub Actions run ID (for links)",
				},
				&cli.StringFlag{
					Name:  "ref-name",
					Usage: "Git ref name (for failure messages)",
				},
				&cli.StringFlag{
					Name:  "sha",
					Usage: "Git commit SHA (for failure messages)",
				},
			},
			Action: runGenerateCommand,
		},
	}

	return app
}

func runGenerateCommand(c *cli.Context) error {
	// Extract parameters
	repo := c.String("repository")
	branch := c.String("branch")
	days := c.Int("days")
	workflowID := c.Int64("workflow-id")
	maxLinks := c.Int("max-links")
	concurrency := c.Int("concurrency")
	outputDir := c.String("output-dir")
	enableGitHubSummary := c.Bool("github-summary")
	slackWebhook := c.String("slack-webhook")
	runID := c.String("run-id")
	refName := c.String("ref-name")
	sha := c.String("sha")

	fmt.Println("Starting flaky test report generation...")
	fmt.Printf("Repository: %s\n", repo)
	fmt.Printf("Branch: %s\n", branch)
	fmt.Printf("Lookback days: %d\n", days)
	fmt.Printf("Workflow ID: %d\n", workflowID)
	fmt.Printf("Parallel workers: %d\n", concurrency)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Step 1: Fetch workflow runs with pagination
	fmt.Println("\n=== Fetching workflow runs ===")
	runs, err := fetchWorkflowRuns(ctx, repo, workflowID, branch, days)
	if err != nil {
		sendFailureNotification(slackWebhook, runID, refName, sha, repo, err)
		return fmt.Errorf("failed to fetch workflow runs: %w", err)
	}

	if len(runs) == 0 {
		fmt.Println("No workflow runs found in the specified time range")
		return nil
	}

	// Step 2: Create temp directory for downloads
	tempDir, err := os.MkdirTemp("", "flakereport-*")
	if err != nil {
		sendFailureNotification(slackWebhook, runID, refName, sha, repo, err)
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			fmt.Printf("Warning: Failed to remove temp directory: %v\n", err)
		}
	}()

	// Step 3: Collect all artifact jobs
	fmt.Println("\n=== Collecting artifacts ===")
	var jobs []ArtifactJob
	totalArtifacts := 0

	for i, run := range runs {
		// Fetch artifacts for this run
		artifacts, err := fetchRunArtifacts(ctx, repo, run.ID)
		if err != nil {
			fmt.Printf("Warning: Failed to fetch artifacts for run %d: %v\n", run.ID, err)
			continue
		}

		if len(artifacts) == 0 {
			fmt.Printf("Run %d/%d (ID: %d): No test artifacts found\n", i+1, len(runs), run.ID)
			continue
		}

		fmt.Printf("Run %d/%d (ID: %d): Found %d artifacts\n", i+1, len(runs), run.ID, len(artifacts))

		// Create jobs for each artifact
		for _, artifact := range artifacts {
			totalArtifacts++
			jobs = append(jobs, ArtifactJob{
				Repo:           repo,
				RunID:          run.ID,
				Artifact:       artifact,
				TempDir:        tempDir,
				RunNumber:      i + 1,
				TotalRuns:      len(runs),
				ArtifactNum:    totalArtifacts,
				TotalArtifacts: 0, // Will be updated below
			})
		}
	}

	// Update total artifact count in all jobs
	for i := range jobs {
		jobs[i].TotalArtifacts = totalArtifacts
	}

	fmt.Printf("\nTotal artifacts to process: %d\n", totalArtifacts)

	// Step 4: Process artifacts in parallel
	fmt.Println("\n=== Processing artifacts in parallel ===")
	allFailures, allTestRuns, processedArtifacts := processArtifactsParallel(ctx, jobs, concurrency)

	fmt.Println("\n=== Processing Results ===")
	fmt.Printf("Total test runs: %d\n", len(allTestRuns))
	fmt.Printf("Total test failures found: %d\n", len(allFailures))
	fmt.Printf("Processed artifacts: %d\n", processedArtifacts)

	// Step 5: Count test runs by name for failure rate calculation
	testRunCounts := countTestRuns(allTestRuns)

	// Step 6: Group failures by test name
	grouped := groupFailuresByTest(allFailures)
	fmt.Printf("Unique tests with failures: %d\n", len(grouped))

	// Step 7: Classify failures
	flakyMap, timeoutMap, crashMap := classifyFailures(grouped)
	fmt.Printf("Flaky tests (>%d failures): %d\n", minFlakyFailures, len(flakyMap))
	fmt.Printf("Timeout tests: %d\n", len(timeoutMap))
	fmt.Printf("Crash tests: %d\n", len(crashMap))

	// Step 8: Convert to reports with failure rates and sort
	flakyReports := convertToReports(flakyMap, testRunCounts, repo, maxLinks)
	timeoutReports := convertToReports(timeoutMap, testRunCounts, repo, maxLinks)
	crashReports := convertToReports(crashMap, testRunCounts, repo, maxLinks)

	// Step 9: Calculate overall failure rate
	overallFailureRate := 0.0
	totalTestRuns := len(allTestRuns)
	if totalTestRuns > 0 {
		overallFailureRate = (float64(len(allFailures)) / float64(totalTestRuns)) * 1000.0
	}
	fmt.Printf("Overall failure rate: %.2f per 1000 test runs\n", overallFailureRate)

	// Step 10: Build summary
	summary := &ReportSummary{
		FlakyTests:         flakyReports,
		Timeouts:           timeoutReports,
		Crashes:            crashReports,
		TotalFailures:      len(allFailures),
		TotalTestRuns:      totalTestRuns,
		OverallFailureRate: overallFailureRate,
		TotalFlakyCount:    len(flakyReports),
	}

	// Step 11: Write output files
	fmt.Println("\n=== Writing report files ===")
	if err := writeReportFiles(outputDir, summary, maxLinks); err != nil {
		sendFailureNotification(slackWebhook, runID, refName, sha, repo, err)
		return fmt.Errorf("failed to write report files: %w", err)
	}

	// Step 12: Write GitHub Actions summary
	if enableGitHubSummary {
		fmt.Println("\n=== Writing GitHub Actions summary ===")
		summaryContent := generateGitHubSummary(summary, runID, maxLinks)
		if err := writeGitHubSummary(summaryContent); err != nil {
			fmt.Printf("Warning: Failed to write GitHub summary: %v\n", err)
		}
	}

	// Step 13: Send Slack notification
	if slackWebhook != "" {
		fmt.Println("\n=== Sending Slack notification ===")
		_, flakySlack, _ := generateFlakyReport(summary.FlakyTests, maxLinks)
		message := buildSuccessMessage(summary, flakySlack, runID, repo, days)
		if err := sendSlackMessage(slackWebhook, message); err != nil {
			fmt.Printf("Warning: Failed to send Slack notification: %v\n", err)
		}
	}

	fmt.Println("\n=== Report generation complete! ===")
	return nil
}

func sendFailureNotification(webhookURL, runID, refName, sha, repo string, err error) {
	if webhookURL == "" {
		return
	}

	fmt.Println("Sending failure notification to Slack...")
	message := buildFailureMessage(runID, refName, sha, repo)
	if sendErr := sendSlackMessage(webhookURL, message); sendErr != nil {
		fmt.Printf("Warning: Failed to send failure notification: %v\n", sendErr)
	}
}
