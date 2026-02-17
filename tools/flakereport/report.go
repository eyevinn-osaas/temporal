package flakereport

import (
	"fmt"
	"strings"
)

// generateFlakyReport creates flaky test report (>3 failures)
// Markdown shows ALL tests, Slack text limited to top 10
// Returns: markdown content, slack plain text, total count
func generateFlakyReport(reports []TestReport, maxLinks int) (markdown, slackText string, totalCount int) {
	if len(reports) == 0 {
		return "", "", 0
	}

	totalCount = len(reports)

	var mdLines []string
	var slackLines []string

	// Markdown: show ALL tests
	for i := 0; i < totalCount; i++ {
		report := reports[i]
		mdLine := formatTestReportMarkdown(report.TestName, report.FailureCount, report.TotalRuns, report.FailureRate, report.GitHubURLs, maxLinks)
		mdLines = append(mdLines, mdLine)
	}

	// Slack: limit to top 10 to keep message concise
	slackDisplayCount := totalCount
	if slackDisplayCount > 10 {
		slackDisplayCount = 10
	}

	for i := 0; i < slackDisplayCount; i++ {
		report := reports[i]
		slackLine := formatTestReportPlainText(report.TestName, report.FailureCount, report.TotalRuns, report.FailureRate)
		slackLines = append(slackLines, slackLine)
	}

	markdown = strings.Join(mdLines, "\n")
	slackText = strings.Join(slackLines, "\n")

	return markdown, slackText, totalCount
}

// generateTimeoutReport creates timeout report (tests ending with "(timeout)")
func generateTimeoutReport(reports []TestReport, maxLinks int) string {
	if len(reports) == 0 {
		return ""
	}

	var lines []string
	for _, report := range reports {
		line := formatTestReportMarkdown(report.TestName, report.FailureCount, report.TotalRuns, report.FailureRate, report.GitHubURLs, maxLinks)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

// generateCrashReport creates crash report (tests containing "crash")
func generateCrashReport(reports []TestReport, maxLinks int) string {
	if len(reports) == 0 {
		return ""
	}

	var lines []string
	for _, report := range reports {
		line := formatTestReportMarkdown(report.TestName, report.FailureCount, report.TotalRuns, report.FailureRate, report.GitHubURLs, maxLinks)
		lines = append(lines, line)
	}

	return strings.Join(lines, "\n")
}

// formatTestReportMarkdown formats a single test report line with markdown links
// Format: * {count} failures / {total} runs ({rate}/1000): `{test_name}` [1](url1) [2](url2) [3](url3)
func formatTestReportMarkdown(testName string, failureCount, totalRuns int, failureRate float64, urls []string, maxLinks int) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("* %d failures / %d runs (%.1f/1000): `%s`",
		failureCount, totalRuns, failureRate, testName))

	// Add numbered links
	linkCount := len(urls)
	if linkCount > maxLinks {
		linkCount = maxLinks
	}

	for i := 0; i < linkCount; i++ {
		sb.WriteString(fmt.Sprintf(" [%d](%s)", i+1, urls[i]))
	}

	return sb.String()
}

// formatTestReportPlainText formats a single test report line without links (for Slack)
// Format: • {count} failures / {total} runs ({rate}/1000): `{test_name}`
func formatTestReportPlainText(testName string, failureCount, totalRuns int, failureRate float64) string {
	return fmt.Sprintf("• %d failures / %d runs (%.1f/1000): `%s`",
		failureCount, totalRuns, failureRate, testName)
}
