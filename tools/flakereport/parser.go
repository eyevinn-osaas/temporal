package flakereport

import (
	"encoding/xml"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jstemmer/go-junit-report/v2/junit"
)

var retryRegex = regexp.MustCompile(`\s*\(retry \d+\)$`)

// parseJUnitFile reads and parses a single JUnit XML file
func parseJUnitFile(filePath string) (*junit.Testsuites, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Printf("Warning: Failed to close file %s: %v\n", filePath, err)
		}
	}()

	var testsuites junit.Testsuites
	decoder := xml.NewDecoder(file)
	if err := decoder.Decode(&testsuites); err != nil {
		// Try parsing as a single testsuite
		if _, seekErr := file.Seek(0, 0); seekErr != nil {
			return nil, fmt.Errorf("failed to seek file %s: %w", filePath, seekErr)
		}
		var testsuite junit.Testsuite
		decoder = xml.NewDecoder(file)
		if err := decoder.Decode(&testsuite); err != nil {
			return nil, fmt.Errorf("failed to parse JUnit XML %s: %w", filePath, err)
		}
		testsuites.Suites = []junit.Testsuite{testsuite}
	}

	return &testsuites, nil
}

// extractFailures extracts all test failures from parsed JUnit data
// Filters for: passed = false AND skipped = false (matching tringa SQL query)
func extractFailures(suites *junit.Testsuites, artifactName string, runID int64) []TestFailure {
	var failures []TestFailure

	// Parse artifact name for run_id and job_id
	_, jobID := parseArtifactName(artifactName)

	for _, suite := range suites.Suites {
		for _, testcase := range suite.Testcases {
			// Filter: failure present AND not skipped
			if testcase.Failure != nil && testcase.Skipped == nil {
				failure := TestFailure{
					ClassName:  testcase.Classname,
					Name:       testcase.Name,
					ArtifactID: artifactName,
					RunID:      runID,
					JobID:      jobID,
					Timestamp:  time.Now(),
				}
				failures = append(failures, failure)
			}
		}
	}

	return failures
}

// extractAllTestRuns extracts all test runs (including successes) from parsed JUnit data
// Used for calculating failure rates
func extractAllTestRuns(suites *junit.Testsuites) []TestRun {
	var runs []TestRun

	for _, suite := range suites.Suites {
		for _, testcase := range suite.Testcases {
			run := TestRun{
				Name:    testcase.Name,
				Failed:  testcase.Failure != nil,
				Skipped: testcase.Skipped != nil,
			}
			runs = append(runs, run)
		}
	}

	return runs
}

// normalizeTestName strips "(retry N)" suffix from test names
// Regex: \s*\(retry \d+\)$
func normalizeTestName(name string) string {
	return retryRegex.ReplaceAllString(name, "")
}

// groupFailuresByTest groups failures by normalized test name
func groupFailuresByTest(failures []TestFailure) map[string][]TestFailure {
	grouped := make(map[string][]TestFailure)

	for _, failure := range failures {
		normalizedName := normalizeTestName(failure.Name)
		grouped[normalizedName] = append(grouped[normalizedName], failure)
	}

	return grouped
}

// countTestRuns counts total runs (including successes) by normalized test name
func countTestRuns(allRuns []TestRun) map[string]int {
	counts := make(map[string]int)

	for _, run := range allRuns {
		// Only count non-skipped tests
		if !run.Skipped {
			normalizedName := normalizeTestName(run.Name)
			counts[normalizedName]++
		}
	}

	return counts
}

// classifyFailures separates failures into categories
func classifyFailures(grouped map[string][]TestFailure) (flaky, timeout, crash map[string][]TestFailure) {
	flaky = make(map[string][]TestFailure)
	timeout = make(map[string][]TestFailure)
	crash = make(map[string][]TestFailure)

	for testName, failures := range grouped {
		// Classify based on test name patterns
		if strings.HasSuffix(testName, "(timeout)") {
			timeout[testName] = failures
		} else if strings.Contains(strings.ToLower(testName), "crash") {
			crash[testName] = failures
		}

		// Flaky: more than MinFlakyFailures failures
		if len(failures) > minFlakyFailures {
			flaky[testName] = failures
		}
	}

	return flaky, timeout, crash
}

// convertToReports converts grouped failures to TestReport slice
// testRunCounts maps test name to total number of runs (including successes)
func convertToReports(grouped map[string][]TestFailure, testRunCounts map[string]int, repo string, maxLinks int) []TestReport {
	var reports []TestReport

	for testName, failures := range grouped {
		totalRuns := testRunCounts[testName]
		if totalRuns == 0 {
			totalRuns = len(failures) // Fallback if we don't have run counts
		}

		// Calculate failure rate per 1000 test runs
		failureRate := 0.0
		if totalRuns > 0 {
			failureRate = (float64(len(failures)) / float64(totalRuns)) * 1000.0
		}

		report := TestReport{
			TestName:     testName,
			FailureCount: len(failures),
			TotalRuns:    totalRuns,
			FailureRate:  failureRate,
			GitHubURLs:   make([]string, 0, maxLinks),
		}

		// Add up to maxLinks URLs
		for i := 0; i < len(failures) && i < maxLinks; i++ {
			failure := failures[i]
			runIDStr := strconv.FormatInt(failure.RunID, 10)
			url := buildGitHubURL(repo, runIDStr, failure.JobID)
			report.GitHubURLs = append(report.GitHubURLs, url)
		}

		reports = append(reports, report)
	}

	// Sort by failure rate descending (most problematic tests first)
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].FailureRate > reports[j].FailureRate
	})

	return reports
}
