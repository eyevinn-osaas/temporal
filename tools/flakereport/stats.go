package flakereport

import (
	"math"
	"sort"
	"time"
)

// calculateJobLatencyStats computes p50, p75, p95 latency from job timing data
// Latency is measured as the time between created_at and started_at
func calculateJobLatencyStats(jobs []WorkflowJob) JobLatencyStats {
	if len(jobs) == 0 {
		return JobLatencyStats{}
	}

	// Calculate latencies in seconds
	var latencies []float64
	for _, job := range jobs {
		// Skip jobs that haven't started yet
		if job.StartedAt.IsZero() {
			continue
		}

		latency := job.StartedAt.Sub(job.CreatedAt).Seconds()
		// Only include positive latencies (should always be positive, but sanity check)
		if latency >= 0 {
			latencies = append(latencies, latency)
		}
	}

	if len(latencies) == 0 {
		return JobLatencyStats{}
	}

	// Sort latencies for percentile calculation
	sort.Float64s(latencies)

	// Round to nearest second for cleaner output
	stats := JobLatencyStats{
		P50:      time.Duration(math.Round(percentile(latencies, 50)) * float64(time.Second)),
		P75:      time.Duration(math.Round(percentile(latencies, 75)) * float64(time.Second)),
		P95:      time.Duration(math.Round(percentile(latencies, 95)) * float64(time.Second)),
		JobCount: len(latencies),
	}

	return stats
}

// percentile calculates the nth percentile of a sorted slice
func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}

	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}

	// Calculate index using linear interpolation
	index := float64(p) / 100.0 * float64(len(sorted)-1)
	lower := int(index)
	upper := lower + 1

	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	// Linear interpolation between two nearest values
	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}
