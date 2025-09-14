package integration

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GenerateJSONReport creates a JSON report of benchmark results
func (rg *ReportGenerator) GenerateJSONReport(results *BenchmarkResults) error {
	filename := filepath.Join(rg.config.ReportDirectory, 
		fmt.Sprintf("benchmark_report_%s.json", time.Now().Format("20060102_150405")))
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create JSON report file: %w", err)
	}
	defer file.Close()
	
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	
	if err := encoder.Encode(results); err != nil {
		return fmt.Errorf("failed to encode JSON report: %w", err)
	}
	
	fmt.Printf("JSON report generated: %s\n", filename)
	return nil
}

// GenerateHTMLReport creates an HTML report of benchmark results
func (rg *ReportGenerator) GenerateHTMLReport(results *BenchmarkResults) error {
	filename := filepath.Join(rg.config.ReportDirectory,
		fmt.Sprintf("benchmark_report_%s.html", time.Now().Format("20060102_150405")))
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create HTML report file: %w", err)
	}
	defer file.Close()
	
	tmpl := template.Must(template.New("report").Parse(htmlReportTemplate))
	
	if err := tmpl.Execute(file, results); err != nil {
		return fmt.Errorf("failed to execute HTML template: %w", err)
	}
	
	fmt.Printf("HTML report generated: %s\n", filename)
	return nil
}

// GenerateCSVReport creates a CSV report of benchmark results
func (rg *ReportGenerator) GenerateCSVReport(results *BenchmarkResults) error {
	filename := filepath.Join(rg.config.ReportDirectory,
		fmt.Sprintf("benchmark_report_%s.csv", time.Now().Format("20060102_150405")))
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV report file: %w", err)
	}
	defer file.Close()
	
	writer := csv.NewWriter(file)
	defer writer.Flush()
	
	// Write header
	header := []string{
		"Benchmark Name",
		"Workload Type",
		"Duration (s)",
		"Throughput (ops/sec)",
		"P99 Latency (ms)",
		"Error Rate (%)",
		"CPU Usage (%)",
		"Memory Usage (%)",
		"ZFS ARC Hit Ratio (%)",
		"ZFS Compression Ratio",
	}
	
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	
	// Write data rows
	for name, result := range results.Results {
		row := []string{
			name,
			result.WorkloadType.String(),
			fmt.Sprintf("%.2f", result.Duration.Seconds()),
			fmt.Sprintf("%.0f", result.Throughput),
			fmt.Sprintf("%.2f", float64(result.Latency.P99.Nanoseconds())/1e6),
			fmt.Sprintf("%.4f", result.ErrorRate*100),
			fmt.Sprintf("%.2f", result.ResourceUsage.CPUUsagePercent),
			fmt.Sprintf("%.2f", result.ResourceUsage.MemoryUsagePercent),
			fmt.Sprintf("%.2f", result.ZFSMetrics.ARCHitRatio),
			fmt.Sprintf("%.2f", result.ZFSMetrics.CompressionRatio),
		}
		
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}
	
	fmt.Printf("CSV report generated: %s\n", filename)
	return nil
}

// GenerateMarkdownReport creates a Markdown report of benchmark results
func (rg *ReportGenerator) GenerateMarkdownReport(results *BenchmarkResults) error {
	filename := filepath.Join(rg.config.ReportDirectory,
		fmt.Sprintf("benchmark_report_%s.md", time.Now().Format("20060102_150405")))
	
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create Markdown report file: %w", err)
	}
	defer file.Close()
	
	// Write report content
	fmt.Fprintf(file, "# Benchmark Report: %s\n\n", results.SuiteName)
	fmt.Fprintf(file, "**Generated:** %s\n\n", results.Timestamp.Format("2006-01-02 15:04:05"))
	
	// Summary section
	fmt.Fprintf(file, "## Summary\n\n")
	fmt.Fprintf(file, "- **Total Benchmarks:** %d\n", results.Summary.TotalBenchmarks)
	fmt.Fprintf(file, "- **Passed:** %d\n", results.Summary.PassedBenchmarks)
	fmt.Fprintf(file, "- **Failed:** %d\n", results.Summary.FailedBenchmarks)
	fmt.Fprintf(file, "- **Overall Score:** %.1f\n", results.Summary.OverallScore)
	fmt.Fprintf(file, "- **Performance Grade:** %s\n\n", results.Summary.PerformanceGrade)
	
	// Results table
	fmt.Fprintf(file, "## Benchmark Results\n\n")
	fmt.Fprintf(file, "| Benchmark | Throughput (ops/sec) | P99 Latency (ms) | Error Rate (%%) | Grade |\n")
	fmt.Fprintf(file, "|-----------|---------------------|------------------|----------------|-------|\n")
	
	for name, result := range results.Results {
		grade := rg.getBenchmarkGrade(result)
		fmt.Fprintf(file, "| %s | %.0f | %.2f | %.4f | %s |\n",
			name,
			result.Throughput,
			float64(result.Latency.P99.Nanoseconds())/1e6,
			result.ErrorRate*100,
			grade)
	}
	
	// Detailed results
	fmt.Fprintf(file, "\n## Detailed Results\n\n")
	for name, result := range results.Results {
		fmt.Fprintf(file, "### %s\n\n", strings.Title(strings.ReplaceAll(name, "_", " ")))
		fmt.Fprintf(file, "- **Workload Type:** %s\n", result.WorkloadType.String())
		fmt.Fprintf(file, "- **Duration:** %v\n", result.Duration)
		fmt.Fprintf(file, "- **Sample Count:** %d\n", result.SampleCount)
		fmt.Fprintf(file, "- **Throughput:** %.0f ops/sec\n", result.Throughput)
		
		fmt.Fprintf(file, "\n**Latency Metrics:**\n")
		fmt.Fprintf(file, "- Mean: %v\n", result.Latency.Mean)
		fmt.Fprintf(file, "- Median: %v\n", result.Latency.Median)
		fmt.Fprintf(file, "- P95: %v\n", result.Latency.P95)
		fmt.Fprintf(file, "- P99: %v\n", result.Latency.P99)
		fmt.Fprintf(file, "- Max: %v\n", result.Latency.Max)
		
		fmt.Fprintf(file, "\n**Resource Usage:**\n")
		fmt.Fprintf(file, "- CPU: %.2f%%\n", result.ResourceUsage.CPUUsagePercent)
		fmt.Fprintf(file, "- Memory: %.2f%%\n", result.ResourceUsage.MemoryUsagePercent)
		fmt.Fprintf(file, "- Disk IOPS: %d\n", result.ResourceUsage.DiskIOPS)
		fmt.Fprintf(file, "- Network: %.2f MB/s\n", result.ResourceUsage.NetworkBandwidthMBps)
		
		fmt.Fprintf(file, "\n**ZFS Metrics:**\n")
		fmt.Fprintf(file, "- ARC Hit Ratio: %.2f%%\n", result.ZFSMetrics.ARCHitRatio)
		fmt.Fprintf(file, "- L2ARC Hit Ratio: %.2f%%\n", result.ZFSMetrics.L2ARCHitRatio)
		fmt.Fprintf(file, "- Compression Ratio: %.2fx\n", result.ZFSMetrics.CompressionRatio)
		fmt.Fprintf(file, "- Deduplication Ratio: %.2fx\n", result.ZFSMetrics.DeduplicationRatio)
		fmt.Fprintf(file, "- Fragmentation: %.2f%%\n", result.ZFSMetrics.FragmentationPercent)
		
		fmt.Fprintf(file, "\n")
	}
	
	// Comparisons section
	if len(results.Comparisons) > 0 {
		fmt.Fprintf(file, "## Baseline Comparisons\n\n")
		fmt.Fprintf(file, "| Benchmark | Baseline | Current | Change (%%) | Status |\n")
		fmt.Fprintf(file, "|-----------|----------|---------|------------|--------|\n")
		
		for _, comp := range results.Comparisons {
			status := "✅ " + comp.Significance
			if comp.IsRegression {
				status = "❌ regression"
			}
			
			fmt.Fprintf(file, "| %s | %.0f | %.0f | %+.2f | %s |\n",
				comp.BenchmarkName,
				comp.BaselineValue,
				comp.CurrentValue,
				comp.PercentChange,
				status)
		}
		fmt.Fprintf(file, "\n")
	}
	
	// Recommendations section
	if len(results.Summary.Recommendations) > 0 {
		fmt.Fprintf(file, "## Recommendations\n\n")
		for i, rec := range results.Summary.Recommendations {
			fmt.Fprintf(file, "%d. %s\n", i+1, rec)
		}
		fmt.Fprintf(file, "\n")
	}
	
	// Configuration section
	fmt.Fprintf(file, "## Configuration\n\n")
	fmt.Fprintf(file, "```json\n")
	configJSON, _ := json.MarshalIndent(results.Configuration, "", "  ")
	fmt.Fprintf(file, "%s\n", string(configJSON))
	fmt.Fprintf(file, "```\n")
	
	fmt.Printf("Markdown report generated: %s\n", filename)
	return nil
}

// getBenchmarkGrade assigns a grade to individual benchmark results
func (rg *ReportGenerator) getBenchmarkGrade(result *BenchmarkResult) string {
	score := 100.0
	
	// Penalize high error rates
	score -= result.ErrorRate * 1000
	
	// Penalize high latency
	latencyMs := float64(result.Latency.P99.Nanoseconds()) / 1e6
	if latencyMs > 50 {
		score -= (latencyMs - 50) / 5
	}
	
	// Penalize high resource usage
	if result.ResourceUsage.CPUUsagePercent > 80 {
		score -= (result.ResourceUsage.CPUUsagePercent - 80) * 2
	}
	
	switch {
	case score >= 95:
		return "A+"
	case score >= 90:
		return "A"
	case score >= 85:
		return "A-"
	case score >= 80:
		return "B+"
	case score >= 75:
		return "B"
	case score >= 70:
		return "B-"
	case score >= 65:
		return "C+"
	case score >= 60:
		return "C"
	default:
		return "F"
	}
}

// HTML template for benchmark reports
const htmlReportTemplate = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Benchmark Report: {{.SuiteName}}</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
        }
        .header .timestamp {
            opacity: 0.9;
            margin-top: 10px;
        }
        .summary {
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        .summary h2 {
            color: #667eea;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        .summary-item {
            text-align: center;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        .summary-item .value {
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
        }
        .summary-item .label {
            color: #666;
            margin-top: 5px;
        }
        .grade {
            font-size: 3em !important;
            color: #28a745;
        }
        .grade.warning { color: #ffc107; }
        .grade.danger { color: #dc3545; }
        .results-table {
            background: white;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        .results-table h2 {
            color: #667eea;
            padding: 20px 25px 0;
            margin: 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        th {
            background: #667eea;
            color: white;
            font-weight: 600;
        }
        tr:hover {
            background: #f8f9fa;
        }
        .metric-good { color: #28a745; font-weight: bold; }
        .metric-warning { color: #ffc107; font-weight: bold; }
        .metric-danger { color: #dc3545; font-weight: bold; }
        .detailed-results {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .benchmark-card {
            background: white;
            border-radius: 10px;
            padding: 25px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .benchmark-card h3 {
            color: #667eea;
            margin-top: 0;
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }
        .metric-group {
            margin: 15px 0;
        }
        .metric-group h4 {
            color: #555;
            margin-bottom: 8px;
            font-size: 1.1em;
        }
        .metric-list {
            list-style: none;
            padding: 0;
        }
        .metric-list li {
            padding: 3px 0;
            display: flex;
            justify-content: space-between;
        }
        .metric-name {
            color: #666;
        }
        .metric-value {
            font-weight: bold;
            color: #333;
        }
        .comparisons {
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }
        .comparison-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }
        .comparison-item:last-child {
            border-bottom: none;
        }
        .improvement { color: #28a745; }
        .regression { color: #dc3545; }
        .neutral { color: #6c757d; }
        .recommendations {
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .recommendations h2 {
            color: #667eea;
        }
        .recommendations ul {
            padding-left: 20px;
        }
        .recommendations li {
            margin: 10px 0;
            line-height: 1.5;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>{{.SuiteName}}</h1>
        <div class="timestamp">Generated: {{.Timestamp.Format "2006-01-02 15:04:05"}}</div>
    </div>

    <div class="summary">
        <h2>Summary</h2>
        <div class="summary-grid">
            <div class="summary-item">
                <div class="value">{{.Summary.TotalBenchmarks}}</div>
                <div class="label">Total Benchmarks</div>
            </div>
            <div class="summary-item">
                <div class="value metric-good">{{.Summary.PassedBenchmarks}}</div>
                <div class="label">Passed</div>
            </div>
            <div class="summary-item">
                <div class="value metric-danger">{{.Summary.FailedBenchmarks}}</div>
                <div class="label">Failed</div>
            </div>
            <div class="summary-item">
                <div class="value">{{printf "%.1f" .Summary.OverallScore}}</div>
                <div class="label">Overall Score</div>
            </div>
            <div class="summary-item">
                <div class="value grade{{if lt .Summary.OverallScore 70}} danger{{else if lt .Summary.OverallScore 85}} warning{{end}}">{{.Summary.PerformanceGrade}}</div>
                <div class="label">Grade</div>
            </div>
        </div>
    </div>

    <div class="results-table">
        <h2>Benchmark Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Benchmark</th>
                    <th>Throughput (ops/sec)</th>
                    <th>P99 Latency (ms)</th>
                    <th>Error Rate (%)</th>
                    <th>CPU Usage (%)</th>
                    <th>Memory Usage (%)</th>
                </tr>
            </thead>
            <tbody>
                {{range $name, $result := .Results}}
                <tr>
                    <td><strong>{{$name}}</strong></td>
                    <td class="{{if gt $result.Throughput 10000}}metric-good{{else if gt $result.Throughput 1000}}metric-warning{{else}}metric-danger{{end}}">
                        {{printf "%.0f" $result.Throughput}}
                    </td>
                    <td class="{{if lt (div $result.Latency.P99.Nanoseconds 1000000) 50}}metric-good{{else if lt (div $result.Latency.P99.Nanoseconds 1000000) 100}}metric-warning{{else}}metric-danger{{end}}">
                        {{printf "%.2f" (div (float64 $result.Latency.P99.Nanoseconds) 1000000)}}
                    </td>
                    <td class="{{if lt $result.ErrorRate 0.01}}metric-good{{else if lt $result.ErrorRate 0.05}}metric-warning{{else}}metric-danger{{end}}">
                        {{printf "%.4f" (mul $result.ErrorRate 100)}}
                    </td>
                    <td class="{{if lt $result.ResourceUsage.CPUUsagePercent 70}}metric-good{{else if lt $result.ResourceUsage.CPUUsagePercent 85}}metric-warning{{else}}metric-danger{{end}}">
                        {{printf "%.1f" $result.ResourceUsage.CPUUsagePercent}}
                    </td>
                    <td class="{{if lt $result.ResourceUsage.MemoryUsagePercent 70}}metric-good{{else if lt $result.ResourceUsage.MemoryUsagePercent 85}}metric-warning{{else}}metric-danger{{end}}">
                        {{printf "%.1f" $result.ResourceUsage.MemoryUsagePercent}}
                    </td>
                </tr>
                {{end}}
            </tbody>
        </table>
    </div>

    <div class="detailed-results">
        {{range $name, $result := .Results}}
        <div class="benchmark-card">
            <h3>{{$name}}</h3>
            
            <div class="metric-group">
                <h4>Performance</h4>
                <ul class="metric-list">
                    <li><span class="metric-name">Throughput:</span> <span class="metric-value">{{printf "%.0f ops/sec" $result.Throughput}}</span></li>
                    <li><span class="metric-name">Duration:</span> <span class="metric-value">{{$result.Duration}}</span></li>
                    <li><span class="metric-name">Sample Count:</span> <span class="metric-value">{{$result.SampleCount}}</span></li>
                </ul>
            </div>

            <div class="metric-group">
                <h4>Latency</h4>
                <ul class="metric-list">
                    <li><span class="metric-name">Mean:</span> <span class="metric-value">{{$result.Latency.Mean}}</span></li>
                    <li><span class="metric-name">P95:</span> <span class="metric-value">{{$result.Latency.P95}}</span></li>
                    <li><span class="metric-name">P99:</span> <span class="metric-value">{{$result.Latency.P99}}</span></li>
                    <li><span class="metric-name">Max:</span> <span class="metric-value">{{$result.Latency.Max}}</span></li>
                </ul>
            </div>

            <div class="metric-group">
                <h4>ZFS Metrics</h4>
                <ul class="metric-list">
                    <li><span class="metric-name">ARC Hit Ratio:</span> <span class="metric-value">{{printf "%.1f%%" $result.ZFSMetrics.ARCHitRatio}}</span></li>
                    <li><span class="metric-name">Compression:</span> <span class="metric-value">{{printf "%.1fx" $result.ZFSMetrics.CompressionRatio}}</span></li>
                    <li><span class="metric-name">Fragmentation:</span> <span class="metric-value">{{printf "%.1f%%" $result.ZFSMetrics.FragmentationPercent}}</span></li>
                </ul>
            </div>
        </div>
        {{end}}
    </div>

    {{if .Comparisons}}
    <div class="comparisons">
        <h2>Baseline Comparisons</h2>
        {{range .Comparisons}}
        <div class="comparison-item">
            <span><strong>{{.BenchmarkName}}</strong></span>
            <span class="{{.Significance}}">
                {{printf "%.0f → %.0f (%.1f%%)" .BaselineValue .CurrentValue .PercentChange}}
                {{if .IsRegression}}📉{{else if eq .Significance "improvement"}}📈{{else}}➡️{{end}}
            </span>
        </div>
        {{end}}
    </div>
    {{end}}

    {{if .Summary.Recommendations}}
    <div class="recommendations">
        <h2>Recommendations</h2>
        <ul>
            {{range .Summary.Recommendations}}
            <li>{{.}}</li>
            {{end}}
        </ul>
    </div>
    {{end}}
</body>
</html>
`