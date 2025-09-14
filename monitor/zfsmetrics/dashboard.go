package zfsmetrics

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// Dashboard provides a web interface for ZFS metrics visualization
type Dashboard struct {
	collector *ZFSMetricsCollector
	informer  *ZFSInformer
	config    *DashboardConfig
}

// DashboardConfig holds dashboard configuration
type DashboardConfig struct {
	Port         int    `json:"port"`
	Title        string `json:"title"`
	RefreshRate  int    `json:"refresh_rate"` // seconds
	EnableAlerts bool   `json:"enable_alerts"`
}

// MetricSummary provides a summary view of metrics
type MetricSummary struct {
	Name         string    `json:"name"`
	CurrentValue string    `json:"current_value"`
	Unit         string    `json:"unit"`
	Trend        string    `json:"trend"`
	LastUpdate   time.Time `json:"last_update"`
	Status       string    `json:"status"` // "good", "warning", "critical"
}

// DashboardData contains all data needed for dashboard rendering
type DashboardData struct {
	Title           string           `json:"title"`
	LastUpdate      time.Time        `json:"last_update"`
	PoolName        string           `json:"pool_name"`
	MetricSummaries []MetricSummary  `json:"metric_summaries"`
	CollectionStats map[string]interface{} `json:"collection_stats"`
	Alerts          []Alert          `json:"alerts"`
}

// Alert represents a metric alert
type Alert struct {
	MetricName  string    `json:"metric_name"`
	Message     string    `json:"message"`
	Severity    string    `json:"severity"`
	Timestamp   time.Time `json:"timestamp"`
	Threshold   float64   `json:"threshold"`
	CurrentValue float64  `json:"current_value"`
}

// NewDashboard creates a new ZFS metrics dashboard
func NewDashboard(collector *ZFSMetricsCollector, informer *ZFSInformer, config *DashboardConfig) *Dashboard {
	if config == nil {
		config = &DashboardConfig{
			Port:         8080,
			Title:        "ZFS Metrics Dashboard",
			RefreshRate:  30,
			EnableAlerts: true,
		}
	}
	
	return &Dashboard{
		collector: collector,
		informer:  informer,
		config:    config,
	}
}

// Start starts the dashboard web server
func (d *Dashboard) Start() error {
	http.HandleFunc("/", d.handleDashboard)
	http.HandleFunc("/api/metrics", d.handleAPIMetrics)
	http.HandleFunc("/api/history", d.handleAPIHistory)
	http.HandleFunc("/api/alerts", d.handleAPIAlerts)
	http.HandleFunc("/static/", d.handleStatic)
	
	addr := fmt.Sprintf(":%d", d.config.Port)
	logger.Infof("Starting ZFS metrics dashboard on %s", addr)
	
	return http.ListenAndServe(addr, nil)
}

// handleDashboard serves the main dashboard page
func (d *Dashboard) handleDashboard(w http.ResponseWriter, r *http.Request) {
	data := d.getDashboardData()
	
	tmpl := `
<!DOCTYPE html>
<html>
<head>
    <title>{{.Title}}</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #2c3e50; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
        .metrics-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
        .metric-card { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .metric-value { font-size: 2em; font-weight: bold; margin: 10px 0; }
        .metric-unit { color: #666; font-size: 0.8em; }
        .metric-trend { margin-top: 10px; }
        .trend-up { color: #e74c3c; }
        .trend-down { color: #27ae60; }
        .trend-stable { color: #f39c12; }
        .status-good { border-left: 5px solid #27ae60; }
        .status-warning { border-left: 5px solid #f39c12; }
        .status-critical { border-left: 5px solid #e74c3c; }
        .alerts { background: #fff3cd; border: 1px solid #ffeaa7; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        .alert { margin-bottom: 10px; }
        .stats { background: white; padding: 20px; border-radius: 5px; margin-top: 20px; }
        .refresh-info { text-align: right; color: #666; font-size: 0.9em; }
    </style>
    <script>
        function refreshPage() {
            location.reload();
        }
        setInterval(refreshPage, {{.RefreshRate}}000);
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{{.Title}}</h1>
            <p>ZFS Pool: {{.PoolName}}</p>
            <div class="refresh-info">Last updated: {{.LastUpdate.Format "2006-01-02 15:04:05"}} | Auto-refresh: {{.RefreshRate}}s</div>
        </div>
        
        {{if .Alerts}}
        <div class="alerts">
            <h3>Active Alerts</h3>
            {{range .Alerts}}
            <div class="alert">
                <strong>{{.Severity}}</strong>: {{.Message}} ({{.MetricName}}: {{.CurrentValue}})
            </div>
            {{end}}
        </div>
        {{end}}
        
        <div class="metrics-grid">
            {{range .MetricSummaries}}
            <div class="metric-card status-{{.Status}}">
                <h3>{{.Name}}</h3>
                <div class="metric-value">{{.CurrentValue}} <span class="metric-unit">{{.Unit}}</span></div>
                <div class="metric-trend trend-{{.Trend}}">Trend: {{.Trend}}</div>
                <div style="font-size: 0.8em; color: #666; margin-top: 10px;">
                    Updated: {{.LastUpdate.Format "15:04:05"}}
                </div>
            </div>
            {{end}}
        </div>
        
        <div class="stats">
            <h3>Collection Statistics</h3>
            <p><strong>Collection Count:</strong> {{index .CollectionStats "collection_count"}}</p>
            <p><strong>Monitored Datasets:</strong> {{index .CollectionStats "monitored_datasets"}}</p>
            <p><strong>Metric Histories:</strong> {{index .CollectionStats "metric_histories"}}</p>
        </div>
    </div>
</body>
</html>
`
	
	t, err := template.New("dashboard").Parse(tmpl)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Add refresh rate to template data
	type templateData struct {
		DashboardData
		RefreshRate int
	}
	
	templateDataWithRefresh := templateData{
		DashboardData: data,
		RefreshRate:   d.config.RefreshRate,
	}
	
	w.Header().Set("Content-Type", "text/html")
	if err := t.Execute(w, templateDataWithRefresh); err != nil {
		logger.Errorf("Error executing template: %s", err)
	}
}

// handleAPIMetrics serves metrics data as JSON
func (d *Dashboard) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := d.collector.GetZFSMetrics()
	
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleAPIHistory serves historical metrics data
func (d *Dashboard) handleAPIHistory(w http.ResponseWriter, r *http.Request) {
	metricName := r.URL.Query().Get("metric")
	if metricName == "" {
		http.Error(w, "metric parameter required", http.StatusBadRequest)
		return
	}
	
	history := d.collector.GetMetricHistory(metricName)
	if history == nil {
		http.Error(w, "metric not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(history); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleAPIAlerts serves current alerts as JSON
func (d *Dashboard) handleAPIAlerts(w http.ResponseWriter, r *http.Request) {
	alerts := d.generateAlerts()
	
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(alerts); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// handleStatic serves static files (placeholder)
func (d *Dashboard) handleStatic(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Static files not implemented", http.StatusNotFound)
}

// getDashboardData prepares data for dashboard rendering
func (d *Dashboard) getDashboardData() DashboardData {
	metrics := d.collector.GetZFSMetrics()
	summaries := d.createMetricSummaries(metrics)
	alerts := d.generateAlerts()
	stats := d.collector.GetCollectionStats()
	
	return DashboardData{
		Title:           d.config.Title,
		LastUpdate:      time.Now(),
		PoolName:        d.collector.poolName,
		MetricSummaries: summaries,
		CollectionStats: stats,
		Alerts:          alerts,
	}
}

// createMetricSummaries converts ZFS metrics to dashboard summaries
func (d *Dashboard) createMetricSummaries(metrics []ZFSMetric) []MetricSummary {
	var summaries []MetricSummary
	
	for _, metric := range metrics {
		trend := d.calculateTrend(metric.Name)
		status := d.calculateStatus(metric)
		
		summary := MetricSummary{
			Name:         metric.Name,
			CurrentValue: metric.Value,
			Unit:         metric.Unit,
			Trend:        trend,
			LastUpdate:   time.Now(),
			Status:       status,
		}
		
		summaries = append(summaries, summary)
	}
	
	// Sort summaries by name for consistent display
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})
	
	return summaries
}

// calculateTrend determines the trend for a metric based on history
func (d *Dashboard) calculateTrend(metricName string) string {
	history := d.collector.GetMetricHistory(metricName)
	if history == nil || len(history.Values) < 2 {
		return "stable"
	}
	
	// Simple trend calculation based on last two values
	lastValue := history.Values[len(history.Values)-1]
	prevValue := history.Values[len(history.Values)-2]
	
	diff := lastValue - prevValue
	threshold := 0.01 // 1% change threshold
	
	if diff > threshold {
		return "up"
	} else if diff < -threshold {
		return "down"
	}
	
	return "stable"
}

// calculateStatus determines the status of a metric
func (d *Dashboard) calculateStatus(metric ZFSMetric) string {
	if !metric.Valid {
		return "critical"
	}
	
	// Define thresholds for different metrics
	value, err := strconv.ParseFloat(metric.Value, 64)
	if err != nil {
		return "warning"
	}
	
	switch metric.MetricType {
	case "arc_performance":
		// ARC hit ratio - good if > 90%, warning if > 80%, critical if <= 80%
		if value > 90 {
			return "good"
		} else if value > 80 {
			return "warning"
		}
		return "critical"
		
	case "fragmentation":
		// Fragmentation - good if < 10%, warning if < 25%, critical if >= 25%
		if value < 10 {
			return "good"
		} else if value < 25 {
			return "warning"
		}
		return "critical"
		
	case "compression":
		// Compression ratio - good if > 1.5x, warning if > 1.2x, critical if <= 1.2x
		if value > 1.5 {
			return "good"
		} else if value > 1.2 {
			return "warning"
		}
		return "critical"
		
	default:
		return "good"
	}
}

// generateAlerts creates alerts based on current metrics
func (d *Dashboard) generateAlerts() []Alert {
	if !d.config.EnableAlerts {
		return []Alert{}
	}
	
	var alerts []Alert
	metrics := d.collector.GetZFSMetrics()
	
	for _, metric := range metrics {
		if alert := d.checkMetricForAlert(metric); alert != nil {
			alerts = append(alerts, *alert)
		}
	}
	
	return alerts
}

// checkMetricForAlert checks if a metric should generate an alert
func (d *Dashboard) checkMetricForAlert(metric ZFSMetric) *Alert {
	value, err := strconv.ParseFloat(metric.Value, 64)
	if err != nil {
		return nil
	}
	
	switch metric.MetricType {
	case "arc_performance":
		if value < 80 {
			return &Alert{
				MetricName:   metric.Name,
				Message:      "ARC hit ratio is critically low",
				Severity:     "CRITICAL",
				Timestamp:    time.Now(),
				Threshold:    80.0,
				CurrentValue: value,
			}
		} else if value < 90 {
			return &Alert{
				MetricName:   metric.Name,
				Message:      "ARC hit ratio is below optimal",
				Severity:     "WARNING",
				Timestamp:    time.Now(),
				Threshold:    90.0,
				CurrentValue: value,
			}
		}
		
	case "fragmentation":
		if value > 25 {
			return &Alert{
				MetricName:   metric.Name,
				Message:      "Pool fragmentation is critically high",
				Severity:     "CRITICAL",
				Timestamp:    time.Now(),
				Threshold:    25.0,
				CurrentValue: value,
			}
		} else if value > 10 {
			return &Alert{
				MetricName:   metric.Name,
				Message:      "Pool fragmentation is elevated",
				Severity:     "WARNING",
				Timestamp:    time.Now(),
				Threshold:    10.0,
				CurrentValue: value,
			}
		}
		
	case "compression":
		if value < 1.2 {
			return &Alert{
				MetricName:   metric.Name,
				Message:      "Compression ratio is very low",
				Severity:     "WARNING",
				Timestamp:    time.Now(),
				Threshold:    1.2,
				CurrentValue: value,
			}
		}
	}
	
	return nil
}