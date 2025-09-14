package zfsmetrics

import (
	"fmt"
	"math"
	"time"
)

// mlTrainingLoop runs the ML model training loop
func (po *PerformanceOptimizer) mlTrainingLoop() {
	interval := po.config.ModelUpdateInterval
	if interval == 0 {
		interval = 24 * time.Hour // Default to daily training
	}
	
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := po.mlModel.Train(); err != nil {
				optimizerLogger.Warnf("ML model training failed: %s", err)
			}
		case <-po.ctx.Done():
			return
		}
	}
}

// abTestingLoop runs the A/B testing loop
func (po *PerformanceOptimizer) abTestingLoop() {
	ticker := time.NewTicker(1 * time.Hour) // Check A/B tests every hour
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			po.processABTests()
		case <-po.ctx.Done():
			return
		}
	}
}

// processABTests processes active A/B tests
func (po *PerformanceOptimizer) processABTests() {
	po.abTester.mu.Lock()
	defer po.abTester.mu.Unlock()
	
	for testID, test := range po.abTester.activeTests {
		// Check if test duration has elapsed
		if time.Since(test.StartTime) >= test.Duration {
			result := po.completeABTest(test)
			po.abTester.testHistory = append(po.abTester.testHistory, result)
			delete(po.abTester.activeTests, testID)
			
			optimizerLogger.Infof("Completed A/B test %s, winner: %s, gain: %.2f%%", 
				test.Name, result.Winner, result.PerformanceGain*100)
		} else {
			// Collect metrics for ongoing test
			po.collectABTestMetrics(test)
		}
	}
	
	// Start new A/B tests if we have capacity
	if len(po.abTester.activeTests) < po.config.MaxConcurrentTests {
		po.startNewABTest()
	}
}

// startNewABTest starts a new A/B test
func (po *PerformanceOptimizer) startNewABTest() {
	// Generate test configuration
	testConfig := po.generateTestConfiguration()
	if testConfig == nil {
		return
	}
	
	po.mu.RLock()
	controlConfig := po.currentConfiguration
	po.mu.RUnlock()
	
	testID := fmt.Sprintf("ab_test_%d", time.Now().Unix())
	test := &ABTest{
		ID:            testID,
		Name:          fmt.Sprintf("Test %s optimization", testConfig.Source),
		ControlConfig: controlConfig,
		TestConfig:    testConfig,
		StartTime:     time.Now(),
		Duration:      po.config.ABTestDuration,
		ControlMetrics: []map[string]float64{},
		TestMetrics:   []map[string]float64{},
		Status:        "running",
	}
	
	po.abTester.activeTests[testID] = test
	
	optimizerLogger.Infof("Started A/B test %s for %v", test.Name, test.Duration)
}

// generateTestConfiguration generates a test configuration for A/B testing
func (po *PerformanceOptimizer) generateTestConfiguration() *ZFSConfiguration {
	po.mu.RLock()
	currentConfig := po.currentConfiguration
	po.mu.RUnlock()
	
	if currentConfig == nil {
		return nil
	}
	
	// Create a test configuration with small variations
	testConfig := &ZFSConfiguration{
		PoolName:   currentConfig.PoolName,
		Parameters: make(map[string]interface{}),
		Timestamp:  time.Now(),
		Source:     "ab_test",
		Version:    currentConfig.Version + 1,
	}
	
	// Copy current parameters
	for k, v := range currentConfig.Parameters {
		testConfig.Parameters[k] = v
	}
	
	// Apply small test variations
	testVariations := []struct {
		parameter string
		values    []interface{}
	}{
		{"recordsize", []interface{}{"128K", "256K", "512K", "1M"}},
		{"compression", []interface{}{"lz4", "gzip", "zstd"}},
		{"primarycache", []interface{}{"all", "metadata"}},
		{"logbias", []interface{}{"latency", "throughput"}},
	}
	
	// Randomly select one parameter to test
	if len(testVariations) > 0 {
		variation := testVariations[time.Now().Unix()%int64(len(testVariations))]
		currentValue := currentConfig.Parameters[variation.parameter]
		
		// Find a different value to test
		for _, testValue := range variation.values {
			if testValue != currentValue {
				testConfig.Parameters[variation.parameter] = testValue
				break
			}
		}
	}
	
	return testConfig
}

// collectABTestMetrics collects metrics for an ongoing A/B test
func (po *PerformanceOptimizer) collectABTestMetrics(test *ABTest) {
	// In a real implementation, this would:
	// 1. Split traffic between control and test configurations
	// 2. Collect metrics for each configuration separately
	// 3. Store the metrics for statistical analysis
	
	// For this simplified implementation, we'll simulate metric collection
	currentMetrics := po.collectCurrentMetrics()
	
	// Simulate control metrics (current configuration)
	test.ControlMetrics = append(test.ControlMetrics, currentMetrics)
	
	// Simulate test metrics (with some variation)
	testMetrics := make(map[string]float64)
	for k, v := range currentMetrics {
		// Add small random variation for test metrics
		variation := 1.0 + (float64(time.Now().UnixNano()%100)-50)/1000.0 // ±5% variation
		testMetrics[k] = v * variation
	}
	test.TestMetrics = append(test.TestMetrics, testMetrics)
}

// completeABTest completes an A/B test and determines the winner
func (po *PerformanceOptimizer) completeABTest(test *ABTest) ABTestResult {
	// Calculate average performance for control and test
	controlPerformance := po.calculateAveragePerformance(test.ControlMetrics)
	testPerformance := po.calculateAveragePerformance(test.TestMetrics)
	
	// Perform statistical significance test
	significance := po.calculateStatisticalSignificance(test.ControlMetrics, test.TestMetrics)
	
	// Determine winner
	winner := "inconclusive"
	performanceGain := 0.0
	
	if significance >= po.config.ABTestConfidence {
		if testPerformance > controlPerformance {
			winner = "test"
			performanceGain = (testPerformance - controlPerformance) / controlPerformance
			
			// Apply winning configuration
			if err := po.applyConfiguration(test.TestConfig); err != nil {
				optimizerLogger.Errorf("Failed to apply winning A/B test configuration: %s", err)
			} else {
				po.mu.Lock()
				po.currentConfiguration = test.TestConfig
				po.mu.Unlock()
			}
		} else {
			winner = "control"
			performanceGain = (controlPerformance - testPerformance) / testPerformance
		}
	}
	
	return ABTestResult{
		TestID:                  test.ID,
		TestName:                test.Name,
		StartTime:               test.StartTime,
		EndTime:                 time.Now(),
		Winner:                  winner,
		PerformanceGain:         performanceGain,
		StatisticalSignificance: significance,
		ControlConfig:           test.ControlConfig,
		TestConfig:              test.TestConfig,
	}
}

// calculateAveragePerformance calculates average performance from metrics
func (po *PerformanceOptimizer) calculateAveragePerformance(metricsHistory []map[string]float64) float64 {
	if len(metricsHistory) == 0 {
		return 0.0
	}
	
	totalPerformance := 0.0
	for _, metrics := range metricsHistory {
		performance := po.calculatePerformanceTargets(metrics)["performance_score"]
		totalPerformance += performance
	}
	
	return totalPerformance / float64(len(metricsHistory))
}

// calculateStatisticalSignificance calculates statistical significance using t-test
func (po *PerformanceOptimizer) calculateStatisticalSignificance(controlMetrics, testMetrics []map[string]float64) float64 {
	// Calculate performance scores for both groups
	var controlScores, testScores []float64
	
	for _, metrics := range controlMetrics {
		score := po.calculatePerformanceTargets(metrics)["performance_score"]
		controlScores = append(controlScores, score)
	}
	
	for _, metrics := range testMetrics {
		score := po.calculatePerformanceTargets(metrics)["performance_score"]
		testScores = append(testScores, score)
	}
	
	// Perform Welch's t-test (simplified implementation)
	if len(controlScores) < 2 || len(testScores) < 2 {
		return 0.0
	}
	
	controlMean := calculateMean(controlScores)
	testMean := calculateMean(testScores)
	controlVar := calculateVariance(controlScores, controlMean)
	testVar := calculateVariance(testScores, testMean)
	
	// Calculate t-statistic
	pooledStdErr := math.Sqrt(controlVar/float64(len(controlScores)) + testVar/float64(len(testScores)))
	if pooledStdErr == 0 {
		return 0.0
	}
	
	tStat := math.Abs(testMean-controlMean) / pooledStdErr
	
	// Convert t-statistic to confidence level (simplified)
	// In a real implementation, you would use proper statistical tables
	if tStat > 2.576 {
		return 0.99 // 99% confidence
	} else if tStat > 1.96 {
		return 0.95 // 95% confidence
	} else if tStat > 1.645 {
		return 0.90 // 90% confidence
	} else {
		return tStat / 2.576 // Approximate confidence
	}
}

// calculateMean calculates the mean of a slice of float64
func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0.0
	}
	
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	
	return sum / float64(len(values))
}

// calculateVariance calculates the variance of a slice of float64
func calculateVariance(values []float64, mean float64) float64 {
	if len(values) <= 1 {
		return 0.0
	}
	
	sumSquaredDiff := 0.0
	for _, v := range values {
		diff := v - mean
		sumSquaredDiff += diff * diff
	}
	
	return sumSquaredDiff / float64(len(values)-1)
}

// StartABTest manually starts an A/B test with specific configurations
func (po *PerformanceOptimizer) StartABTest(name string, testConfig *ZFSConfiguration, duration time.Duration) (string, error) {
	po.abTester.mu.Lock()
	defer po.abTester.mu.Unlock()
	
	if len(po.abTester.activeTests) >= po.config.MaxConcurrentTests {
		return "", fmt.Errorf("maximum concurrent A/B tests reached (%d)", po.config.MaxConcurrentTests)
	}
	
	po.mu.RLock()
	controlConfig := po.currentConfiguration
	po.mu.RUnlock()
	
	testID := fmt.Sprintf("manual_ab_test_%d", time.Now().Unix())
	test := &ABTest{
		ID:            testID,
		Name:          name,
		ControlConfig: controlConfig,
		TestConfig:    testConfig,
		StartTime:     time.Now(),
		Duration:      duration,
		ControlMetrics: []map[string]float64{},
		TestMetrics:   []map[string]float64{},
		Status:        "running",
	}
	
	po.abTester.activeTests[testID] = test
	
	optimizerLogger.Infof("Started manual A/B test %s (%s) for %v", name, testID, duration)
	return testID, nil
}

// StopABTest manually stops an A/B test
func (po *PerformanceOptimizer) StopABTest(testID string) (*ABTestResult, error) {
	po.abTester.mu.Lock()
	defer po.abTester.mu.Unlock()
	
	test, exists := po.abTester.activeTests[testID]
	if !exists {
		return nil, fmt.Errorf("A/B test %s not found", testID)
	}
	
	result := po.completeABTest(test)
	po.abTester.testHistory = append(po.abTester.testHistory, result)
	delete(po.abTester.activeTests, testID)
	
	optimizerLogger.Infof("Manually stopped A/B test %s", testID)
	return &result, nil
}

// GetActiveABTests returns currently active A/B tests
func (po *PerformanceOptimizer) GetActiveABTests() map[string]*ABTest {
	po.abTester.mu.RLock()
	defer po.abTester.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	activeTests := make(map[string]*ABTest)
	for k, v := range po.abTester.activeTests {
		testCopy := *v
		activeTests[k] = &testCopy
	}
	
	return activeTests
}

// GetABTestHistory returns A/B test history
func (po *PerformanceOptimizer) GetABTestHistory() []ABTestResult {
	po.abTester.mu.RLock()
	defer po.abTester.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	history := make([]ABTestResult, len(po.abTester.testHistory))
	copy(history, po.abTester.testHistory)
	return history
}

// GetABTestStats returns A/B testing statistics
func (po *PerformanceOptimizer) GetABTestStats() map[string]interface{} {
	po.abTester.mu.RLock()
	defer po.abTester.mu.RUnlock()
	
	totalTests := len(po.abTester.testHistory)
	testWins := 0
	controlWins := 0
	inconclusiveTests := 0
	totalGain := 0.0
	
	for _, result := range po.abTester.testHistory {
		switch result.Winner {
		case "test":
			testWins++
			totalGain += result.PerformanceGain
		case "control":
			controlWins++
		case "inconclusive":
			inconclusiveTests++
		}
	}
	
	avgGain := 0.0
	if testWins > 0 {
		avgGain = totalGain / float64(testWins)
	}
	
	return map[string]interface{}{
		"total_tests":        totalTests,
		"active_tests":       len(po.abTester.activeTests),
		"test_wins":          testWins,
		"control_wins":       controlWins,
		"inconclusive_tests": inconclusiveTests,
		"test_win_rate":      float64(testWins) / float64(totalTests),
		"average_gain":       avgGain,
	}
}

// Validate validates the optimizer configuration
func (config *OptimizerConfig) Validate() error {
	if config.OptimizationInterval <= 0 {
		return fmt.Errorf("optimization_interval must be positive")
	}
	
	if config.ABTestDuration <= 0 {
		config.ABTestDuration = 24 * time.Hour // Default to 24 hours
	}
	
	if config.ABTestConfidence <= 0 || config.ABTestConfidence > 1 {
		config.ABTestConfidence = 0.95 // Default to 95% confidence
	}
	
	if config.MaxConcurrentTests <= 0 {
		config.MaxConcurrentTests = 3 // Default to 3 concurrent tests
	}
	
	if config.MaxParameterChange <= 0 {
		config.MaxParameterChange = 0.5 // Default to 50% max change
	}
	
	if config.RollbackThreshold == 0 {
		config.RollbackThreshold = -0.05 // Default to -5% (rollback if performance drops by 5%)
	}
	
	return nil
}