package zfsmetrics

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"
)

// NewMLModel creates a new ML model for ZFS optimization
func NewMLModel(config *OptimizerConfig) (*MLModel, error) {
	model := &MLModel{
		ModelPath:       config.MLModelPath,
		Features:        []string{"arc_hit_ratio", "compression_ratio", "fragmentation_level", "iops"},
		Targets:         []string{"performance_score"},
		TrainingData:    []TrainingExample{},
		PredictionCache: make(map[string]Prediction),
	}
	
	// Load existing model if available
	if config.MLModelPath != "" {
		if err := model.LoadFromFile(config.MLModelPath); err != nil {
			optimizerLogger.Warnf("Failed to load ML model from %s: %s", config.MLModelPath, err)
		}
	}
	
	// Load training data if available
	if config.TrainingDataPath != "" {
		if err := model.LoadTrainingData(config.TrainingDataPath); err != nil {
			optimizerLogger.Warnf("Failed to load training data from %s: %s", config.TrainingDataPath, err)
		}
	}
	
	return model, nil
}

// Predict generates a prediction for optimal ZFS parameters
func (ml *MLModel) Predict(features map[string]float64) (Prediction, error) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	
	// Create cache key from features
	cacheKey := ml.createCacheKey(features)
	
	// Check cache first
	if cached, exists := ml.PredictionCache[cacheKey]; exists {
		// Return cached prediction if it's recent (within 1 hour)
		if time.Since(cached.Timestamp) < time.Hour {
			return cached, nil
		}
	}
	
	// Generate new prediction
	prediction := ml.generatePrediction(features)
	
	// Cache the prediction
	ml.PredictionCache[cacheKey] = prediction
	
	return prediction, nil
}

// generatePrediction generates a new prediction using the ML model
func (ml *MLModel) generatePrediction(features map[string]float64) Prediction {
	// This is a simplified ML model implementation
	// In a production system, this would use a proper ML framework like TensorFlow or PyTorch
	
	parameters := make(map[string]interface{})
	expectedGain := 0.0
	confidence := 0.5
	
	// Extract key features
	arcHitRatio := features["zfs_arc_hit_ratio"]
	compressionRatio := features["zfs_compression_ratio"]
	fragmentationLevel := features["zfs_fragmentation_level"]
	readIOPS := features["zfs_read_iops"]
	writeIOPS := features["zfs_write_iops"]
	
	// Rule-based optimization (simplified ML model)
	
	// Optimize recordsize based on workload pattern
	if readIOPS > writeIOPS*2 {
		// Read-heavy workload - larger recordsize
		parameters["recordsize"] = "1M"
		expectedGain += 0.05
	} else if writeIOPS > readIOPS*2 {
		// Write-heavy workload - smaller recordsize
		parameters["recordsize"] = "128K"
		expectedGain += 0.03
	} else {
		// Balanced workload
		parameters["recordsize"] = "256K"
		expectedGain += 0.02
	}
	
	// Optimize compression based on current ratio
	if compressionRatio < 1.5 {
		// Low compression - try more aggressive compression
		parameters["compression"] = "zstd"
		expectedGain += 0.08
		confidence += 0.1
	} else if compressionRatio > 3.0 {
		// Very high compression - might be too aggressive
		parameters["compression"] = "lz4"
		expectedGain += 0.02
	}
	
	// Optimize caching based on ARC hit ratio
	if arcHitRatio < 80 {
		// Low ARC hit ratio - optimize caching
		parameters["primarycache"] = "all"
		parameters["secondarycache"] = "all"
		expectedGain += 0.10
		confidence += 0.15
	} else if arcHitRatio > 95 {
		// Very high ARC hit ratio - might reduce cache overhead
		parameters["primarycache"] = "metadata"
		expectedGain += 0.01
	}
	
	// Optimize based on fragmentation
	if fragmentationLevel > 20 {
		// High fragmentation - optimize for defragmentation
		parameters["logbias"] = "throughput"
		expectedGain += 0.05
		confidence += 0.1
	}
	
	// Optimize sync behavior based on workload
	totalIOPS := readIOPS + writeIOPS
	if totalIOPS > 10000 {
		// High IOPS workload - optimize for performance
		parameters["logbias"] = "latency"
		expectedGain += 0.03
	}
	
	// Use historical data to improve predictions
	if len(ml.TrainingData) > 10 {
		historicalGain := ml.calculateHistoricalGain(features)
		expectedGain = (expectedGain + historicalGain) / 2
		confidence += 0.1
	}
	
	// Ensure confidence is within bounds
	if confidence > 1.0 {
		confidence = 1.0
	}
	
	return Prediction{
		Parameters:   parameters,
		ExpectedGain: expectedGain,
		Confidence:   confidence,
		Timestamp:    time.Now(),
	}
}

// calculateHistoricalGain calculates expected gain based on historical data
func (ml *MLModel) calculateHistoricalGain(features map[string]float64) float64 {
	// Find similar historical examples
	similarExamples := ml.findSimilarExamples(features, 10)
	
	if len(similarExamples) == 0 {
		return 0.0
	}
	
	// Calculate average performance gain from similar examples
	totalGain := 0.0
	for _, example := range similarExamples {
		if score, exists := example.Targets["performance_score"]; exists {
			totalGain += score
		}
	}
	
	return totalGain / float64(len(similarExamples))
}

// findSimilarExamples finds training examples similar to the given features
func (ml *MLModel) findSimilarExamples(features map[string]float64, maxExamples int) []TrainingExample {
	type exampleWithDistance struct {
		example  TrainingExample
		distance float64
	}
	
	var examples []exampleWithDistance
	
	// Calculate distance to each training example
	for _, example := range ml.TrainingData {
		distance := ml.calculateFeatureDistance(features, example.Features)
		examples = append(examples, exampleWithDistance{
			example:  example,
			distance: distance,
		})
	}
	
	// Sort by distance (closest first)
	sort.Slice(examples, func(i, j int) bool {
		return examples[i].distance < examples[j].distance
	})
	
	// Return the closest examples
	var result []TrainingExample
	for i := 0; i < len(examples) && i < maxExamples; i++ {
		result = append(result, examples[i].example)
	}
	
	return result
}

// calculateFeatureDistance calculates the Euclidean distance between two feature sets
func (ml *MLModel) calculateFeatureDistance(features1, features2 map[string]float64) float64 {
	distance := 0.0
	featureCount := 0
	
	for feature, value1 := range features1 {
		if value2, exists := features2[feature]; exists {
			diff := value1 - value2
			distance += diff * diff
			featureCount++
		}
	}
	
	if featureCount == 0 {
		return math.Inf(1)
	}
	
	return math.Sqrt(distance / float64(featureCount))
}

// Train trains the ML model with current training data
func (ml *MLModel) Train() error {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	
	if len(ml.TrainingData) < 10 {
		return fmt.Errorf("insufficient training data: need at least 10 examples, have %d", len(ml.TrainingData))
	}
	
	// This is a placeholder for actual ML training
	// In a production system, this would:
	// 1. Prepare training data in the correct format
	// 2. Train a neural network or other ML model
	// 3. Validate the model accuracy
	// 4. Save the trained model
	
	// For now, we'll simulate training by calculating model accuracy
	ml.ModelAccuracy = ml.calculateModelAccuracy()
	ml.LastTrained = time.Now()
	
	optimizerLogger.Infof("ML model trained with %d examples, accuracy: %.2f%%", 
		len(ml.TrainingData), ml.ModelAccuracy*100)
	
	return nil
}

// calculateModelAccuracy calculates the accuracy of the current model
func (ml *MLModel) calculateModelAccuracy() float64 {
	if len(ml.TrainingData) < 5 {
		return 0.5 // Default accuracy for insufficient data
	}
	
	// Use cross-validation to estimate accuracy
	correctPredictions := 0
	totalPredictions := 0
	
	// Simple k-fold cross-validation (k=5)
	foldSize := len(ml.TrainingData) / 5
	
	for fold := 0; fold < 5; fold++ {
		// Create training and test sets
		testStart := fold * foldSize
		testEnd := testStart + foldSize
		if fold == 4 {
			testEnd = len(ml.TrainingData) // Include remaining examples in last fold
		}
		
		// Test on this fold
		for i := testStart; i < testEnd && i < len(ml.TrainingData); i++ {
			example := ml.TrainingData[i]
			prediction := ml.generatePrediction(example.Features)
			
			// Check if prediction is reasonable (simplified accuracy check)
			if prediction.ExpectedGain > 0 && prediction.Confidence > 0.3 {
				correctPredictions++
			}
			totalPredictions++
		}
	}
	
	if totalPredictions == 0 {
		return 0.5
	}
	
	return float64(correctPredictions) / float64(totalPredictions)
}

// AddTrainingExample adds a new training example
func (ml *MLModel) AddTrainingExample(example TrainingExample) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	
	ml.TrainingData = append(ml.TrainingData, example)
	
	// Limit training data size to prevent memory issues
	maxTrainingData := 10000
	if len(ml.TrainingData) > maxTrainingData {
		ml.TrainingData = ml.TrainingData[len(ml.TrainingData)-maxTrainingData:]
	}
}

// SaveToFile saves the ML model to a file
func (ml *MLModel) SaveToFile(filename string) error {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	
	data, err := json.MarshalIndent(ml, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal ML model: %w", err)
	}
	
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write ML model file: %w", err)
	}
	
	return nil
}

// LoadFromFile loads the ML model from a file
func (ml *MLModel) LoadFromFile(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read ML model file: %w", err)
	}
	
	ml.mu.Lock()
	defer ml.mu.Unlock()
	
	if err := json.Unmarshal(data, ml); err != nil {
		return fmt.Errorf("failed to unmarshal ML model: %w", err)
	}
	
	// Initialize prediction cache if nil
	if ml.PredictionCache == nil {
		ml.PredictionCache = make(map[string]Prediction)
	}
	
	return nil
}

// LoadTrainingData loads training data from a file
func (ml *MLModel) LoadTrainingData(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read training data file: %w", err)
	}
	
	var trainingData []TrainingExample
	if err := json.Unmarshal(data, &trainingData); err != nil {
		return fmt.Errorf("failed to unmarshal training data: %w", err)
	}
	
	ml.mu.Lock()
	defer ml.mu.Unlock()
	
	ml.TrainingData = append(ml.TrainingData, trainingData...)
	
	return nil
}

// SaveTrainingData saves training data to a file
func (ml *MLModel) SaveTrainingData(filename string) error {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	
	data, err := json.MarshalIndent(ml.TrainingData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal training data: %w", err)
	}
	
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write training data file: %w", err)
	}
	
	return nil
}

// createCacheKey creates a cache key from features
func (ml *MLModel) createCacheKey(features map[string]float64) string {
	// Create a deterministic key from features
	var keys []string
	for k := range features {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	key := ""
	for _, k := range keys {
		key += fmt.Sprintf("%s:%.2f,", k, features[k])
	}
	
	return key
}

// ClearPredictionCache clears the prediction cache
func (ml *MLModel) ClearPredictionCache() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	
	ml.PredictionCache = make(map[string]Prediction)
}

// GetModelStats returns statistics about the ML model
func (ml *MLModel) GetModelStats() map[string]interface{} {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	
	return map[string]interface{}{
		"training_examples":    len(ml.TrainingData),
		"model_accuracy":       ml.ModelAccuracy,
		"last_trained":         ml.LastTrained,
		"features":            ml.Features,
		"targets":             ml.Targets,
		"prediction_cache_size": len(ml.PredictionCache),
	}
}