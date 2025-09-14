package resource

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
)

// AccessPredictor uses ML algorithms to predict future access patterns
type AccessPredictor struct {
	config      *TieredConfig
	model       *MLModel
	trainingData []*TrainingDataPoint
	lastUpdate  time.Time
}

// MLModel represents a machine learning model for access prediction
type MLModel struct {
	ModelType   string                 `json:"model_type"`
	Parameters  map[string]float64     `json:"parameters"`
	Features    []string               `json:"features"`
	Accuracy    float64                `json:"accuracy"`
	TrainedAt   time.Time              `json:"trained_at"`
	Version     int                    `json:"version"`
}

// TrainingDataPoint represents a data point for ML training
type TrainingDataPoint struct {
	CID             string    `json:"cid"`
	Timestamp       time.Time `json:"timestamp"`
	Features        []float64 `json:"features"`
	AccessFrequency float64   `json:"access_frequency"`
	OptimalTier     string    `json:"optimal_tier"`
}

// PredictionResult represents the result of an access prediction
type PredictionResult struct {
	CID                 string    `json:"cid"`
	PredictedFrequency  float64   `json:"predicted_frequency"`
	PredictedTier       string    `json:"predicted_tier"`
	Confidence          float64   `json:"confidence"`
	PredictionHorizon   time.Duration `json:"prediction_horizon"`
	GeneratedAt         time.Time `json:"generated_at"`
}

// NewAccessPredictor creates a new access predictor
func NewAccessPredictor(config *TieredConfig) *AccessPredictor {
	return &AccessPredictor{
		config:       config,
		model:        &MLModel{ModelType: "linear_regression"},
		trainingData: make([]*TrainingDataPoint, 0),
		lastUpdate:   time.Now(),
	}
}

// Initialize initializes the ML model
func (ap *AccessPredictor) Initialize(ctx context.Context) error {
	tieredLogger.Info("Initializing access prediction model")

	// Initialize model parameters
	ap.model.Parameters = map[string]float64{
		"frequency_weight":    1.0,
		"recency_weight":      0.8,
		"seasonality_weight":  0.6,
		"trend_weight":        0.7,
		"size_weight":         0.3,
	}

	ap.model.Features = []string{
		"access_frequency",
		"recency_hours",
		"seasonality_variance",
		"trend_slope",
		"data_size_log",
		"age_days",
	}

	ap.model.TrainedAt = time.Now()
	ap.model.Version = 1

	tieredLogger.Info("Access prediction model initialized")
	return nil
}

// UpdateModel updates the ML model with new training data
func (ap *AccessPredictor) UpdateModel(ctx context.Context, accessHistory map[string]*AccessHistory) error {
	if time.Since(ap.lastUpdate) < ap.config.ModelUpdateInterval {
		return nil // Too soon to update
	}

	tieredLogger.Info("Updating access prediction model")

	// Generate training data from access history
	newTrainingData := ap.generateTrainingData(accessHistory)
	
	// Add to existing training data
	ap.trainingData = append(ap.trainingData, newTrainingData...)
	
	// Limit training data size
	if len(ap.trainingData) > ap.config.TrainingDataSize {
		// Keep most recent data
		sort.Slice(ap.trainingData, func(i, j int) bool {
			return ap.trainingData[i].Timestamp.After(ap.trainingData[j].Timestamp)
		})
		ap.trainingData = ap.trainingData[:ap.config.TrainingDataSize]
	}

	// Train the model
	if err := ap.trainModel(); err != nil {
		return errors.Wrap(err, "training model")
	}

	ap.lastUpdate = time.Now()
	ap.model.TrainedAt = time.Now()
	ap.model.Version++

	tieredLogger.Infof("Model updated successfully (version %d, %d training points)", 
		ap.model.Version, len(ap.trainingData))
	return nil
}

// PredictAccess predicts future access patterns for a data item
func (ap *AccessPredictor) PredictAccess(dataItem *DataItem, accessHistory *AccessHistory) (*PredictionResult, error) {
	if ap.model == nil {
		return nil, fmt.Errorf("model not initialized")
	}

	// Extract features
	features := ap.extractFeatures(dataItem, accessHistory)
	
	// Make prediction using the model
	predictedFrequency := ap.predict(features)
	
	// Determine predicted tier based on frequency
	predictedTier := ap.frequencyToTier(predictedFrequency)
	
	// Calculate confidence based on model accuracy and feature quality
	confidence := ap.calculateConfidence(features)

	return &PredictionResult{
		CID:                dataItem.CID,
		PredictedFrequency: predictedFrequency,
		PredictedTier:      predictedTier,
		Confidence:         confidence,
		PredictionHorizon:  ap.config.PredictionHorizon,
		GeneratedAt:        time.Now(),
	}, nil
}

// GetModelMetrics returns metrics about the ML model
func (ap *AccessPredictor) GetModelMetrics() map[string]interface{} {
	return map[string]interface{}{
		"model_type":        ap.model.ModelType,
		"version":           ap.model.Version,
		"accuracy":          ap.model.Accuracy,
		"training_points":   len(ap.trainingData),
		"last_trained":      ap.model.TrainedAt,
		"last_updated":      ap.lastUpdate,
		"features":          ap.model.Features,
	}
}

// Private methods

func (ap *AccessPredictor) generateTrainingData(accessHistory map[string]*AccessHistory) []*TrainingDataPoint {
	var trainingData []*TrainingDataPoint

	for cid, history := range accessHistory {
		if len(history.AccessTimes) < 2 {
			continue // Need at least 2 access points
		}

		// Calculate access frequency
		windowDuration := history.WindowEnd.Sub(history.WindowStart)
		if windowDuration <= 0 {
			continue
		}

		accessFrequency := float64(len(history.AccessTimes)) * float64(24*time.Hour) / float64(windowDuration)

		// Create training data point
		dataPoint := &TrainingDataPoint{
			CID:             cid,
			Timestamp:       time.Now(),
			AccessFrequency: accessFrequency,
			OptimalTier:     ap.frequencyToTier(accessFrequency),
		}

		// Extract features (simplified version for training)
		dataPoint.Features = []float64{
			accessFrequency,
			float64(time.Since(history.AccessTimes[len(history.AccessTimes)-1])) / float64(time.Hour),
			ap.calculateSeasonalityVariance(history),
			ap.calculateTrend(history),
			math.Log10(float64(ap.calculateAverageAccessSize(history)) + 1),
			float64(time.Since(history.AccessTimes[0])) / float64(24*time.Hour),
		}

		trainingData = append(trainingData, dataPoint)
	}

	return trainingData
}

func (ap *AccessPredictor) extractFeatures(dataItem *DataItem, accessHistory *AccessHistory) []float64 {
	features := make([]float64, len(ap.model.Features))

	dataItem.mu.RLock()
	accessPattern := dataItem.AccessPattern
	size := dataItem.Size
	createdAt := dataItem.CreatedAt
	dataItem.mu.RUnlock()

	// Feature 0: access_frequency
	features[0] = accessPattern.Frequency

	// Feature 1: recency_hours
	features[1] = float64(accessPattern.Recency) / float64(time.Hour)

	// Feature 2: seasonality_variance
	features[2] = ap.calculateSeasonalityVarianceFromPattern(accessPattern.Seasonality)

	// Feature 3: trend_slope
	features[3] = accessPattern.Trend

	// Feature 4: data_size_log
	features[4] = math.Log10(float64(size) + 1)

	// Feature 5: age_days
	features[5] = float64(time.Since(createdAt)) / float64(24*time.Hour)

	return features
}

func (ap *AccessPredictor) trainModel() error {
	if len(ap.trainingData) < 10 {
		return fmt.Errorf("insufficient training data: %d points", len(ap.trainingData))
	}

	// Simple linear regression implementation
	// In a real implementation, this would use a proper ML library

	// For now, just update model accuracy based on training data consistency
	ap.model.Accuracy = ap.calculateModelAccuracy()

	return nil
}

func (ap *AccessPredictor) predict(features []float64) float64 {
	if len(features) != len(ap.model.Features) {
		return 1.0 // Default prediction
	}

	// Simple weighted sum prediction
	prediction := 0.0
	weights := []float64{1.0, -0.1, 0.3, 0.5, 0.2, -0.05} // Simplified weights

	for i, feature := range features {
		if i < len(weights) {
			prediction += feature * weights[i]
		}
	}

	// Ensure positive prediction
	if prediction < 0 {
		prediction = 0.1
	}

	return prediction
}

func (ap *AccessPredictor) frequencyToTier(frequency float64) string {
	if frequency > 10 {
		return "hot"
	} else if frequency > 1 {
		return "warm"
	} else if frequency > 0.1 {
		return "cold"
	} else {
		return "archive"
	}
}

func (ap *AccessPredictor) calculateConfidence(features []float64) float64 {
	// Simple confidence calculation based on feature completeness and model accuracy
	featureCompleteness := 1.0
	for _, feature := range features {
		if math.IsNaN(feature) || math.IsInf(feature, 0) {
			featureCompleteness -= 0.1
		}
	}

	if featureCompleteness < 0 {
		featureCompleteness = 0
	}

	return ap.model.Accuracy * featureCompleteness
}

func (ap *AccessPredictor) calculateSeasonalityVariance(history *AccessHistory) float64 {
	if len(history.AccessTimes) < 24 {
		return 0.5 // Default variance
	}

	// Calculate hourly access distribution
	hourlyAccesses := make([]int, 24)
	for _, accessTime := range history.AccessTimes {
		hour := accessTime.Hour()
		hourlyAccesses[hour]++
	}

	// Calculate variance
	mean := float64(len(history.AccessTimes)) / 24.0
	variance := 0.0
	for _, count := range hourlyAccesses {
		diff := float64(count) - mean
		variance += diff * diff
	}
	variance /= 24.0

	return math.Sqrt(variance) / mean // Coefficient of variation
}

func (ap *AccessPredictor) calculateSeasonalityVarianceFromPattern(seasonality []float64) float64 {
	if len(seasonality) == 0 {
		return 0.5
	}

	mean := 0.0
	for _, value := range seasonality {
		mean += value
	}
	mean /= float64(len(seasonality))

	variance := 0.0
	for _, value := range seasonality {
		diff := value - mean
		variance += diff * diff
	}
	variance /= float64(len(seasonality))

	if mean > 0 {
		return math.Sqrt(variance) / mean
	}
	return 0.5
}

func (ap *AccessPredictor) calculateTrend(history *AccessHistory) float64 {
	if len(history.AccessTimes) < 4 {
		return 0.0
	}

	// Simple trend calculation: compare first and second half
	midPoint := len(history.AccessTimes) / 2
	firstHalf := float64(midPoint)
	secondHalf := float64(len(history.AccessTimes) - midPoint)

	if firstHalf > 0 {
		return (secondHalf - firstHalf) / firstHalf
	}
	return 0.0
}

func (ap *AccessPredictor) calculateAverageAccessSize(history *AccessHistory) int64 {
	if len(history.AccessSizes) == 0 {
		return 1024 // Default 1KB
	}

	total := int64(0)
	for _, size := range history.AccessSizes {
		total += size
	}

	return total / int64(len(history.AccessSizes))
}

func (ap *AccessPredictor) calculateModelAccuracy() float64 {
	if len(ap.trainingData) < 10 {
		return 0.5 // Default accuracy
	}

	// Simple accuracy calculation based on prediction consistency
	// In a real implementation, this would use cross-validation
	
	correct := 0
	total := 0

	for _, dataPoint := range ap.trainingData {
		predicted := ap.predict(dataPoint.Features)
		predictedTier := ap.frequencyToTier(predicted)
		
		if predictedTier == dataPoint.OptimalTier {
			correct++
		}
		total++
	}

	if total > 0 {
		return float64(correct) / float64(total)
	}
	return 0.5
}