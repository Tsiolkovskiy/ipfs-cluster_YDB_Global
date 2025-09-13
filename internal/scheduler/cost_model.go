package scheduler

import (
	"time"
)

// SimpleCostModel provides a basic implementation of the CostModel interface
type SimpleCostModel struct {
	// Cost per GB per month for storage in different zones
	StorageCosts map[string]float64
	
	// Cost per GB for egress between zones
	EgressCosts map[string]map[string]float64
	
	// Cost per operation in different zones
	ComputeCosts map[string]float64
	
	// Network latency between zones (in milliseconds)
	ZoneLatencies map[string]map[string]time.Duration
}

// NewSimpleCostModel creates a new simple cost model with default values
func NewSimpleCostModel() *SimpleCostModel {
	return &SimpleCostModel{
		StorageCosts: map[string]float64{
			"MSK": 0.023, // $0.023 per GB per month (Moscow)
			"NN":  0.025, // $0.025 per GB per month (Nizhny Novgorod)
			"SPB": 0.024, // $0.024 per GB per month (Saint Petersburg)
		},
		EgressCosts: map[string]map[string]float64{
			"MSK": {
				"NN":  0.05, // $0.05 per GB MSK -> NN
				"SPB": 0.04, // $0.04 per GB MSK -> SPB
			},
			"NN": {
				"MSK": 0.05, // $0.05 per GB NN -> MSK
				"SPB": 0.06, // $0.06 per GB NN -> SPB
			},
			"SPB": {
				"MSK": 0.04, // $0.04 per GB SPB -> MSK
				"NN":  0.06, // $0.06 per GB SPB -> NN
			},
		},
		ComputeCosts: map[string]float64{
			"MSK": 0.001, // $0.001 per operation
			"NN":  0.0008, // $0.0008 per operation
			"SPB": 0.0009, // $0.0009 per operation
		},
		ZoneLatencies: map[string]map[string]time.Duration{
			"MSK": {
				"NN":  15 * time.Millisecond,
				"SPB": 8 * time.Millisecond,
			},
			"NN": {
				"MSK": 15 * time.Millisecond,
				"SPB": 20 * time.Millisecond,
			},
			"SPB": {
				"MSK": 8 * time.Millisecond,
				"NN":  20 * time.Millisecond,
			},
		},
	}
}

// CalculateStorageCost calculates the monthly storage cost for data in a zone
func (c *SimpleCostModel) CalculateStorageCost(sizeBytes int64, zoneID string) float64 {
	sizeGB := float64(sizeBytes) / (1024 * 1024 * 1024)
	
	costPerGB, exists := c.StorageCosts[zoneID]
	if !exists {
		costPerGB = 0.025 // Default cost
	}
	
	return sizeGB * costPerGB
}

// CalculateEgressCost calculates the cost for transferring data between zones
func (c *SimpleCostModel) CalculateEgressCost(sizeBytes int64, fromZone, toZone string) float64 {
	if fromZone == toZone || fromZone == "" || toZone == "" {
		return 0.0 // No cost for same-zone or unspecified zones
	}
	
	sizeGB := float64(sizeBytes) / (1024 * 1024 * 1024)
	
	if fromCosts, exists := c.EgressCosts[fromZone]; exists {
		if costPerGB, exists := fromCosts[toZone]; exists {
			return sizeGB * costPerGB
		}
	}
	
	// Default egress cost if not specified
	return sizeGB * 0.05
}

// CalculateComputeCost calculates the cost for compute operations in a zone
func (c *SimpleCostModel) CalculateComputeCost(operations int, zoneID string) float64 {
	costPerOp, exists := c.ComputeCosts[zoneID]
	if !exists {
		costPerOp = 0.001 // Default cost
	}
	
	return float64(operations) * costPerOp
}

// GetZoneLatency returns the network latency between two zones
func (c *SimpleCostModel) GetZoneLatency(fromZone, toZone string) time.Duration {
	if fromZone == toZone {
		return 1 * time.Millisecond // Intra-zone latency
	}
	
	if fromLatencies, exists := c.ZoneLatencies[fromZone]; exists {
		if latency, exists := fromLatencies[toZone]; exists {
			return latency
		}
	}
	
	// Default latency if not specified
	return 50 * time.Millisecond
}

// UpdateStorageCost updates the storage cost for a zone
func (c *SimpleCostModel) UpdateStorageCost(zoneID string, costPerGB float64) {
	c.StorageCosts[zoneID] = costPerGB
}

// UpdateEgressCost updates the egress cost between two zones
func (c *SimpleCostModel) UpdateEgressCost(fromZone, toZone string, costPerGB float64) {
	if c.EgressCosts[fromZone] == nil {
		c.EgressCosts[fromZone] = make(map[string]float64)
	}
	c.EgressCosts[fromZone][toZone] = costPerGB
}

// UpdateComputeCost updates the compute cost for a zone
func (c *SimpleCostModel) UpdateComputeCost(zoneID string, costPerOp float64) {
	c.ComputeCosts[zoneID] = costPerOp
}

// UpdateZoneLatency updates the latency between two zones
func (c *SimpleCostModel) UpdateZoneLatency(fromZone, toZone string, latency time.Duration) {
	if c.ZoneLatencies[fromZone] == nil {
		c.ZoneLatencies[fromZone] = make(map[string]time.Duration)
	}
	c.ZoneLatencies[fromZone][toZone] = latency
}

// GetTotalMonthlyCost estimates the total monthly cost for storing data across zones
func (c *SimpleCostModel) GetTotalMonthlyCost(assignments []*NodeAssignment, sizeBytes int64) float64 {
	totalCost := 0.0
	
	for _, assignment := range assignments {
		totalCost += c.CalculateStorageCost(sizeBytes, assignment.ZoneID)
	}
	
	return totalCost
}

// GetAverageLatency calculates the average latency for accessing data from a reference zone
func (c *SimpleCostModel) GetAverageLatency(assignments []*NodeAssignment, referenceZone string) time.Duration {
	if len(assignments) == 0 {
		return 0
	}
	
	totalLatency := time.Duration(0)
	count := 0
	
	for _, assignment := range assignments {
		latency := c.GetZoneLatency(referenceZone, assignment.ZoneID)
		totalLatency += latency
		count++
	}
	
	return totalLatency / time.Duration(count)
}