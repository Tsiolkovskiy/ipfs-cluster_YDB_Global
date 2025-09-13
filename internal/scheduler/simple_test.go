package scheduler

import (
	"testing"
	"time"
)

// Test helper functions

// TestParseIntOrDefault tests the parseIntOrDefault helper function
func TestParseIntOrDefault(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"1", 1},
		{"2", 2},
		{"3", 3},
		{"4", 4},
		{"invalid", 5}, // should return default
		{"", 5},        // should return default
	}

	for _, test := range tests {
		result := parseIntOrDefault(test.input, 5)
		if result != test.expected {
			t.Errorf("parseIntOrDefault(%s, 5) = %d, expected %d", test.input, result, test.expected)
		}
	}
}

// TestParseStringList tests the parseStringList helper function
func TestParseStringList(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"", []string{}},
		{"MSK", []string{"MSK"}},
		{"MSK,NN", []string{"MSK", "NN"}},
	}

	for _, test := range tests {
		result := parseStringList(test.input)
		if len(result) != len(test.expected) {
			t.Errorf("parseStringList(%s) length = %d, expected %d", test.input, len(result), len(test.expected))
			continue
		}

		for i, expected := range test.expected {
			if result[i] != expected {
				t.Errorf("parseStringList(%s)[%d] = %s, expected %s", test.input, i, result[i], expected)
			}
		}
	}
}

// TestCostEstimate_Calculation tests cost calculation
func TestCostEstimate_Calculation(t *testing.T) {
	cost := &CostEstimate{
		StorageCost: 0.023,
		EgressCost:  0.05,
		ComputeCost: 0.001,
	}
	cost.TotalCost = cost.StorageCost + cost.EgressCost + cost.ComputeCost

	expectedTotal := 0.023 + 0.05 + 0.001
	if cost.TotalCost != expectedTotal {
		t.Errorf("Expected total cost %f, got %f", expectedTotal, cost.TotalCost)
	}
}

// TestNodeAssignment_Structure tests NodeAssignment structure
func TestNodeAssignment_Structure(t *testing.T) {
	assignment := &NodeAssignment{
		NodeID:    "node-1",
		ClusterID: "cluster-1",
		ZoneID:    "MSK",
		Role:      "replica",
	}

	if assignment.NodeID != "node-1" {
		t.Errorf("Expected NodeID 'node-1', got '%s'", assignment.NodeID)
	}

	if assignment.ClusterID != "cluster-1" {
		t.Errorf("Expected ClusterID 'cluster-1', got '%s'", assignment.ClusterID)
	}

	if assignment.ZoneID != "MSK" {
		t.Errorf("Expected ZoneID 'MSK', got '%s'", assignment.ZoneID)
	}

	if assignment.Role != "replica" {
		t.Errorf("Expected Role 'replica', got '%s'", assignment.Role)
	}
}

// TestPlacementRequest_Validation tests PlacementRequest validation
func TestPlacementRequest_Validation(t *testing.T) {
	// Test valid request
	request := &PlacementRequest{
		CID:      "QmTest123",
		Size:     1024 * 1024, // 1MB
		Policies: []string{"default-rf-2"},
		Priority: 5,
	}

	if request.CID == "" {
		t.Error("CID should not be empty")
	}

	if request.Size <= 0 {
		t.Error("Size should be positive")
	}

	// Test invalid request
	invalidRequest := &PlacementRequest{
		CID:  "",
		Size: -1,
	}

	if invalidRequest.CID != "" {
		t.Error("Expected empty CID")
	}

	if invalidRequest.Size > 0 {
		t.Error("Size should be negative")
	}
}

// TestSimpleCostModel_CalculateStorageCost tests storage cost calculation
func TestSimpleCostModel_CalculateStorageCost(t *testing.T) {
	costModel := NewSimpleCostModel()

	// Test storage cost calculation
	sizeBytes := int64(1024 * 1024 * 1024) // 1GB
	cost := costModel.CalculateStorageCost(sizeBytes, "MSK")

	expectedCost := 0.023 // $0.023 per GB for MSK
	if cost != expectedCost {
		t.Errorf("Expected storage cost %f, got %f", expectedCost, cost)
	}

	// Test unknown zone (should use default)
	cost = costModel.CalculateStorageCost(sizeBytes, "UNKNOWN")
	expectedCost = 0.025 // Default cost
	if cost != expectedCost {
		t.Errorf("Expected default storage cost %f, got %f", expectedCost, cost)
	}
}

// TestSimpleCostModel_CalculateEgressCost tests egress cost calculation
func TestSimpleCostModel_CalculateEgressCost(t *testing.T) {
	costModel := NewSimpleCostModel()

	// Test egress cost calculation
	sizeBytes := int64(1024 * 1024 * 1024) // 1GB
	cost := costModel.CalculateEgressCost(sizeBytes, "MSK", "NN")

	expectedCost := 0.05 // $0.05 per GB MSK -> NN
	if cost != expectedCost {
		t.Errorf("Expected egress cost %f, got %f", expectedCost, cost)
	}

	// Test same zone (should be 0)
	cost = costModel.CalculateEgressCost(sizeBytes, "MSK", "MSK")
	if cost != 0.0 {
		t.Errorf("Expected 0 cost for same zone, got %f", cost)
	}
}

// TestSimpleCostModel_GetZoneLatency tests zone latency calculation
func TestSimpleCostModel_GetZoneLatency(t *testing.T) {
	costModel := NewSimpleCostModel()

	// Test known latency
	latency := costModel.GetZoneLatency("MSK", "NN")
	expectedLatency := 15 * time.Millisecond
	if latency != expectedLatency {
		t.Errorf("Expected latency %v, got %v", expectedLatency, latency)
	}

	// Test same zone
	latency = costModel.GetZoneLatency("MSK", "MSK")
	expectedLatency = 1 * time.Millisecond
	if latency != expectedLatency {
		t.Errorf("Expected intra-zone latency %v, got %v", expectedLatency, latency)
	}

	// Test unknown zones
	latency = costModel.GetZoneLatency("UNKNOWN1", "UNKNOWN2")
	expectedLatency = 50 * time.Millisecond // Default
	if latency != expectedLatency {
		t.Errorf("Expected default latency %v, got %v", expectedLatency, latency)
	}
}

// Benchmark tests for the cost model

// BenchmarkCostModel_CalculateStorageCost benchmarks storage cost calculation
func BenchmarkCostModel_CalculateStorageCost(b *testing.B) {
	costModel := NewSimpleCostModel()
	sizeBytes := int64(1024 * 1024 * 1024) // 1GB

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		costModel.CalculateStorageCost(sizeBytes, "MSK")
	}
}

// BenchmarkCostModel_CalculateEgressCost benchmarks egress cost calculation
func BenchmarkCostModel_CalculateEgressCost(b *testing.B) {
	costModel := NewSimpleCostModel()
	sizeBytes := int64(1024 * 1024 * 1024) // 1GB

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		costModel.CalculateEgressCost(sizeBytes, "MSK", "NN")
	}
}