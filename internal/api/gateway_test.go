package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/global-data-controller/gdc/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// MockServices implements the Services interface for testing
type MockServices struct {
	mock.Mock
}

func (m *MockServices) GetZones(ctx context.Context) ([]*models.Zone, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*models.Zone), args.Error(1)
}

func (m *MockServices) GetZone(ctx context.Context, id string) (*models.Zone, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Zone), args.Error(1)
}

func (m *MockServices) CreateZone(ctx context.Context, zone *models.Zone) error {
	args := m.Called(ctx, zone)
	return args.Error(0)
}

func (m *MockServices) UpdateZone(ctx context.Context, id string, zone *models.Zone) error {
	args := m.Called(ctx, id, zone)
	return args.Error(0)
}

func (m *MockServices) DeleteZone(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockServices) GetClusters(ctx context.Context, zoneID string) ([]*models.Cluster, error) {
	args := m.Called(ctx, zoneID)
	return args.Get(0).([]*models.Cluster), args.Error(1)
}

func (m *MockServices) GetCluster(ctx context.Context, id string) (*models.Cluster, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Cluster), args.Error(1)
}

func (m *MockServices) CreateCluster(ctx context.Context, cluster *models.Cluster) error {
	args := m.Called(ctx, cluster)
	return args.Error(0)
}

func (m *MockServices) UpdateCluster(ctx context.Context, id string, cluster *models.Cluster) error {
	args := m.Called(ctx, id, cluster)
	return args.Error(0)
}

func (m *MockServices) DeleteCluster(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockServices) PinContent(ctx context.Context, request *PinRequest) (*models.PinPlan, error) {
	args := m.Called(ctx, request)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.PinPlan), args.Error(1)
}

func (m *MockServices) UnpinContent(ctx context.Context, cid string) error {
	args := m.Called(ctx, cid)
	return args.Error(0)
}

func (m *MockServices) GetContentStatus(ctx context.Context, cid string) (*models.PinStatus, error) {
	args := m.Called(ctx, cid)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.PinStatus), args.Error(1)
}

func (m *MockServices) ListContent(ctx context.Context, filter *ContentFilter) ([]*models.Content, error) {
	args := m.Called(ctx, filter)
	return args.Get(0).([]*models.Content), args.Error(1)
}

func (m *MockServices) GetPolicies(ctx context.Context) ([]*models.Policy, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*models.Policy), args.Error(1)
}

func (m *MockServices) GetPolicy(ctx context.Context, id string) (*models.Policy, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Policy), args.Error(1)
}

func (m *MockServices) CreatePolicy(ctx context.Context, policy *models.Policy) error {
	args := m.Called(ctx, policy)
	return args.Error(0)
}

func (m *MockServices) UpdatePolicy(ctx context.Context, id string, policy *models.Policy) error {
	args := m.Called(ctx, id, policy)
	return args.Error(0)
}

func (m *MockServices) DeletePolicy(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockServices) GetTopology(ctx context.Context) (*models.Topology, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Topology), args.Error(1)
}

// Test setup helper
func setupTestGateway() (*HTTPGateway, *MockServices) {
	mockServices := &MockServices{}
	logger := zap.NewNop()
	gateway := NewHTTPGateway(mockServices, logger)
	return gateway, mockServices
}

// Test Health endpoint
func TestHealthHandler(t *testing.T) {
	gateway, _ := setupTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	gateway.HealthHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "healthy", response["status"])
	assert.Equal(t, "global-data-controller", response["service"])
	assert.NotEmpty(t, response["timestamp"])
}

func TestHealthHandler_MethodNotAllowed(t *testing.T) {
	gateway, _ := setupTestGateway()

	req := httptest.NewRequest(http.MethodPost, "/health", nil)
	w := httptest.NewRecorder()

	gateway.HealthHandler(w, req)

	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

// Test Ready endpoint
func TestReadyHandler(t *testing.T) {
	gateway, _ := setupTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	gateway.ReadyHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "ready", response["status"])
}

// Test Zone endpoints
func TestListZones(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	zones := []*models.Zone{
		{
			ID:     "zone-1",
			Name:   "Moscow",
			Region: "RU-MSK",
			Status: models.ZoneStatusActive,
		},
		{
			ID:     "zone-2",
			Name:   "Nizhny Novgorod",
			Region: "RU-NN",
			Status: models.ZoneStatusActive,
		},
	}

	mockServices.On("GetZones", mock.Anything).Return(zones, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/zones", nil)
	w := httptest.NewRecorder()

	gateway.listZones(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, float64(2), response["count"])
	assert.NotNil(t, response["zones"])

	mockServices.AssertExpectations(t)
}

func TestCreateZone(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	zone := &models.Zone{
		Name:   "Test Zone",
		Region: "TEST",
		Status: models.ZoneStatusActive,
	}

	mockServices.On("CreateZone", mock.Anything, mock.MatchedBy(func(z *models.Zone) bool {
		return z.Name == "Test Zone" && z.Region == "TEST"
	})).Return(nil)

	body, _ := json.Marshal(zone)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/zones", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	gateway.createZone(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
	mockServices.AssertExpectations(t)
}

func TestCreateZone_ValidationError(t *testing.T) {
	gateway, _ := setupTestGateway()

	// Zone without required name
	zone := &models.Zone{
		Region: "TEST",
	}

	body, _ := json.Marshal(zone)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/zones", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	gateway.createZone(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "VALIDATION_ERROR", response.Code)
}

func TestGetZone(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	zone := &models.Zone{
		ID:     "zone-1",
		Name:   "Test Zone",
		Region: "TEST",
		Status: models.ZoneStatusActive,
	}

	mockServices.On("GetZone", mock.Anything, "zone-1").Return(zone, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/zones/zone-1", nil)
	w := httptest.NewRecorder()

	gateway.getZone(w, req, "zone-1")

	assert.Equal(t, http.StatusOK, w.Code)

	var response models.Zone
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "zone-1", response.ID)
	assert.Equal(t, "Test Zone", response.Name)

	mockServices.AssertExpectations(t)
}

func TestGetZone_NotFound(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	mockServices.On("GetZone", mock.Anything, "nonexistent").Return(nil, fmt.Errorf("zone not found"))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/zones/nonexistent", nil)
	w := httptest.NewRecorder()

	gateway.getZone(w, req, "nonexistent")

	assert.Equal(t, http.StatusNotFound, w.Code)
	mockServices.AssertExpectations(t)
}

// Test Content endpoints
func TestPinContent(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	pinRequest := &PinRequest{
		CID:      "QmTest123",
		Size:     1024,
		Policies: []string{"default"},
		Priority: 1,
	}

	expectedPlan := &models.PinPlan{
		ID:  "plan-1",
		CID: "QmTest123",
		Assignments: []*models.NodeAssignment{
			{
				NodeID:    "node-1",
				ClusterID: "cluster-1",
				ZoneID:    "zone-1",
				Priority:  1,
			},
		},
	}

	mockServices.On("PinContent", mock.Anything, mock.MatchedBy(func(req *PinRequest) bool {
		return req.CID == "QmTest123"
	})).Return(expectedPlan, nil)

	body, _ := json.Marshal(pinRequest)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/content/pin", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	gateway.pinContentHandler(w, req)

	assert.Equal(t, http.StatusAccepted, w.Code)

	var response models.PinPlan
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "QmTest123", response.CID)

	mockServices.AssertExpectations(t)
}

func TestPinContent_ValidationError(t *testing.T) {
	gateway, _ := setupTestGateway()

	// Pin request without CID
	pinRequest := &PinRequest{
		Size: 1024,
	}

	body, _ := json.Marshal(pinRequest)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/content/pin", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	gateway.pinContentHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "VALIDATION_ERROR", response.Code)
}

func TestGetContentStatus(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	status := &models.PinStatus{
		CID:      "QmTest123",
		Status:   models.ContentStatusPinned,
		Progress: 1.0,
	}

	mockServices.On("GetContentStatus", mock.Anything, "QmTest123").Return(status, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/content/status/QmTest123", nil)
	w := httptest.NewRecorder()

	gateway.contentStatusHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response models.PinStatus
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "QmTest123", response.CID)
	assert.Equal(t, models.ContentStatusPinned, response.Status)

	mockServices.AssertExpectations(t)
}

// Test Rate Limiting
func TestRateLimiting(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	// Set a very low rate limit for testing
	gateway.limiter = NewRateLimiter(1, 1) // 1 request per second, burst of 1

	mockServices.On("GetZones", mock.Anything).Return([]*models.Zone{}, nil).Maybe()

	// First request should succeed
	req1 := httptest.NewRequest(http.MethodGet, "/api/v1/zones", nil)
	req1.RemoteAddr = "127.0.0.1:12345"
	w1 := httptest.NewRecorder()

	gateway.rateLimitMiddleware(gateway.listZones)(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	// Second request immediately should be rate limited
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/zones", nil)
	req2.RemoteAddr = "127.0.0.1:12345"
	w2 := httptest.NewRecorder()

	gateway.rateLimitMiddleware(gateway.listZones)(w2, req2)
	assert.Equal(t, http.StatusTooManyRequests, w2.Code)

	var response ErrorResponse
	err := json.Unmarshal(w2.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "RATE_LIMIT_EXCEEDED", response.Code)
}

// Test CORS middleware
func TestCORSMiddleware(t *testing.T) {
	gateway, _ := setupTestGateway()

	req := httptest.NewRequest(http.MethodOptions, "/api/v1/zones", nil)
	w := httptest.NewRecorder()

	gateway.corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, POST, PUT, DELETE, OPTIONS", w.Header().Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "Content-Type, Authorization", w.Header().Get("Access-Control-Allow-Headers"))
}

// Test OpenAPI specification
func TestOpenAPISpec(t *testing.T) {
	gateway, _ := setupTestGateway()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/openapi.json", nil)
	w := httptest.NewRecorder()

	gateway.openAPISpecHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var spec map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &spec)
	assert.NoError(t, err)
	assert.Equal(t, "3.0.3", spec["openapi"])
	assert.NotNil(t, spec["info"])
	assert.NotNil(t, spec["paths"])
	assert.NotNil(t, spec["components"])
}

// Test validation functions
func TestValidateZone(t *testing.T) {
	gateway, _ := setupTestGateway()

	tests := []struct {
		name    string
		zone    *models.Zone
		wantErr bool
	}{
		{
			name: "valid zone",
			zone: &models.Zone{
				Name:   "Test Zone",
				Region: "TEST",
				Status: models.ZoneStatusActive,
			},
			wantErr: false,
		},
		{
			name: "missing name",
			zone: &models.Zone{
				Region: "TEST",
			},
			wantErr: true,
		},
		{
			name: "missing region",
			zone: &models.Zone{
				Name: "Test Zone",
			},
			wantErr: true,
		},
		{
			name: "invalid status",
			zone: &models.Zone{
				Name:   "Test Zone",
				Region: "TEST",
				Status: "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := gateway.validateZone(tt.zone)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidatePinRequest(t *testing.T) {
	gateway, _ := setupTestGateway()

	tests := []struct {
		name    string
		request *PinRequest
		wantErr bool
	}{
		{
			name: "valid request",
			request: &PinRequest{
				CID:      "QmTest123",
				Size:     1024,
				Priority: 1,
			},
			wantErr: false,
		},
		{
			name: "missing CID",
			request: &PinRequest{
				Size: 1024,
			},
			wantErr: true,
		},
		{
			name: "negative size",
			request: &PinRequest{
				CID:  "QmTest123",
				Size: -1,
			},
			wantErr: true,
		},
		{
			name: "negative priority",
			request: &PinRequest{
				CID:      "QmTest123",
				Priority: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := gateway.validatePinRequest(tt.request)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Test client IP extraction
func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expected   string
	}{
		{
			name: "X-Forwarded-For header",
			headers: map[string]string{
				"X-Forwarded-For": "192.168.1.1, 10.0.0.1",
			},
			remoteAddr: "127.0.0.1:12345",
			expected:   "192.168.1.1",
		},
		{
			name: "X-Real-IP header",
			headers: map[string]string{
				"X-Real-IP": "192.168.1.1",
			},
			remoteAddr: "127.0.0.1:12345",
			expected:   "192.168.1.1",
		},
		{
			name:       "RemoteAddr fallback",
			headers:    map[string]string{},
			remoteAddr: "127.0.0.1:12345",
			expected:   "127.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			ip := getClientIP(req)
			assert.Equal(t, tt.expected, ip)
		})
	}
}

// Integration test for full request flow
func TestFullRequestFlow(t *testing.T) {
	gateway, mockServices := setupTestGateway()

	// Setup mock expectations
	zones := []*models.Zone{
		{
			ID:     "zone-1",
			Name:   "Moscow",
			Region: "RU-MSK",
			Status: models.ZoneStatusActive,
		},
	}
	mockServices.On("GetZones", mock.Anything).Return(zones, nil)

	// Create a test server
	server := httptest.NewServer(gateway.mux)
	defer server.Close()

	// Make a request to the test server
	resp, err := http.Get(server.URL + "/api/v1/zones")
	assert.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	// Check CORS headers
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))

	var response map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&response)
	assert.NoError(t, err)
	assert.Equal(t, float64(1), response["count"])

	mockServices.AssertExpectations(t)
}