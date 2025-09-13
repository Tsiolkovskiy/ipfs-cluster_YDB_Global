package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
)

func TestExtractResourceActionFromGRPCMethod(t *testing.T) {
	tests := []struct {
		method           string
		expectedResource string
		expectedAction   string
	}{
		{
			method:           "/gdc.v1.PolicyService/CreatePolicy",
			expectedResource: "policies",
			expectedAction:   "create",
		},
		{
			method:           "/gdc.v1.SchedulerService/ComputePlacement",
			expectedResource: "scheduler",
			expectedAction:   "compute",
		},
		{
			method:           "/gdc.v1.ClusterService/DrainCluster",
			expectedResource: "clusters",
			expectedAction:   "drain",
		},
		{
			method:           "/gdc.v1.HealthService/Check",
			expectedResource: "health",
			expectedAction:   "read",
		},
		{
			method:           "/invalid/method",
			expectedResource: "unknown",
			expectedAction:   "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			resource, action := extractResourceActionFromGRPCMethod(tt.method)
			assert.Equal(t, tt.expectedResource, resource)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestExtractResourceActionFromHTTPRequest(t *testing.T) {
	tests := []struct {
		method           string
		path             string
		expectedResource string
		expectedAction   string
	}{
		{
			method:           "GET",
			path:             "/api/v1/policies",
			expectedResource: "policies",
			expectedAction:   "read",
		},
		{
			method:           "POST",
			path:             "/api/v1/policies",
			expectedResource: "policies",
			expectedAction:   "create",
		},
		{
			method:           "PUT",
			path:             "/api/v1/policies/123",
			expectedResource: "policies",
			expectedAction:   "update",
		},
		{
			method:           "DELETE",
			path:             "/api/v1/clusters/cluster-1",
			expectedResource: "clusters",
			expectedAction:   "delete",
		},
		{
			method:           "GET",
			path:             "/health",
			expectedResource: "health",
			expectedAction:   "read",
		},
		{
			method:           "GET",
			path:             "/",
			expectedResource: "unknown",
			expectedAction:   "read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_"+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			resource, action := extractResourceActionFromHTTPRequest(req)
			assert.Equal(t, tt.expectedResource, resource)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestMapServiceToResource(t *testing.T) {
	tests := []struct {
		serviceName      string
		expectedResource string
	}{
		{"PolicyService", "policies"},
		{"SchedulerService", "scheduler"},
		{"ClusterService", "clusters"},
		{"HealthService", "health"},
		{"UnknownService", "unknown"},
		{"CustomService", "custom"},
	}

	for _, tt := range tests {
		t.Run(tt.serviceName, func(t *testing.T) {
			resource := mapServiceToResource(tt.serviceName)
			assert.Equal(t, tt.expectedResource, resource)
		})
	}
}

func TestMapMethodToAction(t *testing.T) {
	tests := []struct {
		methodName     string
		expectedAction string
	}{
		{"CreatePolicy", "create"},
		{"GetPolicy", "read"},
		{"ListPolicies", "read"},
		{"UpdatePolicy", "update"},
		{"DeletePolicy", "delete"},
		{"ComputePlacement", "compute"},
		{"StartWorkflow", "start"},
		{"Check", "read"},
		{"UnknownMethod", "unknown"},
		{"CustomCreate", "create"},
		{"CustomGet", "read"},
	}

	for _, tt := range tests {
		t.Run(tt.methodName, func(t *testing.T) {
			action := mapMethodToAction(tt.methodName)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestMapHTTPMethodToAction(t *testing.T) {
	tests := []struct {
		method         string
		hasID          bool
		expectedAction string
	}{
		{"GET", false, "read"},
		{"GET", true, "read"},
		{"POST", false, "create"},
		{"PUT", true, "update"},
		{"PATCH", true, "update"},
		{"DELETE", true, "delete"},
		{"HEAD", false, "read"},
		{"OPTIONS", false, "read"},
		{"UNKNOWN", false, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			action := mapHTTPMethodToAction(tt.method, tt.hasID)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expectedIP string
	}{
		{
			name: "X-Forwarded-For header",
			headers: map[string]string{
				"X-Forwarded-For": "192.168.1.1, 10.0.0.1",
			},
			remoteAddr: "127.0.0.1:12345",
			expectedIP: "192.168.1.1",
		},
		{
			name: "X-Real-IP header",
			headers: map[string]string{
				"X-Real-IP": "192.168.1.2",
			},
			remoteAddr: "127.0.0.1:12345",
			expectedIP: "192.168.1.2",
		},
		{
			name:       "RemoteAddr fallback",
			headers:    map[string]string{},
			remoteAddr: "192.168.1.3:12345",
			expectedIP: "192.168.1.3",
		},
		{
			name:       "RemoteAddr without port",
			headers:    map[string]string{},
			remoteAddr: "192.168.1.4",
			expectedIP: "192.168.1.4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remoteAddr
			
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			ip := getClientIP(req)
			assert.Equal(t, tt.expectedIP, ip)
		})
	}
}



func TestContextServerStream(t *testing.T) {
	// This is a simple test for the contextServerStream wrapper
	originalCtx := context.Background()
	newCtx := context.WithValue(originalCtx, "test_key", "test_value")

	// Create a mock server stream (we'll use nil for simplicity in this test)
	wrapper := &contextServerStream{
		ServerStream: nil, // In real usage, this would be a proper grpc.ServerStream
		ctx:          newCtx,
	}

	// Test that Context() returns our custom context
	retrievedCtx := wrapper.Context()
	assert.Equal(t, newCtx, retrievedCtx)
	assert.Equal(t, "test_value", retrievedCtx.Value("test_key"))
}

// Integration test for HTTP middleware chain
func TestHTTPMiddlewareIntegration(t *testing.T) {
	// This test would require a full SPIFFE setup, so we'll create a mock version
	// In a real integration test, you would set up SPIRE server/agent

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, ok := GetIdentityFromContext(r.Context())
		if !ok {
			http.Error(w, "No identity", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello " + identity.Subject))
	})

	// Create a test request
	req := httptest.NewRequest("GET", "/api/v1/policies", nil)
	rr := httptest.NewRecorder()

	// Simulate adding identity to context (normally done by auth middleware)
	spiffeID, _ := spiffeid.FromString("spiffe://gdc.local/test")
	identity := &Identity{
		ID:       "test",
		Subject:  "test",
		SPIFFEID: spiffeID,
		Claims: map[string]string{
			"spiffe_id": spiffeID.String(),
		},
	}
	
	ctx := context.WithValue(req.Context(), IdentityContextKey, identity)
	req = req.WithContext(ctx)

	// Call handler
	handler.ServeHTTP(rr, req)

	// Check response
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "Hello test", rr.Body.String())
}