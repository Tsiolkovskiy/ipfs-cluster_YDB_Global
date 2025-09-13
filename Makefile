# Global Data Controller Makefile

# Variables
BINARY_NAME=gdc
DOCKER_IMAGE=gdc:latest
GO_VERSION=1.21
COVERAGE_FILE=coverage.out

# Default target
.DEFAULT_GOAL := help

# Help target
.PHONY: help
help: ## Show this help message
	@echo "Global Data Controller - Available commands:"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Build targets
.PHONY: build
build: ## Build the application binary
	@echo "Building $(BINARY_NAME)..."
	@go build -o bin/$(BINARY_NAME) ./cmd/gdc
	@echo "Build complete: bin/$(BINARY_NAME)"

.PHONY: build-linux
build-linux: ## Build the application binary for Linux
	@echo "Building $(BINARY_NAME) for Linux..."
	@GOOS=linux GOARCH=amd64 go build -o bin/$(BINARY_NAME)-linux ./cmd/gdc
	@echo "Build complete: bin/$(BINARY_NAME)-linux"

.PHONY: build-docker
build-docker: ## Build Docker image
	@echo "Building Docker image $(DOCKER_IMAGE)..."
	@docker build -t $(DOCKER_IMAGE) .
	@echo "Docker image built: $(DOCKER_IMAGE)"

# Development targets
.PHONY: run
run: ## Run the application locally
	@echo "Running $(BINARY_NAME)..."
	@go run ./cmd/gdc --config configs/config.yaml

.PHONY: dev
dev: ## Start development environment with dependencies
	@echo "Starting development environment..."
	@docker-compose up -d ydb nats prometheus grafana jaeger redis
	@echo "Development environment started. Services available at:"
	@echo "  - YDB: http://localhost:8765"
	@echo "  - NATS: http://localhost:8222"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - Grafana: http://localhost:3000 (admin/admin)"
	@echo "  - Jaeger: http://localhost:16686"

.PHONY: dev-full
dev-full: build-docker ## Start full development environment including GDC
	@echo "Starting full development environment..."
	@docker-compose --profile full up -d
	@echo "Full environment started. GDC available at http://localhost:8080"

.PHONY: dev-stop
dev-stop: ## Stop development environment
	@echo "Stopping development environment..."
	@docker-compose down
	@echo "Development environment stopped"

.PHONY: dev-clean
dev-clean: ## Clean development environment (remove volumes)
	@echo "Cleaning development environment..."
	@docker-compose down -v --remove-orphans
	@docker system prune -f
	@echo "Development environment cleaned"

# Testing targets
.PHONY: test
test: ## Run all tests
	@echo "Running tests..."
	@go test -v ./...

.PHONY: test-race
test-race: ## Run tests with race detection
	@echo "Running tests with race detection..."
	@go test -race -v ./...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	@go test -coverprofile=$(COVERAGE_FILE) ./...
	@go tool cover -html=$(COVERAGE_FILE) -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: benchmark
benchmark: ## Run benchmarks
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

# Code quality targets
.PHONY: lint
lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run ./...

.PHONY: fmt
fmt: ## Format code
	@echo "Formatting code..."
	@go fmt ./...
	@goimports -w .

.PHONY: vet
vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

.PHONY: mod-tidy
mod-tidy: ## Tidy go modules
	@echo "Tidying go modules..."
	@go mod tidy
	@go mod verify

# Database targets
.PHONY: db-init
db-init: ## Initialize database schema
	@echo "Initializing database schema..."
	@go run ./scripts/db-init.go

.PHONY: db-migrate
db-migrate: ## Run database migrations
	@echo "Running database migrations..."
	@go run ./scripts/db-migrate.go

.PHONY: db-seed
db-seed: ## Seed database with test data
	@echo "Seeding database with test data..."
	@go run ./scripts/db-seed.go

# YDB targets
.PHONY: ydb-start
ydb-start: ## Start local YDB instance
	@echo "Starting local YDB instance..."
	@docker-compose -f docker-compose.ydb.yml up -d
	@echo "YDB started. Web UI available at http://localhost:8765"

.PHONY: ydb-stop
ydb-stop: ## Stop local YDB instance
	@echo "Stopping local YDB instance..."
	@docker-compose -f docker-compose.ydb.yml down

.PHONY: ydb-setup
ydb-setup: ydb-start ## Setup YDB schema
	@echo "Waiting for YDB to be ready..."
	@sleep 10
	@echo "Setting up YDB schema..."
	@YDB_CONNECTION_STRING="grpc://localhost:2136/local" go run ./scripts/setup-ydb-schema.go grpc://localhost:2136/local

.PHONY: ydb-test
ydb-test: ## Run YDB integration tests
	@echo "Running YDB integration tests..."
	@YDB_CONNECTION_STRING="grpc://localhost:2136/local" go test -v ./internal/storage/...

.PHONY: ydb-test-setup
ydb-test-setup: ydb-setup ydb-test ## Setup YDB and run integration tests
	@echo "YDB integration tests completed"

.PHONY: ydb-clean
ydb-clean: ## Clean YDB data and containers
	@echo "Cleaning YDB environment..."
	@docker-compose -f docker-compose.ydb.yml down -v
	@docker system prune -f

# Deployment targets
.PHONY: deploy-local
deploy-local: build-docker ## Deploy to local environment
	@echo "Deploying to local environment..."
	@docker-compose --profile full up -d

.PHONY: generate
generate: ## Generate code (protobuf, mocks, etc.)
	@echo "Generating code..."
	@go generate ./...

# Monitoring targets
.PHONY: logs
logs: ## Show application logs
	@docker-compose logs -f gdc

.PHONY: logs-all
logs-all: ## Show all service logs
	@docker-compose logs -f

.PHONY: status
status: ## Show service status
	@docker-compose ps

# Cleanup targets
.PHONY: clean
clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -f $(COVERAGE_FILE) coverage.html
	@go clean -cache -testcache -modcache

.PHONY: clean-docker
clean-docker: ## Clean Docker images and containers
	@echo "Cleaning Docker artifacts..."
	@docker-compose down --remove-orphans
	@docker rmi $(DOCKER_IMAGE) 2>/dev/null || true
	@docker system prune -f

# Install targets
.PHONY: install-tools
install-tools: ## Install development tools
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install golang.org/x/tools/cmd/goimports@latest
	@go install github.com/golang/mock/mockgen@latest

# Health check targets
.PHONY: health
health: ## Check application health
	@echo "Checking application health..."
	@curl -f http://localhost:8080/health || echo "Application not healthy"

.PHONY: ready
ready: ## Check application readiness
	@echo "Checking application readiness..."
	@curl -f http://localhost:8080/ready || echo "Application not ready"

# SPIRE/SPIFFE targets
.PHONY: spire-start
spire-start: ## Start SPIRE server and agent
	@echo "Starting SPIRE infrastructure..."
	@docker-compose -f docker-compose.spire.yml up -d spire-server spire-agent
	@echo "SPIRE infrastructure started. Waiting for services to be ready..."
	@sleep 10

.PHONY: spire-stop
spire-stop: ## Stop SPIRE server and agent
	@echo "Stopping SPIRE infrastructure..."
	@docker-compose -f docker-compose.spire.yml down

.PHONY: spire-setup
spire-setup: spire-start ## Setup SPIRE with registration entries
	@echo "Setting up SPIRE registration entries..."
	@powershell -ExecutionPolicy Bypass -File scripts/setup-spire-entries.ps1
	@echo "SPIRE setup complete"

.PHONY: spire-status
spire-status: ## Check SPIRE status
	@echo "Checking SPIRE server status..."
	@docker exec gdc-spire-server /opt/spire/bin/spire-server healthcheck -socketPath /tmp/spire-server/private/api.sock || echo "SPIRE server not healthy"
	@echo "Checking SPIRE agent status..."
	@docker exec gdc-spire-agent /opt/spire/bin/spire-agent healthcheck -socketPath /tmp/spire-agent/public/api.sock || echo "SPIRE agent not healthy"

.PHONY: spire-entries
spire-entries: ## List SPIRE registration entries
	@echo "SPIRE registration entries:"
	@docker exec gdc-spire-server /opt/spire/bin/spire-server entry show -socketPath /tmp/spire-server/private/api.sock

.PHONY: spire-agents
spire-agents: ## List SPIRE agents
	@echo "SPIRE agents:"
	@docker exec gdc-spire-server /opt/spire/bin/spire-server agent list -socketPath /tmp/spire-server/private/api.sock

.PHONY: spire-clean
spire-clean: ## Clean SPIRE data and containers
	@echo "Cleaning SPIRE environment..."
	@docker-compose -f docker-compose.spire.yml down -v
	@docker system prune -f

.PHONY: spire-logs
spire-logs: ## Show SPIRE logs
	@echo "SPIRE server logs:"
	@docker logs gdc-spire-server --tail 50
	@echo "SPIRE agent logs:"
	@docker logs gdc-spire-agent --tail 50

.PHONY: dev-spiffe
dev-spiffe: spire-setup ## Start development environment with SPIFFE authentication
	@echo "Starting development environment with SPIFFE..."
	@docker-compose -f docker-compose.dev.yml -f docker-compose.spire.yml --profile spiffe up -d
	@echo "Development environment with SPIFFE started"
	@echo "Services available at:"
	@echo "  - GDC HTTPS (mTLS): https://localhost:8443"
	@echo "  - GDC gRPC (mTLS): localhost:9443"
	@echo "  - SPIRE Server: localhost:8081"

.PHONY: test-spiffe
test-spiffe: ## Run SPIFFE integration tests
	@echo "Running SPIFFE integration tests..."
	@go test -tags=integration -v ./internal/auth/...

.PHONY: test-spiffe-setup
test-spiffe-setup: spire-setup test-spiffe ## Setup SPIRE and run integration tests
	@echo "SPIFFE integration tests completed"

# All-in-one targets
.PHONY: setup
setup: install-tools mod-tidy ## Setup development environment
	@echo "Development environment setup complete"

.PHONY: setup-spiffe
setup-spiffe: setup spire-setup ## Setup development environment with SPIFFE
	@echo "Development environment with SPIFFE setup complete"

.PHONY: ci
ci: fmt vet lint test-race ## Run CI pipeline locally
	@echo "CI pipeline completed successfully"

.PHONY: ci-spiffe
ci-spiffe: fmt vet lint test-race test-spiffe-setup ## Run CI pipeline with SPIFFE tests
	@echo "CI pipeline with SPIFFE tests completed successfully"

.PHONY: all
all: clean fmt vet lint test build ## Run full build pipeline
	@echo "Full build pipeline completed successfully"

.PHONY: all-spiffe
all-spiffe: clean fmt vet lint test test-spiffe-setup build ## Run full build pipeline with SPIFFE
	@echo "Full build pipeline with SPIFFE completed successfully"