# Global Data Controller Build Script for Windows

param(
    [string]$Command = "help"
)

function Show-Help {
    Write-Host "Global Data Controller - Available commands:" -ForegroundColor Green
    Write-Host ""
    Write-Host "  build         Build the application binary" -ForegroundColor Cyan
    Write-Host "  test          Run all tests" -ForegroundColor Cyan
    Write-Host "  test-coverage Run tests with coverage" -ForegroundColor Cyan
    Write-Host "  run           Run the application locally" -ForegroundColor Cyan
    Write-Host "  clean         Clean build artifacts" -ForegroundColor Cyan
    Write-Host "  fmt           Format code" -ForegroundColor Cyan
    Write-Host "  vet           Run go vet" -ForegroundColor Cyan
    Write-Host "  mod-tidy      Tidy go modules" -ForegroundColor Cyan
    Write-Host "  docker-build  Build Docker image" -ForegroundColor Cyan
    Write-Host "  help          Show this help message" -ForegroundColor Cyan
}

function Build-App {
    Write-Host "Building Global Data Controller..." -ForegroundColor Green
    go build -o bin/gdc.exe ./cmd/gdc
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Build complete: bin/gdc.exe" -ForegroundColor Green
    } else {
        Write-Host "Build failed" -ForegroundColor Red
        exit 1
    }
}

function Test-App {
    Write-Host "Running tests..." -ForegroundColor Green
    go test -v ./...
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Tests failed" -ForegroundColor Red
        exit 1
    }
}

function Test-Coverage {
    Write-Host "Running tests with coverage..." -ForegroundColor Green
    go test -coverprofile=coverage.out ./...
    if ($LASTEXITCODE -eq 0) {
        go tool cover -html=coverage.out -o coverage.html
        Write-Host "Coverage report generated: coverage.html" -ForegroundColor Green
    } else {
        Write-Host "Coverage test failed" -ForegroundColor Red
        exit 1
    }
}

function Run-App {
    Write-Host "Running Global Data Controller..." -ForegroundColor Green
    go run ./cmd/gdc --config configs/config.yaml
}

function Clean-Artifacts {
    Write-Host "Cleaning build artifacts..." -ForegroundColor Green
    if (Test-Path "bin") {
        Remove-Item -Recurse -Force bin
    }
    if (Test-Path "coverage.out") {
        Remove-Item coverage.out
    }
    if (Test-Path "coverage.html") {
        Remove-Item coverage.html
    }
    go clean -cache -testcache -modcache
    Write-Host "Clean complete" -ForegroundColor Green
}

function Format-Code {
    Write-Host "Formatting code..." -ForegroundColor Green
    go fmt ./...
    Write-Host "Code formatted" -ForegroundColor Green
}

function Vet-Code {
    Write-Host "Running go vet..." -ForegroundColor Green
    go vet ./...
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Vet failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "Vet passed" -ForegroundColor Green
}

function Tidy-Modules {
    Write-Host "Tidying go modules..." -ForegroundColor Green
    go mod tidy
    go mod verify
    Write-Host "Modules tidied" -ForegroundColor Green
}

function Build-Docker {
    Write-Host "Building Docker image..." -ForegroundColor Green
    docker build -t gdc:latest .
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Docker image built: gdc:latest" -ForegroundColor Green
    } else {
        Write-Host "Docker build failed" -ForegroundColor Red
        exit 1
    }
}

# Main command dispatcher
switch ($Command.ToLower()) {
    "build" { Build-App }
    "test" { Test-App }
    "test-coverage" { Test-Coverage }
    "run" { Run-App }
    "clean" { Clean-Artifacts }
    "fmt" { Format-Code }
    "vet" { Vet-Code }
    "mod-tidy" { Tidy-Modules }
    "docker-build" { Build-Docker }
    "help" { Show-Help }
    default { 
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Show-Help
        exit 1
    }
}