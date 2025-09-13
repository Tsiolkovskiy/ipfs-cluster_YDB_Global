package bootstrap

import (
	"context"
	"fmt"

	"github.com/global-data-controller/gdc/internal/config"
	"github.com/global-data-controller/gdc/internal/logging"
	"github.com/global-data-controller/gdc/internal/telemetry"
	"go.uber.org/zap"
)

// Bootstrap initializes the core system components
type Bootstrap struct {
	Config    *config.Config
	Logger    logging.Logger
	Telemetry *telemetry.Telemetry
}

// New creates a new bootstrap instance
func New() *Bootstrap {
	return &Bootstrap{}
}

// Initialize initializes all core components
func (b *Bootstrap) Initialize(ctx context.Context, configFile string) error {
	// Load configuration
	cfg, err := b.loadConfig(configFile)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	b.Config = cfg

	// Initialize logging
	logger, err := b.initLogging(cfg.Logging)
	if err != nil {
		return fmt.Errorf("failed to initialize logging: %w", err)
	}
	b.Logger = logger

	logger.Info(ctx, "Configuration loaded successfully",
		zap.String("config_file", configFile),
		zap.String("log_level", cfg.Logging.Level),
		zap.String("log_format", cfg.Logging.Format))

	// Initialize telemetry
	tel, err := b.initTelemetry(cfg.Telemetry)
	if err != nil {
		logger.Error(ctx, "Failed to initialize telemetry", zap.Error(err))
		return fmt.Errorf("failed to initialize telemetry: %w", err)
	}
	b.Telemetry = tel

	if cfg.Telemetry.Enabled {
		logger.Info(ctx, "Telemetry initialized successfully",
			zap.String("service_name", cfg.Telemetry.ServiceName),
			zap.String("service_version", cfg.Telemetry.ServiceVersion),
			zap.Int("prometheus_port", cfg.Telemetry.PrometheusPort),
			zap.Float64("sample_rate", cfg.Telemetry.SampleRate))
	} else {
		logger.Info(ctx, "Telemetry is disabled")
	}

	return nil
}

// Start starts all initialized components
func (b *Bootstrap) Start(ctx context.Context) error {
	if b.Logger == nil {
		return fmt.Errorf("bootstrap not initialized")
	}

	b.Logger.Info(ctx, "Starting Global Data Controller components")

	// Start telemetry
	if b.Telemetry != nil {
		if err := b.Telemetry.Start(ctx); err != nil {
			b.Logger.Error(ctx, "Failed to start telemetry", zap.Error(err))
			return fmt.Errorf("failed to start telemetry: %w", err)
		}
		b.Logger.Info(ctx, "Telemetry started successfully")
	}

	b.Logger.Info(ctx, "All components started successfully")
	return nil
}

// Stop stops all components gracefully
func (b *Bootstrap) Stop(ctx context.Context) error {
	if b.Logger == nil {
		return nil
	}

	b.Logger.Info(ctx, "Stopping Global Data Controller components")

	// Stop telemetry
	if b.Telemetry != nil {
		if err := b.Telemetry.Stop(ctx); err != nil {
			b.Logger.Error(ctx, "Failed to stop telemetry", zap.Error(err))
			return fmt.Errorf("failed to stop telemetry: %w", err)
		}
		b.Logger.Info(ctx, "Telemetry stopped successfully")
	}

	// Sync logger
	if err := b.Logger.Sync(); err != nil {
		// Don't return error for sync failures as they're often benign
		fmt.Printf("Failed to sync logger: %v\n", err)
	}

	b.Logger.Info(ctx, "All components stopped successfully")
	return nil
}

// loadConfig loads the configuration from file and environment
func (b *Bootstrap) loadConfig(configFile string) (*config.Config, error) {
	if configFile != "" {
		return config.LoadFromFile(configFile)
	}
	return config.Load()
}

// initLogging initializes the logging system
func (b *Bootstrap) initLogging(cfg config.LoggingConfig) (logging.Logger, error) {
	loggingConfig := logging.LoggingConfig{
		Level:      cfg.Level,
		Format:     cfg.Format,
		OutputPath: cfg.OutputPath,
		ErrorPath:  cfg.ErrorPath,
	}

	logger, err := logging.NewLogger(loggingConfig)
	if err != nil {
		return nil, err
	}

	// Initialize global logger
	if err := logging.InitGlobalLogger(loggingConfig); err != nil {
		return nil, fmt.Errorf("failed to initialize global logger: %w", err)
	}

	return logger, nil
}

// initTelemetry initializes the telemetry system
func (b *Bootstrap) initTelemetry(cfg config.TelemetryConfig) (*telemetry.Telemetry, error) {
	telemetryConfig := telemetry.TelemetryConfig{
		Enabled:        cfg.Enabled,
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		PrometheusPort: cfg.PrometheusPort,
		JaegerEndpoint: cfg.JaegerEndpoint,
		SampleRate:     cfg.SampleRate,
	}

	tel, err := telemetry.NewTelemetry(telemetryConfig)
	if err != nil {
		return nil, err
	}

	// Initialize global telemetry
	if err := telemetry.InitGlobalTelemetry(telemetryConfig); err != nil {
		return nil, fmt.Errorf("failed to initialize global telemetry: %w", err)
	}

	return tel, nil
}

// GetConfig returns the loaded configuration
func (b *Bootstrap) GetConfig() *config.Config {
	return b.Config
}

// GetLogger returns the initialized logger
func (b *Bootstrap) GetLogger() logging.Logger {
	return b.Logger
}

// GetTelemetry returns the initialized telemetry
func (b *Bootstrap) GetTelemetry() *telemetry.Telemetry {
	return b.Telemetry
}