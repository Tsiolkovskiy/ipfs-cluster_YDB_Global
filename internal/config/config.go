package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds the application configuration
type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	Database   DatabaseConfig   `mapstructure:"database"`
	EventBus   EventBusConfig   `mapstructure:"eventbus"`
	Telemetry  TelemetryConfig  `mapstructure:"telemetry"`
	Security   SecurityConfig   `mapstructure:"security"`
	Logging    LoggingConfig    `mapstructure:"logging"`
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	GRPCPort     int           `mapstructure:"grpc_port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

// DatabaseConfig holds YDB configuration
type DatabaseConfig struct {
	Endpoint    string `mapstructure:"endpoint"`
	Database    string `mapstructure:"database"`
	Credentials string `mapstructure:"credentials"`
}

// EventBusConfig holds NATS configuration
type EventBusConfig struct {
	URL       string `mapstructure:"url"`
	ClusterID string `mapstructure:"cluster_id"`
	ClientID  string `mapstructure:"client_id"`
}

// TelemetryConfig holds telemetry configuration
type TelemetryConfig struct {
	Enabled           bool    `mapstructure:"enabled"`
	PrometheusPort    int     `mapstructure:"prometheus_port"`
	JaegerEndpoint    string  `mapstructure:"jaeger_endpoint"`
	ServiceName       string  `mapstructure:"service_name"`
	ServiceVersion    string  `mapstructure:"service_version"`
	SampleRate        float64 `mapstructure:"sample_rate"`
}

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	SPIFFESocketPath string   `mapstructure:"spiffe_socket_path"`
	TLSEnabled       bool     `mapstructure:"tls_enabled"`
	CertFile         string   `mapstructure:"cert_file"`
	KeyFile          string   `mapstructure:"key_file"`
	TrustDomain      string   `mapstructure:"trust_domain"`
	ServiceID        string   `mapstructure:"service_id"`
	AuthorizedIDs    []string `mapstructure:"authorized_ids"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	OutputPath string `mapstructure:"output_path"`
	ErrorPath  string `mapstructure:"error_path"`
}

// Load loads configuration from file and environment variables
func Load() (*Config, error) {
	return LoadFromFile("")
}

// LoadFromFile loads configuration from a specific file
func LoadFromFile(configFile string) (*Config, error) {
	v := viper.New()

	// Set defaults
	setDefaults(v)

	// Configure viper
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")
	v.AddConfigPath("./configs")
	v.AddConfigPath("/etc/gdc")

	// Use specific config file if provided
	if configFile != "" {
		v.SetConfigFile(configFile)
	}

	// Enable environment variable support
	v.SetEnvPrefix("GDC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Unmarshal config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.grpc_port", 9090)
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "30s")

	// Database defaults
	v.SetDefault("database.endpoint", "grpc://localhost:2136")
	v.SetDefault("database.database", "/local")

	// Event bus defaults
	v.SetDefault("eventbus.url", "nats://localhost:4222")
	v.SetDefault("eventbus.cluster_id", "gdc-cluster")
	v.SetDefault("eventbus.client_id", "gdc-server")

	// Telemetry defaults
	v.SetDefault("telemetry.enabled", true)
	v.SetDefault("telemetry.prometheus_port", 9091)
	v.SetDefault("telemetry.jaeger_endpoint", "")
	v.SetDefault("telemetry.service_name", "global-data-controller")
	v.SetDefault("telemetry.service_version", "1.0.0")
	v.SetDefault("telemetry.sample_rate", 1.0)

	// Security defaults
	v.SetDefault("security.spiffe_socket_path", "unix:///tmp/spire-agent/public/api.sock")
	v.SetDefault("security.tls_enabled", true)
	v.SetDefault("security.trust_domain", "gdc.local")
	v.SetDefault("security.service_id", "spiffe://gdc.local/gdc-server")
	v.SetDefault("security.authorized_ids", []string{})

	// Logging defaults
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.output_path", "stdout")
	v.SetDefault("logging.error_path", "stderr")
}