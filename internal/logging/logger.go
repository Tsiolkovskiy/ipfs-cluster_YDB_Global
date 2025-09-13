package logging

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger interface for structured logging with trace support
type Logger interface {
	Debug(ctx context.Context, msg string, fields ...zap.Field)
	Info(ctx context.Context, msg string, fields ...zap.Field)
	Warn(ctx context.Context, msg string, fields ...zap.Field)
	Error(ctx context.Context, msg string, fields ...zap.Field)
	Fatal(ctx context.Context, msg string, fields ...zap.Field)
	
	With(fields ...zap.Field) Logger
	WithContext(ctx context.Context) Logger
	
	Sync() error
}

// ZapLogger implements Logger using Zap with OpenTelemetry integration
type ZapLogger struct {
	logger *zap.Logger
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `mapstructure:"level"`
	Format     string `mapstructure:"format"`
	OutputPath string `mapstructure:"output_path"`
	ErrorPath  string `mapstructure:"error_path"`
}

// NewLogger creates a new structured logger with OpenTelemetry integration
func NewLogger(config LoggingConfig) (Logger, error) {
	// Parse log level
	level, err := zapcore.ParseLevel(config.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %s: %w", config.Level, err)
	}

	// Configure encoder
	var encoderConfig zapcore.EncoderConfig
	var encoder zapcore.Encoder

	if config.Format == "json" {
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.TimeKey = "timestamp"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Configure output paths
	outputPath := config.OutputPath
	if outputPath == "" {
		outputPath = "stdout"
	}
	
	errorPath := config.ErrorPath
	if errorPath == "" {
		errorPath = "stderr"
	}

	// Create core
	core := zapcore.NewCore(
		encoder,
		zapcore.NewMultiWriteSyncer(getWriteSyncer(outputPath)),
		level,
	)

	// Create logger with caller info and stack trace
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	return &ZapLogger{logger: logger}, nil
}

// getWriteSyncer returns appropriate WriteSyncer for the given path
func getWriteSyncer(path string) zapcore.WriteSyncer {
	switch path {
	case "stdout":
		return zapcore.AddSync(os.Stdout)
	case "stderr":
		return zapcore.AddSync(os.Stderr)
	default:
		file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			// Fallback to stdout if file can't be opened
			return zapcore.AddSync(os.Stdout)
		}
		return zapcore.AddSync(file)
	}
}

// Debug logs a debug message with trace context
func (l *ZapLogger) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	l.logWithTrace(ctx, l.logger.Debug, msg, fields...)
}

// Info logs an info message with trace context
func (l *ZapLogger) Info(ctx context.Context, msg string, fields ...zap.Field) {
	l.logWithTrace(ctx, l.logger.Info, msg, fields...)
}

// Warn logs a warning message with trace context
func (l *ZapLogger) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	l.logWithTrace(ctx, l.logger.Warn, msg, fields...)
}

// Error logs an error message with trace context
func (l *ZapLogger) Error(ctx context.Context, msg string, fields ...zap.Field) {
	l.logWithTrace(ctx, l.logger.Error, msg, fields...)
}

// Fatal logs a fatal message with trace context and exits
func (l *ZapLogger) Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	l.logWithTrace(ctx, l.logger.Fatal, msg, fields...)
}

// With creates a child logger with additional fields
func (l *ZapLogger) With(fields ...zap.Field) Logger {
	return &ZapLogger{logger: l.logger.With(fields...)}
}

// WithContext creates a logger with trace information from context
func (l *ZapLogger) WithContext(ctx context.Context) Logger {
	fields := extractTraceFields(ctx)
	return l.With(fields...)
}

// Sync flushes any buffered log entries
func (l *ZapLogger) Sync() error {
	return l.logger.Sync()
}

// logWithTrace logs a message with trace information extracted from context
func (l *ZapLogger) logWithTrace(ctx context.Context, logFunc func(string, ...zap.Field), msg string, fields ...zap.Field) {
	traceFields := extractTraceFields(ctx)
	allFields := append(traceFields, fields...)
	logFunc(msg, allFields...)
}

// extractTraceFields extracts trace and span IDs from context
func extractTraceFields(ctx context.Context) []zap.Field {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return nil
	}

	spanContext := span.SpanContext()
	fields := []zap.Field{
		zap.String("trace_id", spanContext.TraceID().String()),
		zap.String("span_id", spanContext.SpanID().String()),
	}

	if spanContext.IsSampled() {
		fields = append(fields, zap.Bool("sampled", true))
	}

	return fields
}

// Global logger instance
var globalLogger Logger

// InitGlobalLogger initializes the global logger
func InitGlobalLogger(config LoggingConfig) error {
	logger, err := NewLogger(config)
	if err != nil {
		return err
	}
	globalLogger = logger
	return nil
}

// GetLogger returns the global logger instance
func GetLogger() Logger {
	if globalLogger == nil {
		// Fallback to a basic logger if not initialized
		logger, _ := NewLogger(LoggingConfig{
			Level:  "info",
			Format: "json",
		})
		globalLogger = logger
	}
	return globalLogger
}

// Convenience functions for global logger
func Debug(ctx context.Context, msg string, fields ...zap.Field) {
	GetLogger().Debug(ctx, msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...zap.Field) {
	GetLogger().Info(ctx, msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...zap.Field) {
	GetLogger().Warn(ctx, msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...zap.Field) {
	GetLogger().Error(ctx, msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	GetLogger().Fatal(ctx, msg, fields...)
}