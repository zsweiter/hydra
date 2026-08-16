package logger

import (
	"context"
	"fmt"
	"os"
)

// Fields is an alias for structured logging fields
type Fields map[string]any

// Logger defines a minimal, fast logging interface
type Logger interface {
	// Debug logs a debug message with optional fields
	Debug(msg string, fields ...Fields)

	// Info logs an info message with optional fields
	Info(msg string, fields ...Fields)

	// Warn logs a warning message with optional fields
	Warn(msg string, fields ...Fields)

	// Error logs an error with optional fields
	Error(err error, fields ...Fields)

	// Fatal logs a fatal error and exits
	Fatal(err error, fields ...Fields)

	// With returns a logger with preset fields
	With(fields Fields) Logger

	// WithContext returns a logger with context values
	WithContext(ctx context.Context) Logger
}

// Level represents log levels
type Level int8

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	default:
		return "info"
	}
}

func ParseLevel(s string) Level {
	switch s {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn":
		return WarnLevel
	case "error":
		return ErrorLevel
	case "fatal":
		return FatalLevel
	default:
		return InfoLevel
	}
}

func (l Level) IsEnabled(level Level) bool {
	return l <= level
}

// create fmt logger that implements Logger interface
func NewDefaultLogger() Logger {
	return &fmtLogger{
		level: InfoLevel,
	}
}

type fmtLogger struct {
	level Level
}

func (l *fmtLogger) Debug(msg string, fields ...Fields) {
	if l.level.IsEnabled(DebugLevel) {
		fmt.Printf("[DEBUG] %s %v\n", msg, fields)
	}
}

func (l *fmtLogger) Info(msg string, fields ...Fields) {
	if l.level.IsEnabled(InfoLevel) {
		fmt.Printf("[INFO] %s %v\n", msg, fields)
	}
}

func (l *fmtLogger) Warn(msg string, fields ...Fields) {
	if l.level.IsEnabled(WarnLevel) {
		fmt.Printf("[WARN] %s %v\n", msg, fields)
	}
}

func (l *fmtLogger) Error(err error, fields ...Fields) {
	if l.level.IsEnabled(ErrorLevel) {
		fmt.Printf("[ERROR] %s: %v %v\n", err.Error(), err, fields)
	}
}

func (l *fmtLogger) Fatal(err error, fields ...Fields) {
	if l.level.IsEnabled(FatalLevel) {
		fmt.Printf("[FATAL] %s: %v %v\n", err.Error(), err, fields)
		os.Exit(1)
	}
}

func (l *fmtLogger) With(fields Fields) Logger {
	return &fmtLogger{
		level: l.level,
	}
}

func (l *fmtLogger) WithContext(ctx context.Context) Logger {
	return &fmtLogger{
		level: l.level,
	}
}

var DefaultLogger Logger = NewDefaultLogger()

var _ Logger = (*fmtLogger)(nil)
