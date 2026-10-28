package logger

import (
	"context"
	"log/slog"
	"os"
)

// NewSlog adapts a *slog.Logger, so hydra logs through the
// standard library, e.g. as JSON with slog.NewJSONHandler.
func NewSlog(l *slog.Logger) Logger {
	return &slogLogger{l: l, ctx: context.Background()}
}

type slogLogger struct {
	l   *slog.Logger
	ctx context.Context
}

func (s *slogLogger) Debug(msg string, fields ...Fields) {
	s.l.DebugContext(s.ctx, msg, attrs(fields)...)
}

func (s *slogLogger) Info(msg string, fields ...Fields) {
	s.l.InfoContext(s.ctx, msg, attrs(fields)...)
}

func (s *slogLogger) Warn(msg string, fields ...Fields) {
	s.l.WarnContext(s.ctx, msg, attrs(fields)...)
}

func (s *slogLogger) Error(err error, fields ...Fields) {
	s.l.ErrorContext(s.ctx, err.Error(), attrs(fields)...)
}

func (s *slogLogger) Fatal(err error, fields ...Fields) {
	s.l.ErrorContext(s.ctx, err.Error(), append(attrs(fields), slog.Bool("fatal", true))...)
	os.Exit(1)
}

func (s *slogLogger) With(fields Fields) Logger {
	return &slogLogger{l: s.l.With(attrs([]Fields{fields})...), ctx: s.ctx}
}

func (s *slogLogger) WithContext(ctx context.Context) Logger {
	return &slogLogger{l: s.l, ctx: ctx}
}

func attrs(fields []Fields) []any {
	var out []any
	for _, f := range fields {
		for k, v := range f {
			out = append(out, slog.Any(k, v))
		}
	}
	return out
}

var _ Logger = (*slogLogger)(nil)
