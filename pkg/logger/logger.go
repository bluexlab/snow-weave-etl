package logger

import (
	"context"
	"io"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
)

type SnowLogger struct {
	inner *logrus.Logger
}

func NewSnowLogger(logger *logrus.Logger) *SnowLogger {
	return &SnowLogger{inner: logger}
}

// SetLogLevel set logging level for calling SnowLogger
func (log *SnowLogger) SetLogLevel(level string) error {
	actualLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	log.inner.SetLevel(actualLevel)
	return nil
}

// WithContext return Entry to include fields in context
func (log *SnowLogger) WithContext(ctx context.Context) *logrus.Entry {
	fields := context2Fields(ctx)
	return log.inner.WithFields(*fields)
}

// WithField allocates a new entry and adds a field to it.
// Debug, Print, Info, Warn, Error, Fatal or Panic must be then applied to
// this new returned entry.
// If you want multiple fields, use `WithFields`.
func (log *SnowLogger) WithField(key string, value interface{}) *logrus.Entry {
	return log.inner.WithField(key, value)

}

// Adds a struct of fields to the log entry. All it does is call `WithField` for
// each `Field`.
func (log *SnowLogger) WithFields(fields logrus.Fields) *logrus.Entry {
	return log.inner.WithFields(fields)
}

// Add an error as single field to the log entry.  All it does is call
// `WithError` for the given `error`.
func (log *SnowLogger) WithError(err error) *logrus.Entry {
	return log.inner.WithError(err)
}

// Overrides the time of the log entry.
func (log *SnowLogger) WithTime(t time.Time) *logrus.Entry {
	return log.inner.WithTime(t)
}

func (log *SnowLogger) Logf(level logrus.Level, format string, args ...interface{}) {
	log.inner.Logf(level, format, args...)
}

func (log *SnowLogger) Tracef(format string, args ...interface{}) {
	log.inner.Tracef(format, args...)
}

func (log *SnowLogger) Debugf(format string, args ...interface{}) {
	log.inner.Debugf(format, args...)
}

func (log *SnowLogger) Infof(format string, args ...interface{}) {
	log.inner.Infof(format, args...)
}

func (log *SnowLogger) Printf(format string, args ...interface{}) {
	log.inner.Printf(format, args...)
}

func (log *SnowLogger) Warnf(format string, args ...interface{}) {
	log.inner.Warnf(format, args...)
}

func (log *SnowLogger) Warningf(format string, args ...interface{}) {
	log.inner.Warningf(format, args...)
}

func (log *SnowLogger) Errorf(format string, args ...interface{}) {
	log.inner.Errorf(format, args...)
}

func (log *SnowLogger) Fatalf(format string, args ...interface{}) {
	log.inner.Fatalf(format, args...)
}

func (log *SnowLogger) Panicf(format string, args ...interface{}) {
	log.inner.Panicf(format, args...)
}

func (log *SnowLogger) Log(level logrus.Level, args ...interface{}) {
	log.inner.Log(level, args...)
}

func (log *SnowLogger) LogFn(level logrus.Level, fn logrus.LogFunction) {
	log.inner.LogFn(level, fn)
}

func (log *SnowLogger) Trace(args ...interface{}) {
	log.inner.Trace(args...)
}

func (log *SnowLogger) Debug(args ...interface{}) {
	log.inner.Debug(args...)
}

func (log *SnowLogger) Info(args ...interface{}) {
	log.inner.Info(args...)
}

func (log *SnowLogger) Print(args ...interface{}) {
	log.inner.Print(args...)
}

func (log *SnowLogger) Warn(args ...interface{}) {
	log.inner.Warn(args...)
}

func (log *SnowLogger) Warning(args ...interface{}) {
	log.inner.Warning(args...)
}

func (log *SnowLogger) Error(args ...interface{}) {
	log.inner.Error(args...)
}

func (log *SnowLogger) Fatal(args ...interface{}) {
	log.inner.Fatal(args...)
}

func (log *SnowLogger) Panic(args ...interface{}) {
	log.inner.Panic(args...)
}

func (log *SnowLogger) TraceFn(fn logrus.LogFunction) {
	log.inner.TraceFn(fn)
}

func (log *SnowLogger) DebugFn(fn logrus.LogFunction) {
	log.inner.DebugFn(fn)
}

func (log *SnowLogger) InfoFn(fn logrus.LogFunction) {
	log.inner.InfoFn(fn)
}

func (log *SnowLogger) PrintFn(fn logrus.LogFunction) {
	log.inner.PrintFn(fn)
}

func (log *SnowLogger) WarnFn(fn logrus.LogFunction) {
	log.inner.PrintFn(fn)
}

func (log *SnowLogger) WarningFn(fn logrus.LogFunction) {
	log.inner.WarningFn(fn)
}

func (log *SnowLogger) ErrorFn(fn logrus.LogFunction) {
	log.inner.ErrorFn(fn)
}

func (log *SnowLogger) FatalFn(fn logrus.LogFunction) {
	log.inner.FatalFn(fn)
}

func (log *SnowLogger) PanicFn(fn logrus.LogFunction) {
	log.inner.PanicFn(fn)
}

func (log *SnowLogger) Logln(level logrus.Level, args ...interface{}) {
	log.inner.Logln(level, args...)
}

func (log *SnowLogger) Traceln(args ...interface{}) {
	log.inner.Traceln(args...)
}

func (log *SnowLogger) Debugln(args ...interface{}) {
	log.inner.Debugln(args...)
}

func (log *SnowLogger) Infoln(args ...interface{}) {
	log.inner.Infoln(args...)
}

func (log *SnowLogger) Println(args ...interface{}) {
	log.inner.Println(args...)
}

func (log *SnowLogger) Warnln(args ...interface{}) {
	log.inner.Warnln(args...)
}

func (log *SnowLogger) Warningln(args ...interface{}) {
	log.inner.Warningln(args...)
}

func (log *SnowLogger) Errorln(args ...interface{}) {
	log.inner.Errorln(args...)
}

func (log *SnowLogger) Fatalln(args ...interface{}) {
	log.inner.Fatalln(args...)
}

func (log *SnowLogger) Panicln(args ...interface{}) {
	log.inner.Panicln(args...)
}

func (log *SnowLogger) Exit(code int) {
	log.inner.Exit(code)
}

// SetLevel sets the logger level.
func (log *SnowLogger) SetLevel(level logrus.Level) {
	log.inner.SetLevel(level)
}

// GetLevel returns the logger level.
func (log *SnowLogger) GetLevel() logrus.Level {
	return log.inner.GetLevel()
}

// AddHook adds a hook to the logger hooks.
func (log *SnowLogger) AddHook(hook logrus.Hook) {
	log.inner.AddHook(hook)

}

// IsLevelEnabled checks if the log level of the logger is greater than the level param
func (log *SnowLogger) IsLevelEnabled(level logrus.Level) bool {
	return log.inner.IsLevelEnabled(level)
}

// SetFormatter sets the logger formatter.
func (log *SnowLogger) SetFormatter(formatter logrus.Formatter) {
	log.inner.SetFormatter(formatter)
}

// SetOutput sets the logger output.
func (log *SnowLogger) SetOutput(output io.Writer) {
	log.inner.SetOutput(output)
}

func (log *SnowLogger) SetReportCaller(reportCaller bool) {
	log.inner.SetReportCaller(reportCaller)
}

func context2Fields(ctx context.Context) *logrus.Fields {
	var fields = logrus.Fields{}
	if ctx == nil {
		return &fields
	}

	for i := 0; i < len(gosnowflake.LogKeys); i++ {
		if ctx.Value(gosnowflake.LogKeys[i]) != nil {
			fields[string(gosnowflake.LogKeys[i])] = ctx.Value(gosnowflake.LogKeys[i])
		}
	}
	return &fields
}
