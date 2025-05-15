package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// DefaultLogger 默认日志实例
	DefaultLogger *zap.Logger
)

// Config 日志配置
type Config struct {
	// Level 日志级别: debug, info, warn, error
	Level string
	// Output 日志输出: stdout, file
	Output string
	// FilePath 日志文件路径
	FilePath string
}

// Init 初始化日志系统
func Init(config Config) error {
	var level zapcore.Level
	switch config.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	var writeSyncer zapcore.WriteSyncer
	if config.Output == "file" && config.FilePath != "" {
		file, err := os.OpenFile(config.FilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		writeSyncer = zapcore.AddSync(file)
	} else {
		writeSyncer = zapcore.AddSync(os.Stdout)
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		level,
	)

	DefaultLogger = zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	zap.ReplaceGlobals(DefaultLogger)

	return nil
}

// Debug 输出Debug级别日志
func Debug(msg string, fields ...zap.Field) {
	DefaultLogger.Debug(msg, fields...)
}

// Info 输出Info级别日志
func Info(msg string, fields ...zap.Field) {
	DefaultLogger.Info(msg, fields...)
}

// Warn 输出Warn级别日志
func Warn(msg string, fields ...zap.Field) {
	DefaultLogger.Warn(msg, fields...)
}

// Error 输出Error级别日志
func Error(msg string, fields ...zap.Field) {
	DefaultLogger.Error(msg, fields...)
}

// Fatal 输出Fatal级别日志并终止程序
func Fatal(msg string, fields ...zap.Field) {
	DefaultLogger.Fatal(msg, fields...)
}

// With 返回带有指定字段的日志记录器
func With(fields ...zap.Field) *zap.Logger {
	return DefaultLogger.With(fields...)
}
