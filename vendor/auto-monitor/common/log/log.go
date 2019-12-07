package log

import (
	"fmt"
	"path"
	"strings"

	"github.com/astaxie/beego/logs"
)

var logInstance *logs.BeeLogger

const logChanLen = 4096

func init() {
	logInstance = logs.NewLogger(logChanLen)
	logInstance.SetLevel(logs.LevelError)
	logInstance.EnableFuncCallDepth(true)
	logInstance.SetLogFuncCallDepth(3)
}

func SetLog(l, typ, filePath string) {
	SetLevel(l)

	typ = strings.ToLower(typ)
	if typ == "file" {
		logPath := path.Join(filePath, "log.log")
		param := fmt.Sprintf(`{"filename":"%s"}`, logPath)
		logInstance.SetLogger("file", param)
	}
}

func SetLevel(l string) {
	l = strings.ToLower(l)
	level := logs.LevelError
	switch l {
	case "debug":
		level = logs.LevelDebug
	case "info":
		level = logs.LevelInfo
	case "warn":
		level = logs.LevelWarn
	case "error":
		level = logs.LevelError
	default:
	}

	logInstance.SetLevel(level)
}

func GetLevelString() string {
	level := ""
	l := logInstance.GetLevel()
	switch l {
	case logs.LevelDebug:
		level = "debug"
	case logs.LevelInfo:
		level = "info"
	case logs.LevelWarn:
		level = "warn"
	case logs.LevelError:
		level = "error"
	default:
	}
	return level
}

func IsDebug() bool {
	return logInstance.GetLevel() == logs.LevelDebug
}

func Error(e string) {
	logInstance.Error(e)
}

func Errorf(format string, v ...interface{}) {
	logInstance.Error(format, v...)
}

func Warn(e string) {
	logInstance.Warn(e)
}

func Warnf(format string, v ...interface{}) {
	logInstance.Warn(format, v...)
}

func Info(e string) {
	logInstance.Info(e)
}

func Infof(format string, v ...interface{}) {
	logInstance.Info(format, v...)
}

func Debug(err string) {
	logInstance.Debug(err)
}

func Debugf(format string, v ...interface{}) {
	logInstance.Debug(format, v...)
}
