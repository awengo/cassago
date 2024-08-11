package cassago

import (
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.TraceLevel)
}

func GetFields(skip int) map[string]interface{} {
	pc, file, line, ok := runtime.Caller(skip)

	var method string
	detail := runtime.FuncForPC(pc)
	if ok && detail != nil {
		method = detail.Name()
	}

	pc, _, _, ok = runtime.Caller(skip + 1)
	var caller string
	detail = runtime.FuncForPC(pc)
	if ok && detail != nil {
		caller = detail.Name()
	}

	fields := map[string]interface{}{
		"file":   file,
		"line":   line,
		"method": method,
		"caller": caller,
	}

	return fields
}

func Trace(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Trace(args...)
}

func Debug(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Debug(args...)
}

func Warn(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Warn(args...)
}

func Error(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Error(args...)
}

func Info(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Info(args...)
}

func Panic(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Panic(args...)
}

func Fatal(args ...interface{}) {
	logrus.WithFields(GetFields(2)).Fatal(args...)
}

func Profiling(args ...interface{}) {
	for i := 1; i < 10; i++ {
		logrus.WithFields(GetFields(i)).Trace(args...)
	}
}

func StartProfile() time.Time {
	now := time.Now()
	logrus.WithFields(GetFields(2)).Info("Profiling start.")

	return now
}

func EndProfile(startTime time.Time) {
	fields := GetFields(2)
	fields["latency"] = time.Now().Sub(startTime).Microseconds()
	logrus.WithFields(fields).Info("Profiling end.")
}
