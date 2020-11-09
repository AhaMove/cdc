package moresql

import (
	"fmt"
	"io"
	"os"

	"github.com/natefinch/lumberjack"
	log "github.com/sirupsen/logrus"
)

// WriterHook is a hook that writes logs of specified LogLevels to specified Writer
type WriterHook struct {
	Writer    io.Writer
	LogLevels []log.Level
}

// Fire will be called when some logging function is called with current hook
// It will format log entry to string and write it to appropriate writer
func (hook *WriterHook) Fire(entry *log.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	_, err = hook.Writer.Write([]byte(line))
	return err
}

// Levels define on which log levels this hook would trigger
func (hook *WriterHook) Levels() []log.Level {
	return hook.LogLevels
}

func SetupLogger(env Env) {
	// Alter logging pattern for heroku
	log.SetOutput(os.Stdout)
	formatter := &log.TextFormatter{
		FullTimestamp: true,
	}
	if os.Getenv("DYNO") != "" {
		formatter.FullTimestamp = false
		log.SetLevel(log.InfoLevel)
		log.SetFormatter(&log.JSONFormatter{})
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		l, err := log.ParseLevel(v)
		if err != nil {
			log.WithField("level", v).Warn("LOG_LEVEL invalid, choose from debug, info, warn, fatal.")
			os.Exit(1)
		}

		logPath := os.Getenv("LOG_PATH")
		if len(logPath) == 0 {
			logPath = "/var/log/cdc"
		}

		log.SetOutput(os.Stdout)

		accessLog := &lumberjack.Logger{
			Filename:   fmt.Sprintf("%s/access.log", logPath),
			MaxSize:    100, // Max megabytes before log is rotated
			MaxBackups: 90,  // Max number of old log files to keep
			MaxAge:     60,  // Max number of days to retain log files
			Compress:   true,
		}

		errorLog := &lumberjack.Logger{
			Filename:   fmt.Sprintf("%s/error.log", logPath),
			MaxSize:    100, // Max megabytes before log is rotated
			MaxBackups: 90,  // Max number of old log files to keep
			MaxAge:     60,  // Max number of days to retain log files
			Compress:   true,
		}

		log.AddHook(&WriterHook{ // Send info and debug logs to stdout
			Writer: accessLog,
			LogLevels: []log.Level{
				log.InfoLevel,
				log.DebugLevel,
				log.TraceLevel,
				log.WarnLevel,
			},
		})

		log.AddHook(&WriterHook{
			Writer: errorLog,
			LogLevels: []log.Level{
				log.ErrorLevel,
				log.FatalLevel,
				log.PanicLevel,
			},
		})

		log.SetLevel(l)
		log.SetFormatter(&log.TextFormatter{
			DisableColors: true,
			FullTimestamp: true,
		})
	}

	log.WithField("logLevel", log.GetLevel()).Debug("Log Settings")
}
