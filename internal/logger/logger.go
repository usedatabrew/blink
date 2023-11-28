package logger

import (
	"github.com/charmbracelet/log"
	"os"
	"time"

	"sync"
)

var (
	once     sync.Once
	instance *log.Logger
)

func GetInstance() *log.Logger {
	once.Do(func() {
		instance = log.NewWithOptions(os.Stdout, log.Options{
			ReportCaller:    true,
			ReportTimestamp: true,
			TimeFormat:      time.DateTime,
			Prefix:          "blink",
		})
	})

	return instance
}
