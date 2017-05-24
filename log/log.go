package log

import (
	"fmt"
	"github.com/astaxie/beego/logs"
)

var (
	Logger *logs.BeeLogger
)

func init() {
	Logger = logs.NewLogger(1)
}

func SetLogger(s_log *logs.BeeLogger) {
	Logger = s_log
	Logger.GetLogFuncCallDepth()
}

func SetFileLogger(file string, maxDays int) {
	Logger.SetLogger("file",
		fmt.Sprintf(`{"filename":"%s","maxDays":%d}`, file, maxDays),
	)
	Logger.EnableFuncCallDepth(true)
	Logger.SetLevel(logs.LevelInformational)
	Logger.DelLogger("console")
}

func SetLoggerDebug() {
	Logger.SetLevel(logs.LevelDebug)
	Logger.SetLogger("console", "")
}
