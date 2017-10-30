/*
Then use the pprof tool to look at the heap profile:
go tool pprof http://localhost:6060/debug/pprof/heap

Or to look at a 30-second CPU profile:
go tool pprof http://localhost:6060/debug/pprof/profile

Or to look at the goroutine blocking profile, after calling runtime.SetBlockProfileRate in your program:
go tool pprof http://localhost:6060/debug/pprof/block

Or to collect a 5-second execution trace:
wget http://localhost:6060/debug/pprof/trace?seconds=5

To view all available profiles, open http://localhost:6060/debug/pprof/ in your browser.
For a study of the facility in action, visit

https://blog.golang.org/2011/06/profiling-go-programs.html
*/
package httpprof

import (
	"runtime"
	"log"
	"net/http"
	_ "net/http/pprof"
)

func init() {
	go func() {
		runtime.SetBlockProfileRate(1)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}
