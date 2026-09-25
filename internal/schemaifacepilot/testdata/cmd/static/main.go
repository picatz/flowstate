package main

import (
	"encoding/json"
	"os"
	"runtime"
	"time"

	"github.com/picatz/flowstate/internal/schemaifacepilot"
	"github.com/spf13/pflag"
)

func main() {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	_ = schemaifacepilot.NewStaticGetBinding(pflag.NewFlagSet("static", pflag.ContinueOnError))
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]uint64{
		"nanoseconds": uint64(elapsed), "allocations": after.Mallocs - before.Mallocs,
		"bytes": after.TotalAlloc - before.TotalAlloc,
	})
}
