package main

import "testing"

// TestPatternKill covers both directions the guard promises: pattern kills
// in command position refuse, and prose that merely mentions them passes.
// The negative direction is the one that keeps the guard trusted; a guard
// that cries wolf on a commit message gets ignored on the day it matters.
func TestPatternKill(t *testing.T) {
	t.Parallel()

	refuse := []struct{ cmd, want string }{
		{`pkill -f 'go test'`, "pkill"},
		{`killall -9 flow`, "killall"},
		{`/usr/bin/pkill flow`, "pkill"},
		{`sudo pkill -f fuzz`, "pkill"},
		{`cd /tmp && pkill -f x`, "pkill"},
		{`make check; killall go`, "killall"},
		{`ps aux | grep flow | xargs pkill`, "pkill"},
		{`VAR=1 pkill x`, "pkill"},
		{`timeout 5 pkill x`, "pkill"},
		{`(pkill x)`, "pkill"},
		{"echo start\npkill -f test", "pkill"},
		{`nohup killall worker`, "killall"},
	}
	for _, tt := range refuse {
		if got := patternKill(tt.cmd); got != tt.want {
			t.Errorf("patternKill(%q) = %q, want %q", tt.cmd, got, tt.want)
		}
	}

	allow := []string{
		``,
		`go test ./...`,
		`git commit -m "docs: explain why pkill -f is forbidden"`,
		`git commit -m 'never use killall here'`,
		`grep -rn pkill CLAUDE.md`,
		`grep -rn 'pkill -f' .`,
		`echo "use pkill responsibly"`,
		`echo pkill killall`,
		`ps -Ao pid,args | grep pkill`,
		`cat docs/pkill-notes.md`,
		`./scripts/pkill-helper --dry-run`,
		`kill -TERM 12345`,
		`kill $PID`,
	}
	for _, cmd := range allow {
		if got := patternKill(cmd); got != "" {
			t.Errorf("patternKill(%q) = %q, want no match", cmd, got)
		}
	}
}
