---
description: Bounded fast test tier for the inner loop, plus the fuzz recipe from CLAUDE.md
---

Run the bounded fast tier — short tests only, with a memory ceiling:

```
GOMEMLIMIT=1GiB go test -short -timeout 120s ./...
```

For fuzzing a specific target, bound both time and memory, and run single-
threaded so parallel workers don't multiply memory use:

```
GOMEMLIMIT=512MiB go test -timeout 120s -parallel 1 -run=XXX -fuzz FuzzName -fuzztime 60s ./path/
```

`-fuzztime` bounds time, not memory — a fuzzer's whole purpose is to find the
input that explodes, so the `GOMEMLIMIT` and `-parallel 1` matter even for a
short run. If a run behaves oddly, check whether the test binary actually
exited:

```
ps -Ao pid,rss,args | grep -E '\.test|-fuzz' | grep -v grep
```
