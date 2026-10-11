package flowfile

// SameCompiledStepsForTest exposes the consolidation guard to the external tests.
func SameCompiledStepsForTest(a, b []byte) bool { return sameCompiledSteps(Env{}, a, b) }
