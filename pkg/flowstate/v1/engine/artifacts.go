package engine

import (
	"context"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// releaseArtifactsActivity is the name the run-end release is registered
// under. Kept stable because histories record the activity type name.
const releaseArtifactsActivity = "ReleaseArtifacts"

// artifactReleaseChange gates the release command. It is recorded only by a run
// whose specification declares `workspace:` or `produce:`, which no run written
// before artifacts existed does, so the marker never enters an old history; the
// gate exists so that a later change to how the release is scheduled has a
// version to move from.
const artifactReleaseChange = "artifact-release"

// artifactReleaseTimeout bounds the release. It deletes a handful of pin files,
// and a store that cannot do that in a minute is not going to.
const artifactReleaseTimeout = time.Minute

// artifactReleaseSummary labels the release in history.
const artifactReleaseSummary = "release the run's artifact pins"

// ReleaseArtifacts lets go of everything a run pinned in one tenant's store.
//
// A worker with no artifact store has nothing to release and succeeds: the
// activity may be taken by a worker other than the one that holds the pins, and
// a refusal there would fail a finished run over bookkeeping. The pins on the
// worker that does hold them are released by the next `flow artifacts gc
// --release`, which is the documented backstop.
func (a taskActivities) ReleaseArtifacts(ctx context.Context, namespace, runKey string) error {
	if a.configured.artifacts == nil {
		return nil
	}

	return v1.ReleaseArtifacts(ctx, a.configured.artifacts, namespace, runKey)
}

// releaseRunArtifacts schedules the release when a run that used artifacts
// ends.
//
// Not when the segment ends in Continue-As-New: that is the same run going on,
// and the pins it holds are the key to the next segment's inputs. Run on a
// context that survives the run's cancellation, since a cancelled run must let
// go of what it pinned, and never changing the run's result: a release that
// fails leaves blobs pinned, which is a storage cost and not a reason to report
// a finished run as failed.
func releaseRunArtifacts(ctx workflow.Context, st *v1.RunState, runErr error) {
	if runErr != nil && workflow.IsContinueAsNewError(runErr) {
		return
	}
	if !v1.WorkflowUsesArtifacts(st.GetWorkflow()) {
		return
	}

	key, err := v1.ArtifactRunKey(runAddress(ctx))
	if err != nil {
		return
	}

	workflow.GetVersion(ctx, artifactReleaseChange, workflow.DefaultVersion, 1)

	release, _ := workflow.NewDisconnectedContext(ctx)
	release = workflow.WithActivityOptions(release, workflow.ActivityOptions{
		StartToCloseTimeout: artifactReleaseTimeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	if err := workflow.ExecuteActivity(withSummary(release, artifactReleaseSummary), releaseArtifactsActivity,
		v1.ArtifactNamespace(st.GetIdentity().GetNamespace()), key).Get(release, nil); err != nil {
		workflow.GetLogger(ctx).Warn("releasing the run's artifact pins failed; `flow artifacts gc --release` clears them",
			"error", err.Error())
	}
}
