package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Only a trigger verified under a scheme whose sender needs the bodyless 200 is
// answered with it; an unknown scheme name, a jwt trigger and the other
// providers are not.
func TestWebhookAnswersEmptyOnlyForSlack(t *testing.T) {
	t.Parallel()

	verify := func(names ...string) *flowstatev1.WebhookTrigger {
		trigger := &flowstatev1.WebhookTrigger{Verify: map[string]*flowstatev1.Value{}}
		for _, name := range names {
			trigger.Verify[name] = &flowstatev1.Value{}
		}

		return trigger
	}

	assert.True(t, flowstatev1.WebhookAnswersEmpty(verify(flowstatev1.WebhookSchemeSlack)))
	assert.False(t, flowstatev1.WebhookAnswersEmpty(verify(flowstatev1.WebhookSchemeGitHub)))
	assert.False(t, flowstatev1.WebhookAnswersEmpty(verify(flowstatev1.WebhookSchemeStripe)))
	assert.False(t, flowstatev1.WebhookAnswersEmpty(verify(flowstatev1.WebhookSchemeJWT)))
	assert.False(t, flowstatev1.WebhookAnswersEmpty(verify("not-a-scheme")))
	assert.False(t, flowstatev1.WebhookAnswersEmpty(&flowstatev1.WebhookTrigger{}))
	assert.False(t, flowstatev1.WebhookAnswersEmpty(nil))
}
