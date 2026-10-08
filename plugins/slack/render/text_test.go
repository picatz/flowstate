package render_test

import (
	"strings"
	"testing"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"

	"github.com/picatz/flowstate/plugins/slack/render"
)

// TestMarkupBoundsExpansionWhileBuilding proves a small template repeating a
// large argument fails before it allocates the product.
func TestMarkupBoundsExpansionWhileBuilding(t *testing.T) {
	t.Parallel()

	text := &chatv1.Text{Kind: &chatv1.Text_Markup{Markup: &chatv1.Markup{
		Template: strings.Repeat("{a}", 900),
		Args:     map[string]string{"a": strings.Repeat("&", 3000)}, // escapes to 5 bytes each
	}}}
	_, err := render.Mrkdwn("text", text)
	if err == nil || !strings.Contains(err.Error(), "expands past") {
		t.Fatalf("want an expansion-bound error, got %v", err)
	}
}
