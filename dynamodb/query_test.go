package dynamodb

import (
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/require"
)

func TestBuildBuilderUsesInclusiveUntil(t *testing.T) {
	until := nostr.Timestamp(42)

	expr, err := buildBuilder(nostr.Filter{Until: &until}).Build()

	require.NoError(t, err)
	require.NotNil(t, expr.Filter())
	require.Contains(t, *expr.Filter(), "<=")
}
