package worker

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/shridarpatil/whatomate/internal/config"
	"github.com/shridarpatil/whatomate/pkg/whatsapp"
	"github.com/shridarpatil/whatomate/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Campaign sends must go to the configured Meta base URL, so a mocked or
// staging base_url never lets a campaign reach the real Graph API.
func TestNew_WhatsAppClientUsesConfiguredBaseURL(t *testing.T) {
	rdb := testutil.SetupTestRedis(t)
	if rdb == nil {
		t.Skip("TEST_REDIS_URL not set, skipping test")
	}
	var hits atomic.Int32
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": []map[string]string{{"id": "wamid.mock"}}})
	}))
	t.Cleanup(mock.Close)

	cfg := &config.Config{WhatsApp: config.WhatsAppConfig{BaseURL: mock.URL}}
	w, err := New(cfg, testutil.SetupTestDB(t), rdb, testutil.NopLogger())
	require.NoError(t, err)

	account := &whatsapp.Account{PhoneID: "123", BusinessID: "456", APIVersion: "v24.0", AccessToken: "t"}
	id, err := w.WhatsApp.SendTextMessage(testutil.TestContext(t), account, whatsapp.Recipient{Phone: "15550001111"}, "hi", "")
	require.NoError(t, err)
	assert.Equal(t, "wamid.mock", id)
	assert.EqualValues(t, 1, hits.Load(), "the send went to the configured base URL")
}
