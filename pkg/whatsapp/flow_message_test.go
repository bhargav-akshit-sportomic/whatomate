package whatsapp_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shridarpatil/whatomate/pkg/whatsapp"
	"github.com/shridarpatil/whatomate/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sendFlowCapture sends m and returns the interactive object Meta received.
func sendFlowCapture(t *testing.T, m whatsapp.FlowMessage) (map[string]any, error) {
	t.Helper()
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&captured)
		_ = json.NewEncoder(w).Encode(map[string]any{"messages": []map[string]string{{"id": "wamid.flow1"}}})
	}))
	t.Cleanup(server.Close)

	client := whatsapp.NewWithTimeout(testutil.NopLogger(), 5*time.Second)
	client.HTTPClient = &http.Client{Transport: &testServerTransport{serverURL: server.URL}}
	account := &whatsapp.Account{PhoneID: "123", BusinessID: "456", APIVersion: "v21.0", AccessToken: "t"}

	id, err := client.SendFlow(testutil.TestContext(t), account, whatsapp.Recipient{Phone: "919876543210"}, m)
	if err != nil {
		return nil, err
	}
	assert.Equal(t, "wamid.flow1", id)
	interactive, ok := captured["interactive"].(map[string]any)
	require.True(t, ok)
	return interactive, nil
}

func flowParams(t *testing.T, interactive map[string]any) map[string]any {
	t.Helper()
	action, ok := interactive["action"].(map[string]any)
	require.True(t, ok)
	params, ok := action["parameters"].(map[string]any)
	require.True(t, ok)
	return params
}

func TestClient_SendFlow_DataExchangeKeepsCallerTokenAndOmitsPayload(t *testing.T) {
	t.Parallel()
	interactive, err := sendFlowCapture(t, whatsapp.FlowMessage{
		FlowID:     "1234",
		BodyText:   "Book a court",
		CTAText:    "Book now",
		FlowToken:  "venue-1|919876543210|UGxheWVy",
		FlowAction: whatsapp.FlowActionDataExchange,
		// Ignored for data_exchange: Meta rejects flow_action_payload there
		FirstScreen:    "SLOTS",
		FlowActionData: map[string]any{"x": 1},
	})
	require.NoError(t, err)

	params := flowParams(t, interactive)
	assert.Equal(t, "data_exchange", params["flow_action"])
	assert.Equal(t, "venue-1|919876543210|UGxheWVy", params["flow_token"])
	assert.Equal(t, "1234", params["flow_id"])
	assert.Equal(t, "Book now", params["flow_cta"])
	_, hasPayload := params["flow_action_payload"]
	assert.False(t, hasPayload)
}

func TestClient_SendFlow_NavigateWithScreenData(t *testing.T) {
	t.Parallel()
	interactive, err := sendFlowCapture(t, whatsapp.FlowMessage{
		FlowID:         "1234",
		BodyText:       "Enquire",
		FirstScreen:    "ENQUIRY",
		FlowActionData: map[string]any{"venue_name": "Gundecha"},
	})
	require.NoError(t, err)

	params := flowParams(t, interactive)
	assert.Equal(t, "navigate", params["flow_action"])
	payload, ok := params["flow_action_payload"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ENQUIRY", payload["screen"])
	assert.Equal(t, map[string]any{"venue_name": "Gundecha"}, payload["data"])
	assert.NotEmpty(t, params["flow_token"], "a token is generated when none is given")
}

func TestClient_SendFlow_ImageHeaderWinsOverText(t *testing.T) {
	t.Parallel()
	interactive, err := sendFlowCapture(t, whatsapp.FlowMessage{
		FlowID:         "1234",
		BodyText:       "Enquire",
		HeaderText:     "ignored",
		HeaderImageURL: "https://cdn.example.com/enquiry.jpg",
	})
	require.NoError(t, err)

	header, ok := interactive["header"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "image", header["type"])
	assert.Equal(t, map[string]any{"link": "https://cdn.example.com/enquiry.jpg"}, header["image"])
}

func TestClient_SendFlow_RejectsUnknownAction(t *testing.T) {
	t.Parallel()
	_, err := sendFlowCapture(t, whatsapp.FlowMessage{FlowID: "1234", BodyText: "x", FlowAction: "complete"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flow_action")
}

func TestClient_SendFlowMessage_DefaultsUnchanged(t *testing.T) {
	t.Parallel()
	// The legacy helper keeps navigate + FIRST_SCREEN + text header.
	interactive, err := sendFlowCapture(t, whatsapp.FlowMessage{FlowID: "1234", BodyText: "Hi", HeaderText: "Title"})
	require.NoError(t, err)

	params := flowParams(t, interactive)
	assert.Equal(t, "navigate", params["flow_action"])
	assert.Equal(t, map[string]any{"screen": "FIRST_SCREEN"}, params["flow_action_payload"])
	assert.Equal(t, map[string]any{"type": "text", "text": "Title"}, interactive["header"])
}
