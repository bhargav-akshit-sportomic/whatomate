package handlers

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/config"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metaInboundMessage decodes a messages[] entry exactly as Meta sends it.
func metaInboundMessage(t *testing.T, raw string) IncomingTextMessage {
	t.Helper()
	var msg IncomingTextMessage
	require.NoError(t, json.Unmarshal([]byte(raw), &msg))
	return msg
}

// newInboundEventTest returns an app with an account and a capture of the given events.
func newInboundEventTest(t *testing.T, events ...models.WebhookEvent) (*App, *models.WhatsAppAccount, func() []OutboundWebhookPayload) {
	t.Helper()
	app := newProcessorTestApp(t)
	if app.Redis == nil {
		t.Skip("TEST_REDIS_URL not set, skipping webhook dispatch test")
	}
	// processIncomingMessageFull looks the account up by phone id, which decrypts its secrets
	app.Config = &config.Config{App: config.AppConfig{EncryptionKey: "test-encryption-key-32-bytes-long"}}
	org, account := createProcessorTestOrg(t, app)
	return app, account, captureWebhooks(t, app, org.ID, events...)
}

func payloadsFor(payloads []OutboundWebhookPayload, event models.WebhookEvent) []map[string]any {
	var out []map[string]any
	for _, p := range payloads {
		if p.Event == string(event) {
			data, _ := p.Data.(map[string]any)
			out = append(out, data)
		}
	}
	return out
}

func TestIncomingFlowReply_ForwardsResponseJSON(t *testing.T) {
	app, account, captured := newInboundEventTest(t, models.WebhookEventMessageIncoming, models.WebhookEventMessageFlowResponse)

	wamid := "wamid.in_" + uuid.New().String()[:8]
	flowMsgWAMID := "wamid.flow_" + uuid.New().String()[:8]
	responseJSON := `{"action":"confirm_booking","selected_slots":["arena-1|slot-1|750|Court 1|5:00 PM - 5:30 PM"],"flow_token":"venue-1|919876543210|UGxheWVy"}`
	msg := metaInboundMessage(t, `{
		"from": "919876543210", "id": "`+wamid+`", "timestamp": "1795172232", "type": "interactive",
		"context": {"from": "15550001111", "id": "`+flowMsgWAMID+`"},
		"interactive": {"type": "nfm_reply", "nfm_reply": {"name": "flow", "body": "Sent", "response_json": `+mustJSONString(t, responseJSON)+`}}
	}`)

	app.processIncomingMessageFull(account.PhoneID, msg, "Player")
	app.WaitForBackgroundTasks()
	payloads := captured()

	incoming := payloadsFor(payloads, models.WebhookEventMessageIncoming)
	require.Len(t, incoming, 1)
	assert.Equal(t, "nfm_reply", incoming[0]["message_type"])
	assert.Equal(t, wamid, incoming[0]["whatsapp_message_id"])
	interactive, ok := incoming[0]["interactive"].(map[string]any)
	require.True(t, ok, "message.incoming must carry the raw interactive object")
	nfm, ok := interactive["nfm_reply"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, responseJSON, nfm["response_json"], "response_json is forwarded unchanged")

	flow := payloadsFor(payloads, models.WebhookEventMessageFlowResponse)
	require.Len(t, flow, 1, "a Flow reply fires message.flow_response once")
	assert.Equal(t, wamid, flow[0]["whatsapp_message_id"])
	assert.Equal(t, "919876543210", flow[0]["contact_phone"])
	assert.Equal(t, account.Name, flow[0]["whatsapp_account"])
	assert.Equal(t, "flow", flow[0]["flow_name"])
	assert.Equal(t, responseJSON, flow[0]["response_json"])
	assert.Equal(t, "venue-1|919876543210|UGxheWVy", flow[0]["flow_token"])
	assert.Equal(t, flowMsgWAMID, flow[0]["reply_to_whatsapp_message_id"])
	response, ok := flow[0]["response"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "confirm_booking", response["action"])
}

func TestIncomingFlowReply_InvalidResponseJSONStillForwardsRawString(t *testing.T) {
	app, account, captured := newInboundEventTest(t, models.WebhookEventMessageFlowResponse)

	msg := metaInboundMessage(t, `{
		"from": "919876543210", "id": "wamid.bad_`+uuid.New().String()[:8]+`", "timestamp": "1795172232", "type": "interactive",
		"interactive": {"type": "nfm_reply", "nfm_reply": {"name": "flow", "body": "Sent", "response_json": "not json"}}
	}`)

	app.processIncomingMessageFull(account.PhoneID, msg, "Player")
	app.WaitForBackgroundTasks()

	flow := payloadsFor(captured(), models.WebhookEventMessageFlowResponse)
	require.Len(t, flow, 1)
	assert.Equal(t, "not json", flow[0]["response_json"])
	_, hasResponse := flow[0]["response"]
	assert.False(t, hasResponse, "response is omitted when response_json is not JSON")
	_, hasToken := flow[0]["flow_token"]
	assert.False(t, hasToken)
}

func TestIncomingButtonReply_ForwardsButtonIDWithoutFlowEvent(t *testing.T) {
	app, account, captured := newInboundEventTest(t, models.WebhookEventMessageIncoming, models.WebhookEventMessageFlowResponse)

	msg := metaInboundMessage(t, `{
		"from": "919876543210", "id": "wamid.btn_`+uuid.New().String()[:8]+`", "timestamp": "1795172232", "type": "interactive",
		"interactive": {"type": "button_reply", "button_reply": {"id": "rating_5", "title": "Excellent"}}
	}`)

	app.processIncomingMessageFull(account.PhoneID, msg, "Player")
	app.WaitForBackgroundTasks()
	payloads := captured()

	incoming := payloadsFor(payloads, models.WebhookEventMessageIncoming)
	require.Len(t, incoming, 1)
	assert.Equal(t, "Excellent", incoming[0]["content"])
	assert.Equal(t, "rating_5", incoming[0]["button_id"])
	interactive, ok := incoming[0]["interactive"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "button_reply", interactive["type"])
	assert.Empty(t, payloadsFor(payloads, models.WebhookEventMessageFlowResponse))
}

func TestIncomingListReply_ForwardsRowID(t *testing.T) {
	app, account, captured := newInboundEventTest(t, models.WebhookEventMessageIncoming)

	msg := metaInboundMessage(t, `{
		"from": "919876543210", "id": "wamid.list_`+uuid.New().String()[:8]+`", "timestamp": "1795172232", "type": "interactive",
		"interactive": {"type": "list_reply", "list_reply": {"id": "venue_42", "title": "Andheri", "description": "Mumbai"}}
	}`)

	app.processIncomingMessageFull(account.PhoneID, msg, "Player")
	app.WaitForBackgroundTasks()

	incoming := payloadsFor(captured(), models.WebhookEventMessageIncoming)
	require.Len(t, incoming, 1)
	assert.Equal(t, "venue_42", incoming[0]["button_id"])
}

func TestIncomingTemplateQuickReply_ForwardsPayloadAsButtonID(t *testing.T) {
	app, account, captured := newInboundEventTest(t, models.WebhookEventMessageIncoming)

	msg := metaInboundMessage(t, `{
		"from": "919876543210", "id": "wamid.qr_`+uuid.New().String()[:8]+`", "timestamp": "1795172232", "type": "button",
		"button": {"text": "Yes", "payload": "nudge_yes"}
	}`)

	app.processIncomingMessageFull(account.PhoneID, msg, "Player")
	app.WaitForBackgroundTasks()

	incoming := payloadsFor(captured(), models.WebhookEventMessageIncoming)
	require.Len(t, incoming, 1)
	assert.Equal(t, "button_reply", incoming[0]["message_type"])
	assert.Equal(t, "nudge_yes", incoming[0]["button_id"])
	_, hasInteractive := incoming[0]["interactive"]
	assert.False(t, hasInteractive, "a template quick-reply has no interactive object")
}

func TestIncomingText_OmitsButtonAndInteractive(t *testing.T) {
	app, account, captured := newInboundEventTest(t, models.WebhookEventMessageIncoming)

	msg := metaInboundMessage(t, `{
		"from": "919876543210", "id": "wamid.txt_`+uuid.New().String()[:8]+`", "timestamp": "1795172232", "type": "text",
		"text": {"body": "book"}
	}`)

	app.processIncomingMessageFull(account.PhoneID, msg, "Player")
	app.WaitForBackgroundTasks()

	incoming := payloadsFor(captured(), models.WebhookEventMessageIncoming)
	require.Len(t, incoming, 1)
	assert.Equal(t, "book", incoming[0]["content"])
	_, hasButton := incoming[0]["button_id"]
	_, hasInteractive := incoming[0]["interactive"]
	assert.False(t, hasButton)
	assert.False(t, hasInteractive)
}

func mustJSONString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}
