package handlers_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/handlers"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/shridarpatil/whatomate/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type sendToPhoneEnv struct {
	app     *handlers.App
	mock    *mockWhatsAppServer
	org     *models.Organization
	user    *models.User
	account *models.WhatsAppAccount
}

func newSendToPhoneEnv(t *testing.T) sendToPhoneEnv {
	t.Helper()
	mock := newMockWhatsAppServer()
	t.Cleanup(mock.close)
	app := newMsgTestApp(t, mock)
	org := testutil.CreateTestOrganization(t, app.DB)
	adminRole := testutil.CreateAdminRole(t, app.DB, org.ID)
	user := testutil.CreateTestUser(t, app.DB, org.ID, testutil.WithRoleID(&adminRole.ID))
	return sendToPhoneEnv{app: app, mock: mock, org: org, user: user, account: createTestAccount(t, app, org.ID)}
}

func (e sendToPhoneEnv) send(t *testing.T, body map[string]any) (int, []byte) {
	t.Helper()
	req := testutil.NewJSONRequest(t, body)
	testutil.SetAuthContext(req, e.org.ID, e.user.ID)
	require.NoError(t, e.app.SendMessageToPhone(req))
	return testutil.GetResponseStatusCode(req), testutil.GetResponseBody(req)
}

func lastFlowParams(t *testing.T, mock *mockWhatsAppServer) map[string]any {
	t.Helper()
	require.NotEmpty(t, mock.sentMessages)
	sent := mock.sentMessages[len(mock.sentMessages)-1]
	interactive := sent["interactive"].(map[string]any)
	return interactive["action"].(map[string]any)["parameters"].(map[string]any)
}

func TestApp_SendMessageToPhone_DataExchangeFlowToNewContact(t *testing.T) {
	t.Parallel()
	env := newSendToPhoneEnv(t)

	// The flow was built directly in Meta, so it is not in the flows table.
	status, body := env.send(t, map[string]any{
		"phone_number":     "+91 98765-43210",
		"type":             "interactive",
		"whatsapp_account": env.account.Name,
		"interactive": map[string]any{
			"type":             "flow",
			"flow_id":          "998877",
			"body":             "Book a court",
			"button_text":      "Book now",
			"flow_token":       "venue-1|919876543210|UGxheWVy",
			"flow_action":      "data_exchange",
			"header_image_url": "https://cdn.example.com/court.jpg",
		},
	})
	require.Equal(t, fasthttp.StatusOK, status, string(body))

	var resp struct {
		Data handlers.MessageResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, models.MessageStatusSent, resp.Data.Status)
	assert.Equal(t, env.mock.nextMessageID, resp.Data.WAMID, "the wamid comes back in the same response")
	assert.Equal(t, env.account.Name, resp.Data.WhatsAppAccount)

	var contact models.Contact
	require.NoError(t, env.app.DB.Where("organization_id = ? AND phone_number = ?", env.org.ID, "919876543210").First(&contact).Error)
	assert.Equal(t, contact.ID, resp.Data.ContactID)

	params := lastFlowParams(t, env.mock)
	assert.Equal(t, "data_exchange", params["flow_action"])
	assert.Equal(t, "venue-1|919876543210|UGxheWVy", params["flow_token"])
	assert.Equal(t, "998877", params["flow_id"])
	_, hasPayload := params["flow_action_payload"]
	assert.False(t, hasPayload)
	assert.Equal(t, "919876543210", env.mock.sentMessages[len(env.mock.sentMessages)-1]["to"])
}

func TestApp_SendMessageToPhone_TextReusesExistingContact(t *testing.T) {
	t.Parallel()
	env := newSendToPhoneEnv(t)
	existing := testutil.CreateTestContactWith(t, env.app.DB, env.org.ID,
		testutil.WithPhoneNumber("919812345678"), testutil.WithContactAccount(env.account.Name))

	status, body := env.send(t, map[string]any{
		"phone_number": "919812345678",
		"type":         "text",
		"content":      map[string]any{"body": "Your booking is confirmed"},
	})
	require.Equal(t, fasthttp.StatusOK, status, string(body))

	var resp struct {
		Data handlers.MessageResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	assert.Equal(t, existing.ID, resp.Data.ContactID)

	var count int64
	env.app.DB.Model(&models.Contact{}).Where("organization_id = ? AND phone_number = ?", env.org.ID, "919812345678").Count(&count)
	assert.Equal(t, int64(1), count, "no duplicate contact")
}

func TestApp_SendMessageToPhone_CTAURL(t *testing.T) {
	t.Parallel()
	env := newSendToPhoneEnv(t)

	status, body := env.send(t, map[string]any{
		"phone_number":     "919800000001",
		"type":             "interactive",
		"whatsapp_account": env.account.Name,
		"interactive": map[string]any{
			"type": "cta_url", "body": "Pay to confirm", "button_text": "Pay now", "url": "https://pay.example.com/x",
		},
	})
	require.Equal(t, fasthttp.StatusOK, status, string(body))
	sent := env.mock.sentMessages[len(env.mock.sentMessages)-1]
	assert.Equal(t, "cta_url", sent["interactive"].(map[string]any)["type"])
}

func TestApp_SendMessageToPhone_MetaRejectionIs502(t *testing.T) {
	t.Parallel()
	env := newSendToPhoneEnv(t)
	env.mock.returnError = true
	env.mock.errorMessage = "Re-engagement message"

	status, body := env.send(t, map[string]any{
		"phone_number":     "919800000002",
		"type":             "text",
		"whatsapp_account": env.account.Name,
		"content":          map[string]any{"body": "hi"},
	})
	assert.Equal(t, fasthttp.StatusBadGateway, status)
	assert.Contains(t, string(body), "Re-engagement message")
	assert.Contains(t, string(body), `"status":"failed"`)
}

func TestApp_SendMessageToPhone_Validation(t *testing.T) {
	t.Parallel()
	env := newSendToPhoneEnv(t)

	cases := map[string]map[string]any{
		"letters in phone": {"phone_number": "91abc", "type": "text", "content": map[string]any{"body": "x"}},
		"phone too short":  {"phone_number": "12345", "type": "text", "content": map[string]any{"body": "x"}},
		"template type":    {"phone_number": "919800000003", "type": "template"},
		"empty text":       {"phone_number": "919800000003", "type": "text"},
		"voice_call":       {"phone_number": "919800000003", "type": "interactive", "interactive": map[string]any{"type": "voice_call", "body": "x"}},
		"bad flow action": {"phone_number": "919800000003", "type": "interactive", "whatsapp_account": env.account.Name,
			"interactive": map[string]any{"type": "flow", "flow_id": "1", "body": "x", "flow_action": "complete"}},
		"flow without id": {"phone_number": "919800000003", "type": "interactive", "whatsapp_account": env.account.Name,
			"interactive": map[string]any{"type": "flow", "body": "x"}},
	}
	for name, body := range cases {
		status, resp := env.send(t, body)
		assert.Equal(t, fasthttp.StatusBadRequest, status, "%s: %s", name, resp)
	}
	assert.Empty(t, env.mock.sentMessages, "nothing is sent for an invalid request")
}

func TestApp_SendMessage_ContactFlowStillNeedsSavedFlowButKeepsCallerToken(t *testing.T) {
	t.Parallel()
	env := newSendToPhoneEnv(t)
	contact := testutil.CreateTestContactWith(t, env.app.DB, env.org.ID, testutil.WithContactAccount(env.account.Name))

	flowBody := map[string]any{
		"type": "interactive",
		"interactive": map[string]any{
			"type": "flow", "flow_id": "554433", "body": "Book", "flow_token": "caller-token",
		},
	}
	send := func() int {
		req := testutil.NewJSONRequest(t, flowBody)
		testutil.SetAuthContext(req, env.org.ID, env.user.ID)
		testutil.SetPathParam(req, "id", contact.ID.String())
		require.NoError(t, env.app.SendMessage(req))
		env.app.WaitForBackgroundTasks()
		return testutil.GetResponseStatusCode(req)
	}

	// The agent endpoint keeps its rule: the flow must be one of the org's saved flows
	assert.Equal(t, fasthttp.StatusBadRequest, send())

	require.NoError(t, env.app.DB.Create(&models.WhatsAppFlow{
		BaseModel:       models.BaseModel{ID: uuid.New()},
		OrganizationID:  env.org.ID,
		WhatsAppAccount: env.account.Name,
		Name:            "booking",
		MetaFlowID:      "554433",
	}).Error)
	assert.Equal(t, fasthttp.StatusOK, send())
	assert.Equal(t, "caller-token", lastFlowParams(t, env.mock)["flow_token"])
}
