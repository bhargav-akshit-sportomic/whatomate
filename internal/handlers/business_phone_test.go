package handlers

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/shridarpatil/whatomate/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestNormalizeBusinessPhone(t *testing.T) {
	assert.Equal(t, "918356850985", normalizeBusinessPhone("+91 83568 50985"))
	assert.Equal(t, "918356850985", normalizeBusinessPhone("91-8356-850985"))
	assert.Equal(t, "15550001111", normalizeBusinessPhone("+1 (555) 000-1111"))
	assert.Equal(t, "", normalizeBusinessPhone(""))
	assert.Equal(t, "", normalizeBusinessPhone("n/a"))
}

func metaTextWebhook(phoneID, displayPhone, wamid string) []byte {
	body, _ := json.Marshal(map[string]any{
		"object": "whatsapp_business_account",
		"entry": []map[string]any{{
			"id": "waba-1",
			"changes": []map[string]any{{
				"field": "messages",
				"value": map[string]any{
					"messaging_product": "whatsapp",
					"metadata":          map[string]any{"display_phone_number": displayPhone, "phone_number_id": phoneID},
					"contacts":          []map[string]any{{"profile": map[string]any{"name": "Player"}, "wa_id": "919876543210"}},
					"messages": []map[string]any{{
						"from": "919876543210", "id": wamid, "timestamp": "1795172232", "type": "text",
						"text": map[string]any{"body": "book"},
					}},
				},
			}},
		}},
	})
	return body
}

func TestWebhookHandler_LearnsBusinessPhoneAndSendsItOnEvents(t *testing.T) {
	app := webhookTestApp(t)
	app.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	org := testutil.CreateTestOrganization(t, app.DB)
	account := testutil.CreateTestWhatsAppAccount(t, app.DB, org.ID)
	require.Empty(t, account.PhoneNumber)
	captured := captureWebhooks(t, app, org.ID, models.WebhookEventMessageIncoming)

	req := testutil.NewRequest(t)
	req.RequestCtx.Request.Header.SetMethod("POST")
	req.RequestCtx.Request.Header.SetContentType("application/json")
	req.RequestCtx.Request.SetBody(metaTextWebhook(account.PhoneID, "+91 83568 50985", "wamid.bp_"+uuid.NewString()[:8]))
	require.NoError(t, app.WebhookHandler(req))
	require.Equal(t, fasthttp.StatusOK, testutil.GetResponseStatusCode(req))
	app.WaitForBackgroundTasks()

	var stored models.WhatsAppAccount
	require.NoError(t, app.DB.First(&stored, "id = ?", account.ID).Error)
	assert.Equal(t, "918356850985", stored.PhoneNumber, "the number is learned from Meta's metadata")

	// Meta messages are processed in a goroutine the handler does not wait for
	var payloads []OutboundWebhookPayload
	require.Eventually(t, func() bool {
		app.WaitForBackgroundTasks()
		payloads = captured()
		return len(payloads) == 1
	}, 5*time.Second, 50*time.Millisecond)
	data := payloads[0].Data.(map[string]any)
	assert.Equal(t, "918356850985", data["business_phone"], "the first inbound event already carries it")
	assert.Equal(t, account.Name, data["whatsapp_account"])
}

func TestDispatchWebhook_AddsBusinessPhoneFromAccount(t *testing.T) {
	app := webhookTestApp(t)
	org := testutil.CreateTestOrganization(t, app.DB)
	account := testutil.CreateTestWhatsAppAccount(t, app.DB, org.ID)
	require.NoError(t, app.DB.Model(account).Update("phone_number", "917000000001").Error)
	captured := captureWebhooks(t, app, org.ID,
		models.WebhookEventMessageDelivered, models.WebhookEventMessageFailed, models.WebhookEventTemplateStatusUpdated)

	app.DispatchWebhook(org.ID, models.WebhookEventMessageDelivered, MessageStatusEventData{WhatsAppAccount: account.Name, MessageID: "m1"})
	app.DispatchWebhook(org.ID, models.WebhookEventMessageFailed, MessageFailedEventData{WhatsAppAccount: account.Name, MessageID: "m2"})
	app.DispatchWebhook(org.ID, models.WebhookEventTemplateStatusUpdated, TemplateStatusEventData{WhatsAppAccount: account.Name, TemplateName: "t"})
	// An event that already names its number keeps it; an unknown account gets none
	app.DispatchWebhook(org.ID, models.WebhookEventMessageDelivered, MessageStatusEventData{WhatsAppAccount: account.Name, MessageID: "m3", BusinessPhone: "919999999999"})
	app.DispatchWebhook(org.ID, models.WebhookEventMessageDelivered, MessageStatusEventData{WhatsAppAccount: "no-such-account", MessageID: "m4"})
	app.WaitForBackgroundTasks()

	byMessage := map[string]map[string]any{}
	for _, p := range captured() {
		data := p.Data.(map[string]any)
		key, _ := data["message_id"].(string)
		if key == "" {
			key = p.Event
		}
		byMessage[key] = data
	}
	require.Len(t, byMessage, 5)
	assert.Equal(t, "917000000001", byMessage["m1"]["business_phone"])
	assert.Equal(t, "917000000001", byMessage["m2"]["business_phone"])
	assert.Equal(t, "917000000001", byMessage[string(models.WebhookEventTemplateStatusUpdated)]["business_phone"])
	assert.Equal(t, "919999999999", byMessage["m3"]["business_phone"])
	_, has := byMessage["m4"]["business_phone"]
	assert.False(t, has)
}

func TestRememberBusinessPhone_UpdatesOnChangeAndRefreshesCache(t *testing.T) {
	app := webhookTestApp(t)
	org := testutil.CreateTestOrganization(t, app.DB)
	account := testutil.CreateTestWhatsAppAccount(t, app.DB, org.ID)

	assert.Equal(t, "", app.businessPhoneFor(org.ID, account.Name))
	app.rememberBusinessPhone(account, "+91 70000 00002")
	assert.Equal(t, "917000000002", app.businessPhoneFor(org.ID, account.Name), "the cached empty value is dropped")

	app.rememberBusinessPhone(account, "") // nothing to learn
	assert.Equal(t, "917000000002", app.businessPhoneFor(org.ID, account.Name))
}

func TestCreateUpdateAccount_PhoneNumber(t *testing.T) {
	app := webhookTestApp(t)
	org := testutil.CreateTestOrganization(t, app.DB)
	role := testutil.CreateAdminRole(t, app.DB, org.ID)
	user := testutil.CreateTestUser(t, app.DB, org.ID, testutil.WithRoleID(&role.ID))

	name := "bot-" + uuid.NewString()[:8]
	req := testutil.NewJSONRequest(t, map[string]any{
		"name": name, "phone_id": "phone-" + uuid.NewString()[:8], "business_id": "biz-1",
		"access_token": "tok", "phone_number": "+91 83568-50985",
	})
	testutil.SetAuthContext(req, org.ID, user.ID)
	require.NoError(t, app.CreateAccount(req))
	require.Equal(t, fasthttp.StatusOK, testutil.GetResponseStatusCode(req))
	var created struct {
		Data AccountResponse `json:"data"`
	}
	require.NoError(t, json.Unmarshal(testutil.GetResponseBody(req), &created))
	assert.Equal(t, "918356850985", created.Data.PhoneNumber)

	update := func(body map[string]any) AccountResponse {
		req := testutil.NewJSONRequest(t, body)
		testutil.SetAuthContext(req, org.ID, user.ID)
		testutil.SetPathParam(req, "id", created.Data.ID.String())
		require.NoError(t, app.UpdateAccount(req))
		require.Equal(t, fasthttp.StatusOK, testutil.GetResponseStatusCode(req))
		var resp struct {
			Data AccountResponse `json:"data"`
		}
		require.NoError(t, json.Unmarshal(testutil.GetResponseBody(req), &resp))
		return resp.Data
	}
	assert.Equal(t, "918356850985", update(map[string]any{"name": name}).PhoneNumber, "omitted keeps the number")
	assert.Equal(t, "917000000003", update(map[string]any{"name": name, "phone_number": "917000000003"}).PhoneNumber)
	assert.Equal(t, "917000000003", app.businessPhoneFor(org.ID, name))
}
