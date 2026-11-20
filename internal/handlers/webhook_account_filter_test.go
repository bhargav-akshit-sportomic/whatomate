package handlers

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/shridarpatil/whatomate/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestWebhookWantsAccount(t *testing.T) {
	all := models.Webhook{}
	botOnly := models.Webhook{WhatsAppAccounts: models.StringArray{"bot"}}

	assert.True(t, webhookWantsAccount(all, MessageEventData{WhatsAppAccount: "campaigns"}), "no filter: every account")
	assert.True(t, webhookWantsAccount(botOnly, MessageEventData{WhatsAppAccount: "bot"}))
	assert.False(t, webhookWantsAccount(botOnly, MessageEventData{WhatsAppAccount: "campaigns"}))
	assert.False(t, webhookWantsAccount(botOnly, MessageStatusEventData{WhatsAppAccount: "campaigns"}))
	assert.False(t, webhookWantsAccount(botOnly, MessageFailedEventData{WhatsAppAccount: "campaigns"}))
	assert.False(t, webhookWantsAccount(botOnly, FlowResponseEventData{WhatsAppAccount: "campaigns"}))
	assert.False(t, webhookWantsAccount(botOnly, ContactEventData{WhatsAppAccount: "campaigns"}))
	assert.False(t, webhookWantsAccount(botOnly, TransferEventData{WhatsAppAccount: "campaigns"}))
	assert.False(t, webhookWantsAccount(botOnly, TemplateStatusEventData{WhatsAppAccount: "campaigns"}))
	assert.True(t, webhookWantsAccount(botOnly, MessageEventData{}), "an event without an account is not dropped")
	assert.True(t, webhookWantsAccount(botOnly, map[string]string{"test": "data"}), "unscoped payloads are not dropped")
}

func TestDispatchWebhook_AccountFilterSkipsOtherNumbers(t *testing.T) {
	app := webhookTestApp(t)
	org := testutil.CreateTestOrganization(t, app.DB)
	everything := captureWebhooks(t, app, org.ID, models.WebhookEventMessageDelivered)

	botOnly := captureWebhooks(t, app, org.ID, models.WebhookEventMessageDelivered)
	// captureWebhooks created the second webhook last; limit it to the bot number
	var hooks []models.Webhook
	require.NoError(t, app.DB.Where("organization_id = ?", org.ID).Order("created_at").Find(&hooks).Error)
	require.Len(t, hooks, 2)
	require.NoError(t, app.DB.Model(&hooks[1]).Update("whats_app_accounts", models.StringArray{"bot"}).Error)
	app.InvalidateWebhooksCache(org.ID)

	for i := 0; i < 5; i++ {
		app.DispatchWebhook(org.ID, models.WebhookEventMessageDelivered, MessageStatusEventData{WhatsAppAccount: "campaigns", MessageID: uuid.NewString()})
	}
	app.DispatchWebhook(org.ID, models.WebhookEventMessageDelivered, MessageStatusEventData{WhatsAppAccount: "bot", MessageID: "bot-msg"})
	app.WaitForBackgroundTasks()

	assert.Len(t, everything(), 6, "an unfiltered webhook gets every account's events")
	got := botOnly()
	require.Len(t, got, 1, "a campaign on another number does not reach the bot webhook")
	assert.Equal(t, "bot-msg", got[0].Data.(map[string]any)["message_id"])
}

func createWebhookReq(t *testing.T, app *App, orgID, userID uuid.UUID, body map[string]any) (int, WebhookResponse) {
	t.Helper()
	req := testutil.NewJSONRequest(t, body)
	testutil.SetAuthContext(req, orgID, userID)
	require.NoError(t, app.CreateWebhook(req))
	var resp struct {
		Data WebhookResponse `json:"data"`
	}
	status := testutil.GetResponseStatusCode(req)
	if status == fasthttp.StatusOK {
		require.NoError(t, json.Unmarshal(testutil.GetResponseBody(req), &resp))
	}
	return status, resp.Data
}

func TestCreateUpdateWebhook_WhatsAppAccounts(t *testing.T) {
	app := webhookTestApp(t)
	org := testutil.CreateTestOrganization(t, app.DB)
	role := testutil.CreateAdminRole(t, app.DB, org.ID)
	user := testutil.CreateTestUser(t, app.DB, org.ID, testutil.WithRoleID(&role.ID))
	account := testutil.CreateTestWhatsAppAccount(t, app.DB, org.ID)
	otherOrg := testutil.CreateTestOrganization(t, app.DB)
	foreign := testutil.CreateTestWhatsAppAccount(t, app.DB, otherOrg.ID)

	base := map[string]any{"name": "bot", "url": "https://hooks.example.com/wa", "events": []string{"message.incoming"}}
	with := func(accounts []string) map[string]any {
		b := map[string]any{}
		for k, v := range base {
			b[k] = v
		}
		b["whatsapp_accounts"] = accounts
		return b
	}

	status, _ := createWebhookReq(t, app, org.ID, user.ID, with([]string{"does-not-exist"}))
	assert.Equal(t, fasthttp.StatusBadRequest, status)
	status, _ = createWebhookReq(t, app, org.ID, user.ID, with([]string{foreign.Name}))
	assert.Equal(t, fasthttp.StatusBadRequest, status, "another organization's account is rejected")

	status, created := createWebhookReq(t, app, org.ID, user.ID, with([]string{account.Name, account.Name}))
	require.Equal(t, fasthttp.StatusOK, status)
	assert.Equal(t, []string{account.Name}, created.WhatsAppAccounts, "duplicates are dropped")

	status, unfiltered := createWebhookReq(t, app, org.ID, user.ID, base)
	require.Equal(t, fasthttp.StatusOK, status)
	assert.Equal(t, []string{}, unfiltered.WhatsAppAccounts, "omitted means every account")

	update := func(body map[string]any) WebhookResponse {
		req := testutil.NewJSONRequest(t, body)
		testutil.SetAuthContext(req, org.ID, user.ID)
		testutil.SetPathParam(req, "id", created.ID.String())
		require.NoError(t, app.UpdateWebhook(req))
		require.Equal(t, fasthttp.StatusOK, testutil.GetResponseStatusCode(req))
		var resp struct {
			Data WebhookResponse `json:"data"`
		}
		require.NoError(t, json.Unmarshal(testutil.GetResponseBody(req), &resp))
		return resp.Data
	}
	assert.Equal(t, []string{account.Name}, update(map[string]any{"is_active": true}).WhatsAppAccounts,
		"an update that omits whatsapp_accounts keeps the filter")
	assert.Equal(t, []string{}, update(map[string]any{"is_active": true, "whatsapp_accounts": []string{}}).WhatsAppAccounts,
		"an empty list clears it")
}

func TestProcessTemplateStatusUpdate_DispatchesEvent(t *testing.T) {
	app := webhookTestApp(t)
	org := testutil.CreateTestOrganization(t, app.DB)
	account := testutil.CreateTestWhatsAppAccount(t, app.DB, org.ID)
	captured := captureWebhooks(t, app, org.ID, models.WebhookEventTemplateStatusUpdated)

	require.NoError(t, app.DB.Create(&models.Template{
		BaseModel:       models.BaseModel{ID: uuid.New()},
		OrganizationID:  org.ID,
		WhatsAppAccount: account.Name,
		Name:            "booking_confirmation",
		DisplayName:     "Booking confirmation",
		Category:        "UTILITY",
		Language:        "en",
		Status:          string(models.TemplateStatusApproved),
		BodyContent:     "Hi",
	}).Error)

	app.processTemplateStatusUpdate(account.BusinessID, "PAUSED", "booking_confirmation", "en", "LOW_QUALITY")
	// A template made directly in Meta, not synced here, still fires the event
	app.processTemplateStatusUpdate(account.BusinessID, "REJECTED", "made_in_meta", "en", "INVALID_FORMAT")
	app.WaitForBackgroundTasks()

	payloads := captured()
	require.Len(t, payloads, 2)
	byName := map[string]map[string]any{}
	for _, p := range payloads {
		assert.Equal(t, "template.status_updated", p.Event)
		data := p.Data.(map[string]any)
		byName[data["template_name"].(string)] = data
	}

	paused := byName["booking_confirmation"]
	assert.Equal(t, "PAUSED", paused["status"])
	assert.Equal(t, "APPROVED", paused["previous_status"])
	assert.Equal(t, "LOW_QUALITY", paused["reason"])
	assert.Equal(t, account.Name, paused["whatsapp_account"])
	assert.Equal(t, account.BusinessID, paused["waba_id"])
	assert.Equal(t, "en", paused["language"])

	rejected := byName["made_in_meta"]
	assert.Equal(t, "REJECTED", rejected["status"])
	_, hasPrevious := rejected["previous_status"]
	assert.False(t, hasPrevious)

	var tpl models.Template
	require.NoError(t, app.DB.Where("whats_app_account = ? AND name = ?", account.Name, "booking_confirmation").First(&tpl).Error)
	assert.Equal(t, "PAUSED", tpl.Status, "the stored status is still updated")
}
