package handlers

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRetryFixture is a webhook logs fixture whose queued deliveries are
// removed from the shared Redis retry queue when the test ends.
func newRetryFixture(t *testing.T) *webhookLogsFixture {
	t.Helper()
	f := newWebhookLogsFixture(t)
	t.Cleanup(func() {
		var ids []string
		f.app.DB.Model(&models.WebhookDelivery{}).Where("webhook_id = ?", f.webhook.ID).Pluck("id::text", &ids)
		for _, id := range ids {
			f.app.Redis.ZRem(context.Background(), webhookRetryQueueKey, id)
		}
	})
	return f
}

func (f *webhookLogsFixture) reload(t *testing.T, id uuid.UUID) models.WebhookDelivery {
	t.Helper()
	var d models.WebhookDelivery
	require.NoError(t, f.app.DB.First(&d, "id = ?", id).Error)
	return d
}

// makeDue moves a queued delivery's due time into the past.
func (f *webhookLogsFixture) makeDue(t *testing.T, id uuid.UUID) {
	t.Helper()
	require.NoError(t, f.app.queueWebhookRetry(context.Background(), id, time.Now().Add(-time.Second)))
}

func respondStatus(code int) func(int32, http.ResponseWriter) {
	return func(_ int32, w http.ResponseWriter) { w.WriteHeader(code) }
}

func TestSendWebhook_FailureQueuesDurableRetry(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusServiceUnavailable))

	before := time.Now()
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)

	d := f.onlyDelivery(t)
	assert.Equal(t, models.WebhookDeliveryProcessing, d.Status, "a failed first attempt is retried, not given up")
	assert.Equal(t, 1, d.AttemptCount)
	assert.Nil(t, d.CompletedAt)
	require.NotNil(t, d.NextAttemptAt)
	assert.WithinDuration(t, before.Add(webhookRetryDelays[0]), *d.NextAttemptAt, 2*time.Second)

	due, queued := f.retryDue(t, d.ID)
	require.True(t, queued, "the retry lives in Redis, so it survives a restart")
	assert.WithinDuration(t, *d.NextAttemptAt, due, time.Second)
}

func TestRetryWebhookDelivery_SuccessKeepsBodyAndDeliveryID(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(func(call int32, w http.ResponseWriter) {
		if call == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		}
	})
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)
	id := f.onlyDelivery(t).ID

	f.makeDue(t, id)
	f.app.retryWebhookDelivery(context.Background(), id.String())

	d := f.reload(t, id)
	assert.Equal(t, models.WebhookDeliverySuccess, d.Status)
	assert.Equal(t, 2, d.AttemptCount)
	assert.Empty(t, d.FailureReason, "a later success clears the earlier failure")
	assert.NotNil(t, d.CompletedAt)
	_, queued := f.retryDue(t, id)
	assert.False(t, queued)

	bodies := f.receivedBodies()
	require.Len(t, bodies, 2)
	assert.Equal(t, bodies[0], bodies[1], "a retry resends the exact same body")
	assert.Equal(t, []string{id.String(), id.String()}, f.receivedDeliveryIDs(),
		"every attempt carries the same delivery id so receivers can drop duplicates")
}

func TestRetryWebhookDelivery_GivesUpAfterMaxAttempts(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusBadGateway))
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)
	id := f.onlyDelivery(t).ID

	for attempt := 2; attempt <= webhookMaxAttempts; attempt++ {
		f.makeDue(t, id)
		f.app.retryWebhookDelivery(context.Background(), id.String())
		if attempt < webhookMaxAttempts {
			d := f.reload(t, id)
			require.Equal(t, models.WebhookDeliveryProcessing, d.Status, "attempt %d", attempt)
			require.NotNil(t, d.NextAttemptAt)
			assert.WithinDuration(t, time.Now().Add(webhookRetryDelays[attempt-1]), *d.NextAttemptAt, 2*time.Second,
				"backoff after attempt %d", attempt)
		}
	}

	d := f.reload(t, id)
	assert.Equal(t, models.WebhookDeliveryFailed, d.Status)
	assert.Equal(t, webhookMaxAttempts, d.AttemptCount)
	assert.Len(t, d.Attempts, webhookMaxAttempts)
	assert.Equal(t, "HTTP 502 Bad Gateway", d.FailureReason)
	assert.NotNil(t, d.CompletedAt)
	_, queued := f.retryDue(t, id)
	assert.False(t, queued, "an exhausted delivery leaves the queue")
	assert.Len(t, f.receivedBodies(), webhookMaxAttempts)
}

func TestWebhookRetryDelays_CoverSeveralHours(t *testing.T) {
	var total time.Duration
	for _, d := range webhookRetryDelays {
		total += d
	}
	assert.Equal(t, 10, webhookMaxAttempts)
	assert.Greater(t, total, 7*time.Hour+30*time.Minute, "an outage of several hours on the receiver loses nothing")
}

func TestProcessDueWebhookRetries_SendsOnlyDueDeliveries(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusServiceUnavailable))
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)

	var deliveries []models.WebhookDelivery
	require.NoError(t, f.app.DB.Where("webhook_id = ?", f.webhook.ID).Order("created_at").Find(&deliveries).Error)
	require.Len(t, deliveries, 2)
	due, notDue := deliveries[0].ID, deliveries[1].ID
	f.makeDue(t, due)
	f.setRespond(respondStatus(http.StatusOK))

	f.app.ProcessDueWebhookRetries(context.Background())

	assert.Equal(t, models.WebhookDeliverySuccess, f.reload(t, due).Status)
	assert.Equal(t, 2, f.reload(t, due).AttemptCount)
	assert.Equal(t, models.WebhookDeliveryProcessing, f.reload(t, notDue).Status, "not due yet")
	assert.Equal(t, 1, f.reload(t, notDue).AttemptCount)
}

func TestProcessDueWebhookRetries_ClaimHidesDeliveryFromOtherProcessors(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusServiceUnavailable))
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)
	id := f.onlyDelivery(t).ID
	f.makeDue(t, id)

	// Claim it as one processor would, without sending
	now := time.Now()
	claimed, err := claimWebhookRetries.Run(context.Background(), f.app.Redis, []string{webhookRetryQueueKey},
		now.UnixMilli(), now.Add(webhookRetryLease).UnixMilli(), 1000).StringSlice()
	require.NoError(t, err)
	require.Contains(t, claimed, id.String())

	due, queued := f.retryDue(t, id)
	require.True(t, queued, "a claimed delivery stays queued until it is finished")
	assert.WithinDuration(t, now.Add(webhookRetryLease), due, time.Second, "pushed to the end of the lease")

	again, err := claimWebhookRetries.Run(context.Background(), f.app.Redis, []string{webhookRetryQueueKey},
		time.Now().UnixMilli(), time.Now().Add(webhookRetryLease).UnixMilli(), 1000).StringSlice()
	require.NoError(t, err)
	assert.NotContains(t, again, id.String(), "a second processor does not get the same delivery")
}

func TestWebhookRetry_RecoversDeliveryInterruptedBeforeFirstAttempt(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusOK))

	// The process recorded and queued the delivery, then died before sending.
	body := []byte(`{"event":"message.sent","data":{"message_id":"msg-crash"}}`)
	id := f.app.startWebhookDelivery(uuid.New(), f.webhook, "message.sent", sentEvent, body,
		webhookRequestHeaders(f.webhook, body, uuid.Nil))
	require.NotEqual(t, uuid.Nil, id)
	require.NoError(t, f.app.queueWebhookRetry(context.Background(), id, time.Now().Add(webhookRetryLease)))

	f.makeDue(t, id) // the lease ran out
	f.app.ProcessDueWebhookRetries(context.Background())

	d := f.reload(t, id)
	assert.Equal(t, models.WebhookDeliverySuccess, d.Status)
	assert.Equal(t, 1, d.AttemptCount)
	require.Len(t, f.receivedBodies(), 1)
	assert.JSONEq(t, string(body), string(f.receivedBodies()[0]))
}

func TestRetryWebhookDelivery_DisabledOrDeletedWebhookStops(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusServiceUnavailable))
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)
	id := f.onlyDelivery(t).ID

	require.NoError(t, f.app.DB.Model(&models.Webhook{}).Where("id = ?", f.webhook.ID).Update("is_active", false).Error)
	f.makeDue(t, id)
	f.app.retryWebhookDelivery(context.Background(), id.String())

	d := f.reload(t, id)
	assert.Equal(t, models.WebhookDeliveryFailed, d.Status)
	assert.Contains(t, d.FailureReason, "disabled")
	assert.Equal(t, 1, d.AttemptCount, "no request is sent to a disabled webhook")
	_, queued := f.retryDue(t, id)
	assert.False(t, queued)
}

func TestAttemptWebhookDelivery_ShutdownDoesNotSpendAnAttempt(t *testing.T) {
	f := newRetryFixture(t)
	f.setRespond(respondStatus(http.StatusServiceUnavailable))
	f.app.sendWebhook(context.Background(), f.webhook, string(models.WebhookEventMessageSent), sentEvent)
	d := f.onlyDelivery(t)
	f.makeDue(t, d.ID)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.app.retryWebhookDelivery(ctx, d.ID.String())

	after := f.reload(t, d.ID)
	assert.Equal(t, models.WebhookDeliveryProcessing, after.Status)
	assert.Equal(t, 1, after.AttemptCount, "an attempt cut short by shutdown is not counted")
	_, queued := f.retryDue(t, d.ID)
	assert.True(t, queued, "it is sent again after the restart")
}

func TestFailStaleWebhookDeliveries_RespectsScheduledRetry(t *testing.T) {
	f := newRetryFixture(t)
	future := time.Now().Add(time.Hour)
	overdue := time.Now().Add(-time.Hour)
	waiting := f.insertDelivery(t, 3*time.Hour, models.WebhookDeliveryProcessing, func(d *models.WebhookDelivery) { d.NextAttemptAt = &future })
	lost := f.insertDelivery(t, 3*time.Hour, models.WebhookDeliveryProcessing, func(d *models.WebhookDelivery) { d.NextAttemptAt = &overdue })

	f.app.FailStaleWebhookDeliveries()

	assert.Equal(t, models.WebhookDeliveryProcessing, f.reload(t, waiting.ID).Status, "an old delivery with a retry still ahead is kept")
	assert.Equal(t, models.WebhookDeliveryFailed, f.reload(t, lost.ID).Status, "a retry long overdue was lost")
}
