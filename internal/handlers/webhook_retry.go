package handlers

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shridarpatil/whatomate/internal/models"
)

// Webhook deliveries survive restarts: before the first attempt a delivery's
// ID is put in a Redis sorted set scored by when it is next due, and the retry
// processor sends whatever is due. The request body and attempt history live
// in the webhook_deliveries row, so Redis only holds IDs and times.
const (
	webhookRetryQueueKey = "whatomate:webhook_retries"
	// webhookRetryLease is how long a claimed (or in-flight) delivery is
	// hidden from other processors. If the process dies mid-attempt the
	// delivery becomes due again once the lease ends.
	webhookRetryLease        = 2 * time.Minute
	webhookRetryPollInterval = 2 * time.Second
	webhookRetryBatch        = 50
	webhookRetryConcurrency  = 10
	webhookRetryAttemptLimit = 30 * time.Second
)

// webhookRetryDelays[n-1] is the wait after failed attempt n. Attempt 1 is
// immediate; the last retry is about 7h50m after the event.
var webhookRetryDelays = []time.Duration{
	5 * time.Second, 30 * time.Second, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute,
	30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour,
}

// webhookMaxAttempts is the first attempt plus one per retry delay.
var webhookMaxAttempts = len(webhookRetryDelays) + 1

// claimWebhookRetries atomically takes up to ARGV[3] deliveries due by
// ARGV[1] (unix ms) and pushes their score to ARGV[2], the end of the lease,
// so two processors never send the same retry at once.
var claimWebhookRetries = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3]))
for _, id in ipairs(due) do
  redis.call('ZADD', KEYS[1], ARGV[2], id)
end
return due
`)

// queueWebhookRetry (re)schedules a delivery to be sent at `at`.
func (a *App) queueWebhookRetry(ctx context.Context, deliveryID uuid.UUID, at time.Time) error {
	return a.Redis.ZAdd(ctx, webhookRetryQueueKey, redis.Z{Score: float64(at.UnixMilli()), Member: deliveryID.String()}).Err()
}

func (a *App) dequeueWebhookRetry(deliveryID string) {
	if err := a.Redis.ZRem(context.Background(), webhookRetryQueueKey, deliveryID).Err(); err != nil {
		a.Log.Error("Failed to remove webhook delivery from retry queue", "error", err, "delivery_id", deliveryID)
	}
}

// attemptWebhookDelivery sends one attempt of a queued delivery, records it,
// and either finishes the delivery or schedules the next attempt.
func (a *App) attemptWebhookDelivery(ctx context.Context, webhook models.Webhook, deliveryID uuid.UUID, attempt int, body []byte, headers map[string]string) {
	result := a.sendWebhookRequest(ctx, webhook, body, headers)

	// Shutting down: don't spend an attempt; the lease runs out and the
	// delivery is picked up again after the restart.
	if !result.ok() && errors.Is(ctx.Err(), context.Canceled) {
		a.Log.Warn("Webhook attempt interrupted by shutdown", "delivery_id", deliveryID, "attempt", attempt)
		return
	}

	final := result.ok() || attempt >= webhookMaxAttempts
	a.recordWebhookAttempt(deliveryID, attempt, result, final)
	if final {
		a.dequeueWebhookRetry(deliveryID.String())
		if !result.ok() {
			a.Log.Error("Webhook delivery failed after all attempts",
				"delivery_id", deliveryID, "webhook_id", webhook.ID, "attempts", attempt, "url", webhook.URL)
		}
		return
	}

	next := time.Now().Add(webhookRetryDelays[attempt-1])
	if err := a.DB.Model(&models.WebhookDelivery{}).Where("id = ?", deliveryID).
		Update("next_attempt_at", next).Error; err != nil {
		a.Log.Error("Failed to record next webhook attempt time", "error", err, "delivery_id", deliveryID)
	}
	if err := a.queueWebhookRetry(context.Background(), deliveryID, next); err != nil {
		// The stale sweeper fails the delivery once it is overdue
		a.Log.Error("Failed to schedule webhook retry", "error", err, "delivery_id", deliveryID)
		return
	}
	a.Log.Warn("Webhook delivery failed, retry scheduled",
		"delivery_id", deliveryID, "webhook_id", webhook.ID, "attempt", attempt,
		"next_attempt_at", next, "error", result.Err)
}

// StartWebhookRetryProcessor sends due webhook retries every
// webhookRetryPollInterval until ctx is cancelled. Safe to run in several
// processes: claims are atomic.
func (a *App) StartWebhookRetryProcessor(ctx context.Context) {
	ticker := time.NewTicker(webhookRetryPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.ProcessDueWebhookRetries(ctx)
		}
	}
}

// ProcessDueWebhookRetries claims and sends the deliveries that are due and
// returns how many it claimed.
func (a *App) ProcessDueWebhookRetries(ctx context.Context) int {
	now := time.Now()
	ids, err := claimWebhookRetries.Run(ctx, a.Redis, []string{webhookRetryQueueKey},
		now.UnixMilli(), now.Add(webhookRetryLease).UnixMilli(), webhookRetryBatch).StringSlice()
	if err != nil {
		if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
			a.Log.Error("Failed to claim webhook retries", "error", err)
		}
		return 0
	}

	sem := make(chan struct{}, webhookRetryConcurrency)
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }()
			a.retryWebhookDelivery(ctx, id)
		}(id)
	}
	wg.Wait()
	return len(ids)
}

// retryWebhookDelivery sends the next attempt of one claimed delivery, using
// the stored body and the webhook's current URL, headers and secret.
func (a *App) retryWebhookDelivery(ctx context.Context, id string) {
	deliveryID, err := uuid.Parse(id)
	if err != nil {
		a.dequeueWebhookRetry(id)
		return
	}
	var delivery models.WebhookDelivery
	if err := a.DB.Where("id = ?", deliveryID).First(&delivery).Error; err != nil {
		a.Log.Warn("Dropping webhook retry without a delivery record", "delivery_id", id, "error", err)
		a.dequeueWebhookRetry(id)
		return
	}
	if delivery.Status != models.WebhookDeliveryProcessing {
		a.dequeueWebhookRetry(id)
		return
	}

	var webhook models.Webhook
	if err := a.DB.Where("id = ?", delivery.WebhookID).First(&webhook).Error; err != nil {
		a.finishWebhookDeliveryFailed(deliveryID, "Webhook was deleted before the delivery succeeded")
		return
	}
	if !webhook.IsActive {
		a.finishWebhookDeliveryFailed(deliveryID, "Webhook was disabled before the delivery succeeded")
		return
	}

	body := []byte(delivery.RequestBody)
	headers := webhookRequestHeaders(webhook, body, deliveryID)
	attemptCtx, cancel := context.WithTimeout(ctx, webhookRetryAttemptLimit)
	defer cancel()
	a.attemptWebhookDelivery(attemptCtx, webhook, deliveryID, delivery.AttemptCount+1, body, headers)
}

// finishWebhookDeliveryFailed closes a delivery that can no longer be sent.
func (a *App) finishWebhookDeliveryFailed(deliveryID uuid.UUID, reason string) {
	if err := a.DB.Model(&models.WebhookDelivery{}).
		Where("id = ? AND status = ?", deliveryID, models.WebhookDeliveryProcessing).
		Updates(map[string]any{
			"status":         models.WebhookDeliveryFailed,
			"failure_reason": reason,
			"completed_at":   time.Now(),
		}).Error; err != nil {
		a.Log.Error("Failed to close webhook delivery", "error", err, "delivery_id", deliveryID)
	}
	a.dequeueWebhookRetry(deliveryID.String())
}
