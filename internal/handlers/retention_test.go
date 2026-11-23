package handlers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shridarpatil/whatomate/internal/config"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunRetention_DeletesOnlyOldFinishedWebhookLogs(t *testing.T) {
	f := newWebhookLogsFixture(t)
	f.app.Config.Retention = config.RetentionConfig{WebhookLogsDays: 30, QueueStreamDays: -1}
	day := 24 * time.Hour

	oldSuccess := f.insertDelivery(t, 40*day, models.WebhookDeliverySuccess, nil)
	oldFailed := f.insertDelivery(t, 31*day, models.WebhookDeliveryFailed, nil)
	oldWaiting := f.insertDelivery(t, 40*day, models.WebhookDeliveryProcessing, nil)
	recent := f.insertDelivery(t, 29*day, models.WebhookDeliverySuccess, nil)

	result := f.app.RunRetention(context.Background(), time.Now())
	assert.GreaterOrEqual(t, result.WebhookDeliveriesDeleted, int64(2))
	assert.Zero(t, result.QueueEntriesTrimmed, "stream trimming is off")

	exists := func(id uuid.UUID) bool {
		var n int64
		f.app.DB.Model(&models.WebhookDelivery{}).Where("id = ?", id).Count(&n)
		return n == 1
	}
	assert.False(t, exists(oldSuccess.ID))
	assert.False(t, exists(oldFailed.ID))
	assert.True(t, exists(oldWaiting.ID), "a delivery waiting for a retry is never deleted")
	assert.True(t, exists(recent.ID))
}

func TestRunRetention_NegativeDaysDisablesCleanup(t *testing.T) {
	f := newWebhookLogsFixture(t)
	f.app.Config.Retention = config.RetentionConfig{WebhookLogsDays: -1, QueueStreamDays: -1}
	old := f.insertDelivery(t, 400*24*time.Hour, models.WebhookDeliverySuccess, nil)

	result := f.app.RunRetention(context.Background(), time.Now())
	assert.Zero(t, result.WebhookDeliveriesDeleted)
	var n int64
	f.app.DB.Model(&models.WebhookDelivery{}).Where("id = ?", old.ID).Count(&n)
	assert.EqualValues(t, 1, n)
}

func TestTrimStreams_RemovesOnlyEntriesOlderThanCutoff(t *testing.T) {
	app := webhookTestApp(t)
	ctx := context.Background()
	stream := "whatomate:test:retention:" + uuid.NewString()[:8]
	t.Cleanup(func() { app.Redis.Del(ctx, stream) })

	now := time.Now()
	add := func(at time.Time) {
		require.NoError(t, app.Redis.XAdd(ctx, &redis.XAddArgs{
			Stream: stream, ID: fmt.Sprintf("%d-0", at.UnixMilli()), Values: map[string]any{"job": "x"},
		}).Err())
	}
	add(now.Add(-20 * 24 * time.Hour))
	add(now.Add(-15 * 24 * time.Hour))
	add(now.Add(-time.Hour))

	trimmed := app.trimStreams(ctx, []string{stream}, now.Add(-14*24*time.Hour))
	assert.EqualValues(t, 2, trimmed)
	assert.EqualValues(t, 1, app.Redis.XLen(ctx, stream).Val())
}

func TestRunRetentionIfDue_RunsOncePerDayAcrossInstances(t *testing.T) {
	app := webhookTestApp(t)
	app.Config.Retention = config.RetentionConfig{WebhookLogsDays: -1, QueueStreamDays: -1}
	ctx := context.Background()
	app.Redis.Del(ctx, retentionRunKey)
	t.Cleanup(func() { app.Redis.Del(ctx, retentionRunKey) })

	app.runRetentionIfDue(ctx)
	ttl := app.Redis.TTL(ctx, retentionRunKey).Val()
	assert.Greater(t, ttl, 22*time.Hour, "the run is recorded for about a day")

	// A second instance (or the next hourly tick) does not run again
	before := app.Redis.Get(ctx, retentionRunKey).Val()
	app.runRetentionIfDue(ctx)
	assert.Equal(t, before, app.Redis.Get(ctx, retentionRunKey).Val())
}
