package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/shridarpatil/whatomate/internal/queue"
	"gorm.io/gorm"
)

const (
	// retentionCheckInterval is how often the sweeper checks whether a run is due
	retentionCheckInterval = time.Hour
	// retentionRunKey makes one run a day happen across all instances
	retentionRunKey = "whatomate:retention:last_run"
	retentionRunTTL = 23 * time.Hour
)

// RetentionResult is what one cleanup run removed.
type RetentionResult struct {
	WebhookDeliveriesDeleted int64
	QueueEntriesTrimmed      int64
}

// StartRetentionSweeper runs the cleanup about once a day (at most once per
// retentionRunTTL across instances) until ctx is cancelled.
func (a *App) StartRetentionSweeper(ctx context.Context) {
	a.runRetentionIfDue(ctx)
	ticker := time.NewTicker(retentionCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.runRetentionIfDue(ctx)
		}
	}
}

func (a *App) runRetentionIfDue(ctx context.Context) {
	due, err := a.Redis.SetNX(ctx, retentionRunKey, time.Now().UTC().Format(time.RFC3339), retentionRunTTL).Result()
	if err != nil {
		if ctx.Err() == nil {
			a.Log.Error("Retention: failed to check whether a run is due", "error", err)
		}
		return
	}
	if !due {
		return
	}
	result := a.RunRetention(ctx, time.Now())
	a.Log.Info("Retention cleanup finished",
		"webhook_deliveries_deleted", result.WebhookDeliveriesDeleted,
		"queue_entries_trimmed", result.QueueEntriesTrimmed)
}

// RunRetention deletes webhook delivery logs and trims Redis queue streams
// older than the configured retention, relative to now.
func (a *App) RunRetention(ctx context.Context, now time.Time) RetentionResult {
	var result RetentionResult
	cfg := a.Config.Retention
	if cfg.WebhookLogsDays > 0 {
		result.WebhookDeliveriesDeleted = a.deleteOldWebhookDeliveries(now.AddDate(0, 0, -cfg.WebhookLogsDays))
	}
	if cfg.QueueStreamDays > 0 {
		result.QueueEntriesTrimmed = a.trimStreams(ctx, []string{queue.StreamName, queue.ExportStreamName},
			now.AddDate(0, 0, -cfg.QueueStreamDays))
	}
	return result
}

// deleteOldWebhookDeliveries removes finished deliveries created before
// cutoff, one organization at a time so each batch uses the
// (organization_id, created_at) index. Deliveries still processing are kept:
// they may be waiting for a retry.
func (a *App) deleteOldWebhookDeliveries(cutoff time.Time) int64 {
	var orgIDs []uuid.UUID
	if err := a.DB.Model(&models.WebhookDelivery{}).Distinct("organization_id").
		Where("created_at < ?", cutoff).Pluck("organization_id", &orgIDs).Error; err != nil {
		a.Log.Error("Retention: failed to list organizations with old webhook logs", "error", err)
		return 0
	}
	var total int64
	for _, orgID := range orgIDs {
		total += a.purgeWebhookDeliveries(func(db *gorm.DB) *gorm.DB {
			return db.Where("organization_id = ? AND created_at < ? AND status <> ?",
				orgID, cutoff, models.WebhookDeliveryProcessing)
		})
	}
	return total
}

// trimStreams removes stream entries added before cutoff (stream ids start
// with their unix-ms time). Queue entries are acknowledged once processed, so
// only stale ones are older than the retention.
func (a *App) trimStreams(ctx context.Context, streams []string, cutoff time.Time) int64 {
	minID := fmt.Sprintf("%d-0", cutoff.UnixMilli())
	var total int64
	for _, stream := range streams {
		n, err := a.Redis.XTrimMinID(ctx, stream, minID).Result()
		if err != nil {
			a.Log.Error("Retention: failed to trim queue stream", "error", err, "stream", stream)
			continue
		}
		total += n
	}
	return total
}
