package handlers

import (
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
)

// Outbound webhook events name the sending account by its Whatomate name.
// Receivers that key their own configuration on the WhatsApp business number
// also get it as business_phone, read from the account.

// businessPhoneCacheTTL bounds how stale a cached business number can be on
// another instance after the account changes.
const businessPhoneCacheTTL = 5 * time.Minute

type businessPhoneEntry struct {
	phone   string
	expires time.Time
}

// businessPhoneCache maps "orgID|accountName" to the account's business number.
var businessPhoneCache sync.Map

// normalizeBusinessPhone keeps only digits, so "+91 83568 50985" becomes
// "918356850985". Returns "" when there are none.
func normalizeBusinessPhone(raw string) string {
	var b strings.Builder
	for _, ch := range raw {
		if ch >= '0' && ch <= '9' {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

// businessPhoneFor returns the business number of an organization's account
// by name, or "" when it is unknown.
func (a *App) businessPhoneFor(orgID uuid.UUID, accountName string) string {
	if accountName == "" {
		return ""
	}
	key := orgID.String() + "|" + accountName
	if v, ok := businessPhoneCache.Load(key); ok {
		entry := v.(businessPhoneEntry)
		if time.Now().Before(entry.expires) {
			return entry.phone
		}
	}
	var phone string
	if err := a.DB.Model(&models.WhatsAppAccount{}).
		Where("organization_id = ? AND name = ?", orgID, accountName).
		Limit(1).Pluck("phone_number", &phone).Error; err != nil {
		a.Log.Error("Failed to look up account business number", "error", err, "account", accountName)
		return ""
	}
	businessPhoneCache.Store(key, businessPhoneEntry{phone: phone, expires: time.Now().Add(businessPhoneCacheTTL)})
	return phone
}

// forgetBusinessPhones drops an organization's cached business numbers, e.g.
// after an account is renamed or its number changes.
func (a *App) forgetBusinessPhones(orgID uuid.UUID) {
	prefix := orgID.String() + "|"
	businessPhoneCache.Range(func(k, _ any) bool {
		if strings.HasPrefix(k.(string), prefix) {
			businessPhoneCache.Delete(k)
		}
		return true
	})
}

// rememberBusinessPhone stores the number Meta reports for an account (from
// display_phone_number) when it differs from the stored one. Cheap to call on
// every webhook: it writes only on a change.
func (a *App) rememberBusinessPhone(account *models.WhatsAppAccount, displayPhone string) {
	phone := normalizeBusinessPhone(displayPhone)
	if account == nil || phone == "" || phone == account.PhoneNumber {
		return
	}
	if err := a.DB.Model(&models.WhatsAppAccount{}).Where("id = ?", account.ID).
		Update("phone_number", phone).Error; err != nil {
		a.Log.Error("Failed to store account business number", "error", err, "account", account.Name)
		return
	}
	a.Log.Info("Stored account business number from Meta", "account", account.Name, "phone_id", account.PhoneID)
	account.PhoneNumber = phone
	a.InvalidateWhatsAppAccountCache(account.PhoneID)
	a.forgetBusinessPhones(account.OrganizationID)
}

// businessPhoneSettable is implemented by event payloads that carry the
// sending account's business number. withBusinessPhone returns a copy.
type businessPhoneSettable interface {
	webhookAccountScoped
	businessPhone() string
	withBusinessPhone(phone string) any
}

func (d MessageEventData) businessPhone() string       { return d.BusinessPhone }
func (d MessageFailedEventData) businessPhone() string { return d.BusinessPhone }
func (d MessageStatusEventData) businessPhone() string { return d.BusinessPhone }
func (d FlowResponseEventData) businessPhone() string  { return d.BusinessPhone }
func (d ContactEventData) businessPhone() string       { return d.BusinessPhone }
func (d TransferEventData) businessPhone() string      { return d.BusinessPhone }
func (d TemplateStatusEventData) businessPhone() string {
	return d.BusinessPhone
}

func (d MessageEventData) withBusinessPhone(p string) any       { d.BusinessPhone = p; return d }
func (d MessageFailedEventData) withBusinessPhone(p string) any { d.BusinessPhone = p; return d }
func (d MessageStatusEventData) withBusinessPhone(p string) any { d.BusinessPhone = p; return d }
func (d FlowResponseEventData) withBusinessPhone(p string) any  { d.BusinessPhone = p; return d }
func (d ContactEventData) withBusinessPhone(p string) any       { d.BusinessPhone = p; return d }
func (d TransferEventData) withBusinessPhone(p string) any      { d.BusinessPhone = p; return d }
func (d TemplateStatusEventData) withBusinessPhone(p string) any {
	d.BusinessPhone = p
	return d
}

// withEventBusinessPhone fills business_phone on an event payload that has
// an account but no number yet. Other payloads are returned unchanged.
func (a *App) withEventBusinessPhone(orgID uuid.UUID, data any) any {
	event, ok := data.(businessPhoneSettable)
	if !ok || event.businessPhone() != "" {
		return data
	}
	phone := a.businessPhoneFor(orgID, event.webhookAccountName())
	if phone == "" {
		return data
	}
	return event.withBusinessPhone(phone)
}
