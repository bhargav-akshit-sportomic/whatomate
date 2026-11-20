package handlers

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/shridarpatil/whatomate/internal/contactutil"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/valyala/fasthttp"
	"github.com/zerodha/fastglue"
)

// SendPhoneMessageRequest is a session message addressed by phone number
// instead of contact id. The contact is created when it does not exist yet.
// The other fields are the same as POST /api/contacts/{id}/messages.
type SendPhoneMessageRequest struct {
	PhoneNumber string `json:"phone_number"`
	SendMessageRequest
}

// normalizeSendPhone strips spaces, dashes and a leading "+" and returns ""
// when what is left is not 8-15 digits (E.164 without the plus).
func normalizeSendPhone(raw string) string {
	phone := strings.NewReplacer(" ", "", "-", "").Replace(strings.TrimSpace(raw))
	phone = strings.TrimPrefix(phone, "+")
	if len(phone) < 8 || len(phone) > 15 {
		return ""
	}
	for _, ch := range phone {
		if ch < '0' || ch > '9' {
			return ""
		}
	}
	return phone
}

// SendMessageToPhone sends a text or interactive (button, list, cta_url,
// flow) session message to a phone number. It is meant for external systems
// using an API key, so it sends synchronously and returns Meta's result: a
// 200 with status "sent" and the wamid, or a 502 with Meta's error.
//
// For flows the caller may set flow_token, flow_action (navigate or
// data_exchange), flow_action_data and header_image_url; the flow does not
// have to be saved in Whatomate (Meta checks it belongs to the sending WABA).
func (a *App) SendMessageToPhone(r *fastglue.Request) error {
	orgID, userID, err := a.getOrgAndUserID(r)
	if err != nil {
		return r.SendErrorEnvelope(fasthttp.StatusUnauthorized, "Unauthorized", nil, "")
	}

	var req SendPhoneMessageRequest
	if err := json.Unmarshal(r.RequestCtx.PostBody(), &req); err != nil {
		return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "Invalid request body", nil, "")
	}
	phone := normalizeSendPhone(req.PhoneNumber)
	if phone == "" {
		return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "phone_number must be 8-15 digits with country code", nil, "")
	}
	switch req.Type {
	case models.MessageTypeText:
		if strings.TrimSpace(req.Content.Body) == "" {
			return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "content.body is required for a text message", nil, "")
		}
	case models.MessageTypeInteractive:
		if req.Interactive == nil {
			return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "interactive is required for an interactive message", nil, "")
		}
		switch req.Interactive.Type {
		case "button", "list", "cta_url", "flow":
		default:
			return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "interactive.type must be button, list, cta_url or flow", nil, "")
		}
	default:
		return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "type must be text or interactive (use /api/messages/template for templates)", nil, "")
	}

	contact, isNewContact, err := contactutil.GetOrCreateContact(a.DB, orgID, phone, "")
	if err != nil {
		a.Log.Error("Failed to get or create contact", "error", err, "phone", phone)
		return r.SendErrorEnvelope(fasthttp.StatusInternalServerError, "Failed to create contact", nil, "")
	}

	accountName := contact.WhatsAppAccount
	if req.WhatsAppAccount != "" {
		accountName = req.WhatsAppAccount
	}
	account, err := a.resolveWhatsAppAccount(orgID, accountName)
	if err != nil {
		return r.SendErrorEnvelope(fasthttp.StatusBadRequest, "Failed to resolve WhatsApp account", nil, "")
	}

	if isNewContact {
		a.DispatchWebhook(orgID, models.WebhookEventContactCreated, ContactEventData{
			ContactID:       contact.ID.String(),
			ContactPhone:    contact.PhoneNumber,
			ContactName:     contact.ProfileName,
			WhatsAppAccount: account.Name,
		})
	}

	msgReq := OutgoingMessageRequest{
		Account: account,
		Contact: contact,
		Type:    req.Type,
		Content: req.Content.Body,
	}
	if req.Type == models.MessageTypeInteractive {
		if errMsg := a.applyInteractiveContent(&msgReq, req.SendMessageRequest, orgID, contact, false); errMsg != "" {
			return r.SendErrorEnvelope(fasthttp.StatusBadRequest, errMsg, nil, "")
		}
	}

	// Synchronous so the caller gets Meta's verdict in this response
	opts := userSendOptions(r, userID)
	opts.Async = false
	message, err := a.SendOutgoingMessage(context.Background(), msgReq, opts)
	if err != nil {
		a.Log.Error("Failed to send message", "error", err, "phone", phone)
		return r.SendErrorEnvelope(fasthttp.StatusInternalServerError, "Failed to send message", nil, "")
	}

	// finalizeMessageSend writes the outcome to the row, not to message
	var sent models.Message
	if err := a.DB.Where("id = ?", message.ID).First(&sent).Error; err != nil {
		a.Log.Error("Failed to reload sent message", "error", err, "message_id", message.ID)
		return r.SendErrorEnvelope(fasthttp.StatusInternalServerError, "Failed to read send result", nil, "")
	}

	response := MessageResponse{
		ID:              sent.ID,
		ContactID:       sent.ContactID,
		Direction:       sent.Direction,
		MessageType:     sent.MessageType,
		Content:         map[string]string{"body": sent.Content},
		InteractiveData: sent.InteractiveData,
		Status:          sent.Status,
		WAMID:           sent.WhatsAppMessageID,
		Error:           sent.ErrorMessage,
		WhatsAppAccount: sent.WhatsAppAccount,
		CreatedAt:       sent.CreatedAt,
		UpdatedAt:       sent.UpdatedAt,
	}
	if sent.Status == models.MessageStatusFailed {
		return r.SendErrorEnvelope(fasthttp.StatusBadGateway, "WhatsApp rejected the message: "+sent.ErrorMessage, response, "")
	}
	return r.SendEnvelope(response)
}
