package contactutil

import (
	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetOrCreateContact finds or creates a contact for the given phone number.
// Merges behaviors from both handler and worker implementations:
//   - Normalizes phone (strips leading "+")
//   - Tries both normalized and +prefix forms
//   - Updates profile name if changed
//   - Handles race conditions on create by re-fetching
//   - Restores soft-deleted contacts if found
//
// Returns the contact, whether it was newly created, and any error.
func GetOrCreateContact(db *gorm.DB, orgID uuid.UUID, phoneNumber, profileName string) (*models.Contact, bool, error) {
	// Normalize phone number (remove + prefix if present)
	normalizedPhone := phoneNumber
	if len(normalizedPhone) > 0 && normalizedPhone[0] == '+' {
		normalizedPhone = normalizedPhone[1:]
	}

	// Try to find existing contact with normalized phone (including soft-deleted)
	var contact models.Contact
	if err := db.Unscoped().Where("organization_id = ? AND phone_number = ?", orgID, normalizedPhone).First(&contact).Error; err == nil {
		// Restore if soft-deleted
		if contact.DeletedAt.Valid {
			db.Unscoped().Model(&contact).Update("deleted_at", nil)
			contact.DeletedAt.Valid = false
		}
		// Update profile name if changed
		if profileName != "" && contact.ProfileName != profileName {
			db.Model(&contact).Update("profile_name", profileName)
		}
		return &contact, false, nil
	}

	// Also try with + prefix (contacts may have been stored with it)
	if err := db.Unscoped().Where("organization_id = ? AND phone_number = ?", orgID, "+"+normalizedPhone).First(&contact).Error; err == nil {
		// Restore if soft-deleted
		if contact.DeletedAt.Valid {
			db.Unscoped().Model(&contact).Update("deleted_at", nil)
			contact.DeletedAt.Valid = false
		}
		if profileName != "" && contact.ProfileName != profileName {
			db.Model(&contact).Update("profile_name", profileName)
		}
		return &contact, false, nil
	}

	// Create new contact
	contact = models.Contact{
		BaseModel:      models.BaseModel{ID: uuid.New()},
		OrganizationID: orgID,
		PhoneNumber:    normalizedPhone,
		ProfileName:    profileName,
	}
	// Another request may create the same contact at the same moment
	// (idx_contacts_org_phone). ON CONFLICT DO NOTHING inserts nothing then,
	// without an error that would abort a surrounding transaction, and the
	// existing row is loaded below.
	result := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&contact)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 1 {
		return &contact, true, nil
	}

	// A fresh struct: reusing contact would carry its unsaved ID into the
	// WHERE clause and never match the row the other request created.
	var existing models.Contact
	if err := db.Unscoped().Where("organization_id = ? AND phone_number = ?", orgID, normalizedPhone).First(&existing).Error; err != nil {
		return nil, false, err
	}
	// Restore if soft-deleted
	if existing.DeletedAt.Valid {
		db.Unscoped().Model(&existing).Update("deleted_at", nil)
		existing.DeletedAt.Valid = false
	}
	if profileName != "" && existing.ProfileName != profileName {
		db.Model(&existing).Update("profile_name", profileName)
	}
	return &existing, false, nil
}

// FindContact finds a contact for the given phone number with both forms (normalized and +prefix).
func FindContact(db *gorm.DB, orgID uuid.UUID, phoneNumber string) (*models.Contact, error) {
	normalizedPhone := phoneNumber
	if len(normalizedPhone) > 0 && normalizedPhone[0] == '+' {
		normalizedPhone = normalizedPhone[1:]
	}

	var contact models.Contact
	if err := db.Where("organization_id = ? AND phone_number = ?", orgID, normalizedPhone).First(&contact).Error; err == nil {
		return &contact, nil
	}

	if err := db.Where("organization_id = ? AND phone_number = ?", orgID, "+"+normalizedPhone).First(&contact).Error; err == nil {
		return &contact, nil
	}

	return nil, gorm.ErrRecordNotFound
}
