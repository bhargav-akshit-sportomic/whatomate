package contactutil

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shridarpatil/whatomate/internal/models"
	"github.com/shridarpatil/whatomate/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGetOrCreateContact_CreatesNew(t *testing.T) {
	db := testutil.SetupTestDB(t)
	uid := uuid.New().String()[:8]
	org := models.Organization{BaseModel: models.BaseModel{ID: uuid.New()}, Name: "test-" + uid, Slug: "test-" + uid}
	require.NoError(t, db.Create(&org).Error)

	contact, isNew, err := GetOrCreateContact(db, org.ID, "1234567890", "Alice")
	require.NoError(t, err)
	assert.True(t, isNew)
	assert.Equal(t, "1234567890", contact.PhoneNumber)
	assert.Equal(t, "Alice", contact.ProfileName)
}

func TestGetOrCreateContact_FindsExisting(t *testing.T) {
	db := testutil.SetupTestDB(t)
	uid := uuid.New().String()[:8]
	org := models.Organization{BaseModel: models.BaseModel{ID: uuid.New()}, Name: "test-" + uid, Slug: "test-" + uid}
	require.NoError(t, db.Create(&org).Error)

	existing := models.Contact{
		BaseModel:      models.BaseModel{ID: uuid.New()},
		OrganizationID: org.ID,
		PhoneNumber:    "1234567890",
		ProfileName:    "Alice",
	}
	require.NoError(t, db.Create(&existing).Error)

	contact, isNew, err := GetOrCreateContact(db, org.ID, "1234567890", "Alice")
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, existing.ID, contact.ID)
}

func TestGetOrCreateContact_NormalizesPlus(t *testing.T) {
	db := testutil.SetupTestDB(t)
	uid := uuid.New().String()[:8]
	org := models.Organization{BaseModel: models.BaseModel{ID: uuid.New()}, Name: "test-" + uid, Slug: "test-" + uid}
	require.NoError(t, db.Create(&org).Error)

	existing := models.Contact{
		BaseModel:      models.BaseModel{ID: uuid.New()},
		OrganizationID: org.ID,
		PhoneNumber:    "1234567890",
		ProfileName:    "Bob",
	}
	require.NoError(t, db.Create(&existing).Error)

	contact, isNew, err := GetOrCreateContact(db, org.ID, "+1234567890", "Bob")
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, existing.ID, contact.ID)
}

func TestGetOrCreateContact_FindsPlusPrefix(t *testing.T) {
	db := testutil.SetupTestDB(t)
	uid := uuid.New().String()[:8]
	org := models.Organization{BaseModel: models.BaseModel{ID: uuid.New()}, Name: "test-" + uid, Slug: "test-" + uid}
	require.NoError(t, db.Create(&org).Error)

	existing := models.Contact{
		BaseModel:      models.BaseModel{ID: uuid.New()},
		OrganizationID: org.ID,
		PhoneNumber:    "+1234567890",
		ProfileName:    "Charlie",
	}
	require.NoError(t, db.Create(&existing).Error)

	contact, isNew, err := GetOrCreateContact(db, org.ID, "1234567890", "Charlie")
	require.NoError(t, err)
	assert.False(t, isNew)
	assert.Equal(t, existing.ID, contact.ID)
}

func TestGetOrCreateContact_UpdatesProfileName(t *testing.T) {
	db := testutil.SetupTestDB(t)
	uid := uuid.New().String()[:8]
	org := models.Organization{BaseModel: models.BaseModel{ID: uuid.New()}, Name: "test-" + uid, Slug: "test-" + uid}
	require.NoError(t, db.Create(&org).Error)

	existing := models.Contact{
		BaseModel:      models.BaseModel{ID: uuid.New()},
		OrganizationID: org.ID,
		PhoneNumber:    "1234567890",
		ProfileName:    "Old Name",
	}
	require.NoError(t, db.Create(&existing).Error)

	contact, isNew, err := GetOrCreateContact(db, org.ID, "1234567890", "New Name")
	require.NoError(t, err)
	assert.False(t, isNew)

	var reloaded models.Contact
	require.NoError(t, db.First(&reloaded, contact.ID).Error)
	assert.Equal(t, "New Name", reloaded.ProfileName)
}

// requireContactUniqueIndex creates the unique index that `-migrate` adds in
// production (internal/database/postgres.go); the test schema only has the
// tables. Skips when existing test rows already break it.
func requireContactUniqueIndex(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_org_phone ON contacts(organization_id, phone_number)`).Error; err != nil {
		t.Skipf("cannot create idx_contacts_org_phone on the test database: %v", err)
	}
}

// Concurrent first messages from a new number must all get the same contact,
// with exactly one of them creating it, and none may fail.
func TestGetOrCreateContact_ConcurrentCreatesReturnSameContact(t *testing.T) {
	db := testutil.SetupTestDB(t)
	requireContactUniqueIndex(t, db)
	org := testutil.CreateTestOrganization(t, db)

	for round := 0; round < 5; round++ {
		phone := "9198" + uuid.NewString()[:8]
		const n = 12
		var wg sync.WaitGroup
		ids := make([]uuid.UUID, n)
		created := make([]bool, n)
		errs := make([]error, n)
		start := make(chan struct{})
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				c, isNew, err := GetOrCreateContact(db, org.ID, phone, "Player")
				errs[i], created[i] = err, isNew
				if c != nil {
					ids[i] = c.ID
				}
			}(i)
		}
		close(start)
		wg.Wait()

		newCount := 0
		for i := 0; i < n; i++ {
			require.NoError(t, errs[i], "round %d caller %d", round, i)
			assert.Equal(t, ids[0], ids[i], "round %d: every caller gets the same contact", round)
			if created[i] {
				newCount++
			}
		}
		assert.Equal(t, 1, newCount, "round %d: exactly one caller creates the contact", round)

		var count int64
		db.Model(&models.Contact{}).Where("organization_id = ? AND phone_number = ?", org.ID, phone).Count(&count)
		assert.EqualValues(t, 1, count)
	}
}

// The conflict path inside a transaction: tx2 does not yet see tx1's
// uncommitted contact, so it inserts, waits on the unique index, and gets a
// conflict once tx1 commits. It must return tx1's contact and leave tx2 usable.
func TestGetOrCreateContact_ConflictInsideTransactionKeepsItUsable(t *testing.T) {
	db := testutil.SetupTestDB(t)
	requireContactUniqueIndex(t, db)
	org := testutil.CreateTestOrganization(t, db)
	phone := "9197" + uuid.NewString()[:8]

	tx1 := db.Begin()
	first, isNew, err := GetOrCreateContact(tx1, org.ID, phone, "")
	require.NoError(t, err)
	require.True(t, isNew)

	tx2 := db.Begin()
	t.Cleanup(func() { tx2.Rollback() })
	type res struct {
		c     *models.Contact
		isNew bool
		err   error
	}
	done := make(chan res, 1)
	go func() {
		c, n, e := GetOrCreateContact(tx2, org.ID, phone, "")
		done <- res{c, n, e}
	}()

	time.Sleep(300 * time.Millisecond) // tx2 is now blocked on the unique index
	require.NoError(t, tx1.Commit().Error)

	var r res
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("GetOrCreateContact did not return after the other transaction committed")
	}
	require.NoError(t, r.err)
	assert.False(t, r.isNew)
	assert.Equal(t, first.ID, r.c.ID, "returns the contact the other transaction created")

	var n int64
	require.NoError(t, tx2.Model(&models.Contact{}).Where("id = ?", first.ID).Count(&n).Error,
		"the transaction is still usable after the conflict")
	assert.EqualValues(t, 1, n)
}
