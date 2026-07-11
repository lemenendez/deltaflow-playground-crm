package contactsruntime

import (
	"database/sql"
	"encoding/json"
	"testing"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestContactIDFromIdentity(t *testing.T) {
	id, err := contactIDFromIdentity(deltaflow.ProjectionIdentity{
		Type: deltaflow.ProjectionType("contact"),
		Key: deltaflow.ProjectionKey{
			"contact_id": json.RawMessage(`"c-001"`),
		},
	})
	if err != nil {
		t.Fatalf("contactIDFromIdentity error: %v", err)
	}
	if id != "c-001" {
		t.Fatalf("id = %q, want c-001", id)
	}
}

func TestContactIDFromIdentityRequiresKey(t *testing.T) {
	_, err := contactIDFromIdentity(deltaflow.ProjectionIdentity{Key: deltaflow.ProjectionKey{}})
	if err == nil {
		t.Fatal("contactIDFromIdentity error = nil")
	}
}

func TestNewContactProjectorRejectsInvalidTableName(t *testing.T) {
	_, err := NewContactProjector(nil, "app_contacts")
	if err == nil {
		t.Fatal("NewContactProjector expected db error")
	}

	_, err = NewContactProjector(newTestDBHandle(), "app-contacts")
	if err == nil {
		t.Fatal("NewContactProjector expected invalid table error")
	}
}

func newTestDBHandle() *sql.DB { return &sql.DB{} }
