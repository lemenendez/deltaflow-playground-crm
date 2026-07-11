package contactsruntime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

var validIdentifierPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

type ContactProjector struct {
	db    *sql.DB
	table string
}

func NewContactProjector(db *sql.DB, table string) (*ContactProjector, error) {
	if db == nil {
		return nil, errors.New("contact projector: database is required")
	}
	if !validIdentifierPattern.MatchString(table) {
		return nil, fmt.Errorf("contact projector: invalid source table %q", table)
	}
	return &ContactProjector{db: db, table: table}, nil
}

func (p *ContactProjector) Project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	contactID, err := contactIDFromIdentity(identity)
	if err != nil {
		return deltaflow.Projection{}, err
	}

	query := fmt.Sprintf(`SELECT full_name, email, updated_at FROM "%s" WHERE id = $1`, p.table)
	var fullName string
	var email string
	var updatedAt time.Time
	if err := p.db.QueryRowContext(ctx, query, contactID).Scan(&fullName, &email, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
		}
		return deltaflow.Projection{}, err
	}

	payload, err := json.Marshal(map[string]any{
		"contact_id": contactID,
		"full_name":  fullName,
		"email":      email,
		"updated_at": updatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return deltaflow.Projection{}, err
	}

	return deltaflow.Projection{
		Identity:  identity,
		Payload:   payload,
		MediaType: "application/json",
	}, nil
}

func contactIDFromIdentity(identity deltaflow.ProjectionIdentity) (string, error) {
	raw, ok := identity.Key["contact_id"]
	if !ok {
		return "", errors.New("contact projector: projection key contact_id is required")
	}

	var contactID string
	if err := json.Unmarshal(raw, &contactID); err != nil {
		return "", fmt.Errorf("contact projector: decode contact_id: %w", err)
	}
	if contactID == "" {
		return "", errors.New("contact projector: projection key contact_id is required")
	}

	return contactID, nil
}
