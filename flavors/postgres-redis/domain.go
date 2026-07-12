package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type user struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Roles     []string `json:"roles"`
	Version   int      `json:"version"`
	UpdatedAt string   `json:"updated_at"`
}

type customer struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	VIP       bool   `json:"vip"`
	Version   int    `json:"version"`
	UpdatedAt string `json:"updated_at"`
}

type order struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	UserID     string `json:"user_id"`
	TotalCents int    `json:"total_cents"`
	Status     string `json:"status"`
	Version    int    `json:"version"`
	UpdatedAt  string `json:"updated_at"`
}

type crmStore struct {
	db *sql.DB
}

type crmTarget interface {
	deltaflow.ProjectionApplier
	snapshot(ctx context.Context) ([]string, []string, map[string][]byte, int, int, int, error)
}

func newCRMStore(db *sql.DB) *crmStore {
	return &crmStore{db: db}
}

func (s *crmStore) ensureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE SCHEMA IF NOT EXISTS playground_redis;

CREATE TABLE IF NOT EXISTS playground_redis.crm_users (
	user_id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	roles JSONB NOT NULL,
	version INTEGER NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS playground_redis.crm_customers (
	customer_id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	email TEXT NOT NULL,
	phone TEXT NOT NULL,
	vip BOOLEAN NOT NULL,
	version INTEGER NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS playground_redis.crm_orders (
	order_id TEXT PRIMARY KEY,
	customer_id TEXT NOT NULL,
	user_id TEXT NOT NULL,
	total_cents INTEGER NOT NULL,
	status TEXT NOT NULL,
	version INTEGER NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);`)
	return err
}

func (s *crmStore) reset(ctx context.Context, users []user, customers []customer, orders []order) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, table := range []string{
		"playground_redis.crm_orders",
		"playground_redis.crm_customers",
		"playground_redis.crm_users",
	} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return err
		}
	}

	for _, u := range users {
		roles, err := json.Marshal(u.Roles)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO playground_redis.crm_users (user_id, name, roles, version, updated_at)
VALUES ($1, $2, $3::jsonb, $4, $5)`, u.ID, u.Name, roles, u.Version, parseTime(u.UpdatedAt)); err != nil {
			return err
		}
	}
	for _, c := range customers {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO playground_redis.crm_customers (customer_id, name, email, phone, vip, version, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`, c.ID, c.Name, c.Email, c.Phone, c.VIP, c.Version, parseTime(c.UpdatedAt)); err != nil {
			return err
		}
	}
	for _, o := range orders {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO playground_redis.crm_orders (order_id, customer_id, user_id, total_cents, status, version, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`, o.ID, o.CustomerID, o.UserID, o.TotalCents, o.Status, o.Version, parseTime(o.UpdatedAt)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *crmStore) applyMutation(ctx context.Context, tx *sql.Tx, m mutation) error {
	switch m.Entity {
	case "user":
		return s.applyUserMutation(ctx, tx, m)
	case "customer":
		return s.applyCustomerMutation(ctx, tx, m)
	case "order":
		return s.applyOrderMutation(ctx, tx, m)
	default:
		return fmt.Errorf("unsupported entity %q", m.Entity)
	}
}

func (s *crmStore) applyUserMutation(ctx context.Context, tx *sql.Tx, m mutation) error {
	setSQL := ""
	var value any
	switch m.Kind {
	case "roles":
		raw, err := json.Marshal(m.Value.([]string))
		if err != nil {
			return err
		}
		setSQL = "roles = $1::jsonb"
		value = raw
	case "name":
		setSQL = "name = $1"
		value = m.Value.(string)
	default:
		return fmt.Errorf("unsupported user mutation %q", m.Kind)
	}
	return execEntityUpdate(ctx, tx, "playground_redis.crm_users", setSQL, "user_id", value, m)
}

func (s *crmStore) applyCustomerMutation(ctx context.Context, tx *sql.Tx, m mutation) error {
	setSQL := ""
	var value any
	switch m.Kind {
	case "email":
		setSQL = "email = $1"
		value = m.Value.(string)
	case "phone":
		setSQL = "phone = $1"
		value = m.Value.(string)
	case "vip":
		setSQL = "vip = $1"
		value = m.Value.(bool)
	default:
		return fmt.Errorf("unsupported customer mutation %q", m.Kind)
	}
	return execEntityUpdate(ctx, tx, "playground_redis.crm_customers", setSQL, "customer_id", value, m)
}

func (s *crmStore) applyOrderMutation(ctx context.Context, tx *sql.Tx, m mutation) error {
	setSQL := ""
	var value any
	switch m.Kind {
	case "status":
		setSQL = "status = $1"
		value = m.Value.(string)
	case "total":
		setSQL = "total_cents = $1"
		value = m.Value.(int)
	default:
		return fmt.Errorf("unsupported order mutation %q", m.Kind)
	}
	return execEntityUpdate(ctx, tx, "playground_redis.crm_orders", setSQL, "order_id", value, m)
}

func execEntityUpdate(ctx context.Context, tx *sql.Tx, table string, setSQL string, idColumn string, value any, m mutation) error {
	res, err := tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE %s
SET %s,
	version = version + 1,
	updated_at = $2
WHERE %s = $3`, table, setSQL, idColumn), value, m.At, m.EntityID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s %s not found", m.Entity, m.EntityID)
	}
	return nil
}

func (s *crmStore) project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	id, err := hostpkg.StringFromKey(identity.Key, "id")
	if err != nil {
		return deltaflow.Projection{}, err
	}

	switch identity.Type {
	case userProjection:
		u, ok, err := s.getUser(ctx, id)
		if err != nil || !ok {
			return projectionOrNotFound(err, ok)
		}
		return hostpkg.JSONProjection(identity, map[string]any{
			"cache_key":  "user:" + u.ID,
			"user":       u,
			"search_doc": map[string]any{"id": u.ID, "name": u.Name, "roles": u.Roles},
		})
	case custProjection:
		c, ok, err := s.getCustomer(ctx, id)
		if err != nil || !ok {
			return projectionOrNotFound(err, ok)
		}
		return hostpkg.JSONProjection(identity, map[string]any{
			"cache_key":  "customer:" + c.ID,
			"customer":   c,
			"search_doc": map[string]any{"id": c.ID, "name": c.Name, "email": c.Email, "phone": c.Phone, "vip": c.VIP},
		})
	case orderProjection:
		o, ok, err := s.getOrder(ctx, id)
		if err != nil || !ok {
			return projectionOrNotFound(err, ok)
		}
		return hostpkg.JSONProjection(identity, map[string]any{
			"order_stream":        "orders:events",
			"elasticsearch_index": "orders:index",
			"order":               o,
			"customer_cache_key":  "customer:" + o.CustomerID,
		})
	default:
		return deltaflow.Projection{}, fmt.Errorf("unsupported projection type %q", identity.Type)
	}
}

func projectionOrNotFound(err error, ok bool) (deltaflow.Projection, error) {
	if err != nil {
		return deltaflow.Projection{}, err
	}
	if !ok {
		return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
	}
	return deltaflow.Projection{}, nil
}

func (s *crmStore) getUser(ctx context.Context, id string) (user, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT user_id, name, roles, version, updated_at FROM playground_redis.crm_users WHERE user_id = $1`, id)
	var u user
	var roles []byte
	var updatedAt time.Time
	if err := row.Scan(&u.ID, &u.Name, &roles, &u.Version, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return user{}, false, nil
		}
		return user{}, false, err
	}
	if err := json.Unmarshal(roles, &u.Roles); err != nil {
		return user{}, false, err
	}
	u.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return u, true, nil
}

func (s *crmStore) getCustomer(ctx context.Context, id string) (customer, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT customer_id, name, email, phone, vip, version, updated_at FROM playground_redis.crm_customers WHERE customer_id = $1`, id)
	var c customer
	var updatedAt time.Time
	if err := row.Scan(&c.ID, &c.Name, &c.Email, &c.Phone, &c.VIP, &c.Version, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return customer{}, false, nil
		}
		return customer{}, false, err
	}
	c.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return c, true, nil
}

func (s *crmStore) getOrder(ctx context.Context, id string) (order, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT order_id, customer_id, user_id, total_cents, status, version, updated_at FROM playground_redis.crm_orders WHERE order_id = $1`, id)
	var o order
	var updatedAt time.Time
	if err := row.Scan(&o.ID, &o.CustomerID, &o.UserID, &o.TotalCents, &o.Status, &o.Version, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return order{}, false, nil
		}
		return order{}, false, err
	}
	o.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return o, true, nil
}

type countingProjector struct {
	projectFn func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error)
	ghosts    int64
}

func (p *countingProjector) Project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	projection, err := p.projectFn(ctx, identity)
	if errors.Is(err, deltaflow.ErrProjectionNotFound) {
		atomic.AddInt64(&p.ghosts, 1)
	}
	return projection, err
}

func (p *countingProjector) ghostCount() int64 {
	return atomic.LoadInt64(&p.ghosts)
}

func rolesFor(faker *gofakeit.Faker, n int) []string {
	pool := []string{"admin", "sales", "support", "finance", "warehouse", "manager"}
	count := 1 + n%3
	out := make([]string, 0, count)
	seen := make(map[string]bool, count)
	for len(out) < count {
		role := pool[faker.Number(0, len(pool)-1)]
		if !seen[role] {
			seen[role] = true
			out = append(out, role)
		}
	}
	sort.Strings(out)
	return out
}

func statusFor(n int) string {
	statuses := []string{"draft", "accepted", "paid", "packed", "shipped", "cancelled"}
	return statuses[n%len(statuses)]
}

func sortedKeys[T interface{ key() string }](items []T) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.key())
	}
	sort.Strings(keys)
	return keys
}

func (u user) key() string     { return u.ID }
func (c customer) key() string { return c.ID }
func (o order) key() string    { return o.ID }

func parseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}

func queueTail(queue []string, limit int) string {
	if len(queue) == 0 {
		return ""
	}
	if len(queue) > limit {
		queue = queue[len(queue)-limit:]
	}
	return strings.Join(queue, ",")
}
