package main

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestNewCRMTargetUsesConnectorWhenRedisConfigured(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prevAddr := redisAddress
	redisAddress = "127.0.0.1:6379"
	defer func() { redisAddress = prevAddr }()

	target, err := newCRMTarget(context.Background(), &crmStore{db: db}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	redisTarget, ok := target.(*redisCRMTarget)
	if !ok {
		t.Fatalf("target type = %T, want *redisCRMTarget", target)
	}
	if redisTarget.applier == nil {
		t.Fatal("applier = nil, want DeltaFlow redis connector applier")
	}
}

func TestRedisTargetRetryThenSuccessRefreshesMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := &redisCRMTarget{
		db: db,
		state: newRedisStateSnapshot(
			map[string]bool{string(orderProjection) + "/ord-001": true},
			nil,
		),
	}

	op := orderUpsertOperation(t, "ord-001", "cus-001")

	err = target.Apply(context.Background(), op)
	if err == nil {
		t.Fatal("err = nil, want retry error")
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COUNT(*)::bigint,
       COALESCE(SUM(total_cents), 0)::bigint,
       COALESCE(AVG(total_cents::numeric), 0)::double precision
FROM playground_redis.crm_orders
WHERE customer_id = $1`)).
		WithArgs("cus-001").
		WillReturnRows(sqlmock.NewRows([]string{"count", "total", "avg"}).AddRow(3, 15000, 5000.0))

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COUNT(*)::bigint,
       COALESCE(SUM(total_cents), 0)::bigint,
       COALESCE(AVG(total_cents::numeric), 0)::double precision
FROM playground_redis.crm_orders`)).
		WillReturnRows(sqlmock.NewRows([]string{"count", "total", "avg"}).AddRow(12, 75000, 6250.0))

	err = target.Apply(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	_, _, keys, upserts, deletes, failures, err := target.snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if upserts != 1 {
		t.Fatalf("upserts = %d, want 1", upserts)
	}
	if deletes != 0 {
		t.Fatalf("deletes = %d, want 0", deletes)
	}
	if failures != 1 {
		t.Fatalf("failures = %d, want 1", failures)
	}

	customerKey := "metrics:customer:cus-001:orders"
	if _, ok := keys[customerKey]; !ok {
		t.Fatalf("missing key %q", customerKey)
	}
	if _, ok := keys[redisGlobalKey]; !ok {
		t.Fatalf("missing key %q", redisGlobalKey)
	}
}

func TestRedisTargetDeadLetterUpsert(t *testing.T) {
	target := &redisCRMTarget{
		state: newRedisStateSnapshot(
			nil,
			map[string]bool{string(orderProjection) + "/ord-dead-001": true},
		),
	}

	err := target.Apply(context.Background(), orderUpsertOperation(t, "ord-dead-001", "cus-001"))
	if err == nil {
		t.Fatal("err = nil, want dead-letter rejection")
	}

	_, _, _, upserts, deletes, failures, snapErr := target.snapshot(context.Background())
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	if upserts != 0 || deletes != 0 {
		t.Fatalf("unexpected counters upserts=%d deletes=%d", upserts, deletes)
	}
	if failures != 1 {
		t.Fatalf("failures = %d, want 1", failures)
	}
}

func TestRedisTargetIgnoresNonOrderProjection(t *testing.T) {
	target := &redisCRMTarget{state: newRedisStateSnapshot(nil, nil)}
	op := deltaflow.ProjectionOperation{
		Type: deltaflow.ProjectionOpUpsert,
		Identity: deltaflow.ProjectionIdentity{
			Type: userProjection,
			Key:  hostpkg.StringKey("id", "usr-001"),
		},
		Projection: &deltaflow.Projection{Payload: []byte(`{"id":"usr-001"}`), MediaType: "application/json"},
	}

	if err := target.Apply(context.Background(), op); err != nil {
		t.Fatal(err)
	}

	_, _, _, upserts, deletes, failures, err := target.snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if upserts != 0 || deletes != 0 || failures != 0 {
		t.Fatalf("unexpected counters upserts=%d deletes=%d failures=%d", upserts, deletes, failures)
	}
}

func TestCustomerIDFromOrderProjection(t *testing.T) {
	payload, err := json.Marshal(map[string]any{"order": map[string]any{"customer_id": "cus-101"}})
	if err != nil {
		t.Fatal(err)
	}

	id, err := customerIDFromOrderProjection(payload)
	if err != nil {
		t.Fatal(err)
	}
	if id != "cus-101" {
		t.Fatalf("customer id = %q, want %q", id, "cus-101")
	}
}

func orderUpsertOperation(t *testing.T, orderID string, customerID string) deltaflow.ProjectionOperation {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"order": map[string]any{
			"id":          orderID,
			"customer_id": customerID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return deltaflow.ProjectionOperation{
		Type: deltaflow.ProjectionOpUpsert,
		Identity: deltaflow.ProjectionIdentity{
			Type: orderProjection,
			Key:  hostpkg.StringKey("id", orderID),
		},
		Projection: &deltaflow.Projection{Payload: payload, MediaType: "application/json"},
	}
}
