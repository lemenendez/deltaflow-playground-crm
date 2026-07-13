package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	redisconnector "github.com/lemenendez/deltaflow/pkg/connectors/redis"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	redisclient "github.com/redis/go-redis/v9"
)

const (
	redisWriteTimeout    = 2 * time.Second
	redisMetricView      = deltaflow.ProjectionType("RedisMetricView")
	redisMetricKeyField  = "metric_key"
	redisCustomerKeyBase = "metrics:customer:"
	redisGlobalKey       = "metrics:global:orders"
	redisMetricTTL       = 0
)

type orderMetrics struct {
	OrderCount        int     `json:"order_count"`
	AverageTotalCents float64 `json:"avg_total_cents"`
	TotalCents        int64   `json:"total_cents"`
}

type redisStateSnapshot struct {
	mu          sync.Mutex
	ops         []string
	metricOps   []string
	keys        map[string][]byte
	failOnce    map[string]bool
	deadLetters map[string]bool
	upserts     int
	deletes     int
	failures    int
}

func newRedisStateSnapshot(failOnce map[string]bool, deadLetters map[string]bool) *redisStateSnapshot {
	return &redisStateSnapshot{
		keys:        make(map[string][]byte),
		failOnce:    copyBoolMap(failOnce),
		deadLetters: copyBoolMap(deadLetters),
	}
}

type redisCRMTarget struct {
	db      *sql.DB
	applier *redisconnector.Applier
	state   *redisStateSnapshot
}

var redisClientOptions = func(addr string) *redisclient.Options {
	return &redisclient.Options{Addr: addr}
}

func newCRMTarget(ctx context.Context, source *crmStore, failOnce map[string]bool, deadLetters map[string]bool) (crmTarget, error) {
	if source == nil || source.db == nil {
		return nil, errors.New("redis target requires source db")
	}

	state := newRedisStateSnapshot(failOnce, deadLetters)
	if redisAddress == "" {
		return &redisCRMTarget{db: source.db, state: state}, nil
	}

	client := redisclient.NewClient(redisClientOptions(redisAddress))
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to redis at %s: %w", redisAddress, err)
	}
	keyFunc := func(identity deltaflow.ProjectionIdentity) (string, error) {
		return hostpkg.StringFromKey(identity.Key, redisMetricKeyField)
	}
	applier, err := redisconnector.NewApplier(redisconnector.ApplierConfig{
		Client:  client,
		KeyFunc: keyFunc,
		TTL:     redisMetricTTL,
	})
	if err != nil {
		return nil, err
	}

	return &redisCRMTarget{db: source.db, applier: applier, state: state}, nil
}

func (t *redisCRMTarget) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	id, err := hostpkg.StringFromKey(op.Identity.Key, "id")
	if err != nil {
		return err
	}
	queueKey := fmt.Sprintf("%s/%s", op.Identity.Type, id)

	if err := t.applySimulationGuards(op, queueKey); err != nil {
		return err
	}

	if op.Identity.Type != orderProjection {
		return nil
	}

	entries, err := t.metricEntriesForOperation(ctx, op)
	if err != nil {
		return err
	}
	if err := t.writeMetrics(ctx, entries); err != nil {
		return err
	}

	t.state.mu.Lock()
	defer t.state.mu.Unlock()
	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		customerID, extractErr := customerIDFromOrderProjection(op.Projection.Payload)
		if extractErr != nil {
			return extractErr
		}
		t.state.ops = append(t.state.ops, "upsert:order:"+id)
		t.state.metricOps = append(t.state.metricOps, "refresh:customer:"+customerID)
		t.state.upserts++
	case deltaflow.ProjectionOpDelete:
		t.state.ops = append(t.state.ops, "delete:order:"+id)
		t.state.metricOps = append(t.state.metricOps, "refresh:global")
		t.state.deletes++
	}
	return nil
}

func (t *redisCRMTarget) applySimulationGuards(op deltaflow.ProjectionOperation, queueKey string) error {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()

	if op.Type != deltaflow.ProjectionOpUpsert {
		return nil
	}
	if op.Projection == nil {
		return errors.New("upsert operation requires projection")
	}
	if t.state.deadLetters[queueKey] {
		t.state.failures++
		return fmt.Errorf("redis target rejected %s: invalid downstream payload", queueKey)
	}
	if t.state.failOnce[queueKey] {
		delete(t.state.failOnce, queueKey)
		t.state.failures++
		return fmt.Errorf("redis temporary timeout for %s", queueKey)
	}
	return nil
}

func (t *redisCRMTarget) metricEntriesForOperation(ctx context.Context, op deltaflow.ProjectionOperation) (map[string]orderMetrics, error) {
	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		customerID, err := customerIDFromOrderProjection(op.Projection.Payload)
		if err != nil {
			return nil, err
		}
		customerMetrics, err := t.loadMetricsByCustomer(ctx, customerID)
		if err != nil {
			return nil, err
		}
		globalMetrics, err := t.loadGlobalMetrics(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]orderMetrics{
			redisCustomerKeyBase + customerID + ":orders": customerMetrics,
			redisGlobalKey: globalMetrics,
		}, nil
	case deltaflow.ProjectionOpDelete:
		globalMetrics, err := t.loadGlobalMetrics(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]orderMetrics{redisGlobalKey: globalMetrics}, nil
	default:
		return nil, fmt.Errorf("unsupported operation %q", op.Type)
	}
}

func (t *redisCRMTarget) writeMetrics(ctx context.Context, entries map[string]orderMetrics) error {
	serialized := make(map[string][]byte, len(entries))
	for key, value := range entries {
		raw, err := json.Marshal(value)
		if err != nil {
			return err
		}
		serialized[key] = raw
	}

	if t.applier != nil {
		writeCtx, cancel := context.WithTimeout(ctx, redisWriteTimeout)
		defer cancel()
		for key, raw := range serialized {
			if err := t.applier.Apply(writeCtx, deltaflow.ProjectionOperation{
				Type: deltaflow.ProjectionOpUpsert,
				Identity: deltaflow.ProjectionIdentity{
					Type: redisMetricView,
					Key:  hostpkg.StringKey(redisMetricKeyField, key),
				},
				Projection: &deltaflow.Projection{Payload: raw, MediaType: "application/json"},
			}); err != nil {
				return err
			}
		}
	}

	t.state.mu.Lock()
	for key, raw := range serialized {
		t.state.keys[key] = raw
	}
	t.state.mu.Unlock()
	return nil
}

func (t *redisCRMTarget) snapshot(_ context.Context) ([]string, []string, map[string][]byte, int, int, int, error) {
	t.state.mu.Lock()
	defer t.state.mu.Unlock()

	keys := make(map[string][]byte, len(t.state.keys))
	for k, v := range t.state.keys {
		keys[k] = append([]byte(nil), v...)
	}
	return append([]string(nil), t.state.ops...), append([]string(nil), t.state.metricOps...), keys, t.state.upserts, t.state.deletes, t.state.failures, nil
}

func (t *redisCRMTarget) loadMetricsByCustomer(ctx context.Context, customerID string) (orderMetrics, error) {
	row := t.db.QueryRowContext(ctx, `
SELECT COUNT(*)::bigint,
       COALESCE(SUM(total_cents), 0)::bigint,
       COALESCE(AVG(total_cents::numeric), 0)::double precision
FROM playground_redis.crm_orders
WHERE customer_id = $1`, customerID)

	var count int64
	var total int64
	var average float64
	if err := row.Scan(&count, &total, &average); err != nil {
		return orderMetrics{}, err
	}
	return orderMetrics{OrderCount: int(count), TotalCents: total, AverageTotalCents: average}, nil
}

func (t *redisCRMTarget) loadGlobalMetrics(ctx context.Context) (orderMetrics, error) {
	row := t.db.QueryRowContext(ctx, `
SELECT COUNT(*)::bigint,
       COALESCE(SUM(total_cents), 0)::bigint,
       COALESCE(AVG(total_cents::numeric), 0)::double precision
FROM playground_redis.crm_orders`)

	var count int64
	var total int64
	var average float64
	if err := row.Scan(&count, &total, &average); err != nil {
		return orderMetrics{}, err
	}
	return orderMetrics{OrderCount: int(count), TotalCents: total, AverageTotalCents: average}, nil
}

func customerIDFromOrderProjection(payload []byte) (string, error) {
	var body struct {
		Order order `json:"order"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return "", fmt.Errorf("decode order projection payload: %w", err)
	}
	if body.Order.CustomerID == "" {
		return "", errors.New("order projection payload missing customer_id")
	}
	return body.Order.CustomerID, nil
}

func copyBoolMap(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
