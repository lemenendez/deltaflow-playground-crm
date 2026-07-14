package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	redisclient "github.com/redis/go-redis/v9"
)

func TestNewCRMTargetUsesConnectorWhenRedisConfigured(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	server := newTestRedisServer("")

	prevAddr := redisAddress
	redisAddress = server.Addr()
	prevOptions := redisClientOptions
	redisClientOptions = server.Options
	defer func() {
		redisAddress = prevAddr
		redisClientOptions = prevOptions
	}()

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
	if !server.Saw("PING") {
		t.Fatalf("Redis server did not receive PING; commands=%v", server.Commands())
	}
}

func TestNewCRMTargetFailsWhenRedisPingFails(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	server := newTestRedisServer("redis disabled")

	prevAddr := redisAddress
	redisAddress = server.Addr()
	prevOptions := redisClientOptions
	redisClientOptions = server.Options
	defer func() {
		redisAddress = prevAddr
		redisClientOptions = prevOptions
	}()

	target, err := newCRMTarget(context.Background(), &crmStore{db: db}, nil, nil)
	if err == nil {
		t.Fatalf("err = nil, target = %T; want Redis ping error", target)
	}
	if !strings.Contains(err.Error(), "connect to redis at "+server.Addr()) {
		t.Fatalf("err = %q, want Redis address context", err)
	}
	if !server.Saw("PING") {
		t.Fatalf("Redis server did not receive PING; commands=%v", server.Commands())
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

func TestRedisTargetRejectsInvalidOrderPayloadBeforeMetrics(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := &redisCRMTarget{db: db, state: newRedisStateSnapshot(nil, nil)}
	op := deltaflow.ProjectionOperation{
		Type: deltaflow.ProjectionOpUpsert,
		Identity: deltaflow.ProjectionIdentity{
			Type: orderProjection,
			Key:  hostpkg.StringKey("id", "ord-001"),
		},
		Projection: &deltaflow.Projection{Payload: []byte(`{"order":{}}`), MediaType: "application/json"},
	}

	err = target.Apply(context.Background(), op)
	if err == nil {
		t.Fatal("err = nil, want missing customer_id error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	_, _, keys, upserts, deletes, failures, snapErr := target.snapshot(context.Background())
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	if len(keys) != 0 || upserts != 0 || deletes != 0 || failures != 0 {
		t.Fatalf("unexpected side effects keys=%d upserts=%d deletes=%d failures=%d", len(keys), upserts, deletes, failures)
	}
}

func TestRedisTargetIgnoresNonOrderProjection(t *testing.T) {
	target := &redisCRMTarget{state: newRedisStateSnapshot(nil, nil)}
	op := deltaflow.ProjectionOperation{
		Type: deltaflow.ProjectionOpUpsert,
		Identity: deltaflow.ProjectionIdentity{
			Type: userProjection,
			Key:  hostpkg.StringKey("user_id", "usr-001"),
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

type testRedisServer struct {
	pingError string
	mu        sync.Mutex
	commands  []string
}

func newTestRedisServer(pingError string) *testRedisServer {
	return &testRedisServer{pingError: pingError}
}

func (s *testRedisServer) Addr() string {
	return "redis.test:6379"
}

func (s *testRedisServer) Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.commands))
	copy(out, s.commands)
	return out
}

func (s *testRedisServer) Saw(command string) bool {
	for _, seen := range s.Commands() {
		if seen == command {
			return true
		}
	}
	return false
}

func (s *testRedisServer) Options(addr string) *redisclient.Options {
	return &redisclient.Options{
		Addr: addr,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			go s.handle(serverConn)
			return clientConn, nil
		},
	}
}

func (s *testRedisServer) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readTestRedisCommand(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		name := strings.ToUpper(args[0])
		s.record(name)

		switch name {
		case "HELLO":
			_, _ = conn.Write([]byte("-ERR unknown command 'hello'\r\n"))
		case "PING":
			if s.pingError != "" {
				_, _ = fmt.Fprintf(conn, "-ERR %s\r\n", s.pingError)
				continue
			}
			_, _ = conn.Write([]byte("+PONG\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

func (s *testRedisServer) record(command string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, command)
}

func readTestRedisCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array header, got %q", line)
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimRight(header, "\r\n")
		if !strings.HasPrefix(header, "$") {
			return nil, fmt.Errorf("expected RESP bulk header, got %q", header)
		}
		length, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, err
		}
		arg := make([]byte, length)
		if _, err := io.ReadFull(reader, arg); err != nil {
			return nil, err
		}
		if _, err := reader.Discard(2); err != nil {
			return nil, err
		}
		args = append(args, string(arg))
	}
	return args, nil
}
