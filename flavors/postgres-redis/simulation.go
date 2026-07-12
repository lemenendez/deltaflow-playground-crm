package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	pgstore "github.com/lemenendez/deltaflow/pkg/connectors/postgres"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type mutation struct {
	Seq        int
	ActorID    int
	Entity     string
	EntityID   string
	Kind       string
	Value      any
	At         time.Time
	Projection deltaflow.ProjectionType
}

type scenario struct {
	source *crmStore
	target crmTarget
	events []mutation
}

type writerRunResult struct {
	Enqueued int
	Planned  int
}

type writerAck struct {
	event mutation
	err   error
}

func buildScenario(ctx context.Context, db *sql.DB) (*scenario, error) {
	faker := gofakeit.New(seed)
	source := newCRMStore(db)
	if err := source.ensureSchema(ctx); err != nil {
		return nil, err
	}

	users := make([]user, 0, userCount)
	for i := 1; i <= userCount; i++ {
		id := fmt.Sprintf("usr-%03d", i)
		users = append(users, user{
			ID:        id,
			Name:      faker.Name(),
			Roles:     rolesFor(faker, i),
			Version:   1,
			UpdatedAt: fixedTime(0).Format(time.RFC3339),
		})
	}

	customers := make([]customer, 0, customerCount)
	for i := 1; i <= customerCount; i++ {
		id := fmt.Sprintf("cus-%03d", i)
		customers = append(customers, customer{
			ID:        id,
			Name:      faker.Name(),
			Email:     faker.Email(),
			Phone:     faker.Phone(),
			VIP:       faker.Number(0, 5) == 0,
			Version:   1,
			UpdatedAt: fixedTime(0).Format(time.RFC3339),
		})
	}

	userIDs := sortedKeys(users)
	customerIDs := sortedKeys(customers)
	orders := make([]order, 0, orderCount+1)
	for i := 1; i <= orderCount; i++ {
		id := fmt.Sprintf("ord-%03d", i)
		orders = append(orders, order{
			ID:         id,
			CustomerID: customerIDs[faker.Number(0, len(customerIDs)-1)],
			UserID:     userIDs[faker.Number(0, len(userIDs)-1)],
			TotalCents: faker.Number(1200, 98000),
			Status:     statusFor(i),
			Version:    1,
			UpdatedAt:  fixedTime(0).Format(time.RFC3339),
		})
	}
	deadOrderID := "ord-dead-001"
	orders = append(orders, order{
		ID:         deadOrderID,
		CustomerID: customerIDs[0],
		UserID:     userIDs[0],
		TotalCents: 9900,
		Status:     "accepted",
		Version:    1,
		UpdatedAt:  fixedTime(0).Format(time.RFC3339),
	})

	if err := source.reset(ctx, users, customers, orders); err != nil {
		return nil, err
	}

	orderIDs := sortedKeys(orders)
	retryOrderID := orderIDs[0]
	target, err := newCRMTarget(
		ctx,
		source,
		map[string]bool{string(orderProjection) + "/" + retryOrderID: true},
		map[string]bool{string(orderProjection) + "/" + deadOrderID: true},
	)
	if err != nil {
		return nil, err
	}

	events := make([]mutation, 0, mutationCount+3)
	entityCycle := []string{"user", "customer", "order", "customer", "order"}
	for seq := 1; seq <= mutationCount; seq++ {
		entity := entityCycle[(seq+faker.Number(0, len(entityCycle)-1))%len(entityCycle)]
		m := mutation{
			Seq:     seq,
			ActorID: seq % writerCount,
			Entity:  entity,
			At:      fixedTime(seq),
		}
		switch entity {
		case "user":
			m.EntityID = userIDs[faker.Number(0, len(userIDs)-1)]
			m.Projection = userProjection
			if seq%2 == 0 {
				m.Kind = "roles"
				m.Value = rolesFor(faker, seq)
			} else {
				m.Kind = "name"
				m.Value = faker.Name()
			}
		case "customer":
			m.EntityID = customerIDs[faker.Number(0, len(customerIDs)-1)]
			m.Projection = custProjection
			switch seq % 3 {
			case 0:
				m.Kind = "email"
				m.Value = faker.Email()
			case 1:
				m.Kind = "phone"
				m.Value = faker.Phone()
			default:
				m.Kind = "vip"
				m.Value = faker.Number(0, 1) == 1
			}
		case "order":
			m.EntityID = orderIDs[faker.Number(0, len(orderIDs)-2)]
			m.Projection = orderProjection
			if seq%2 == 0 {
				m.Kind = "status"
				m.Value = statusFor(seq)
			} else {
				m.Kind = "total"
				m.Value = faker.Number(1200, 120000)
			}
		}
		events = append(events, m)
	}
	events = append(events, mutation{
		Seq:        mutationCount + 1,
		ActorID:    2,
		Entity:     "order",
		EntityID:   retryOrderID,
		Kind:       "total",
		Value:      faker.Number(1200, 120000),
		Projection: orderProjection,
		At:         fixedTime(mutationCount + 1),
	})
	events = append(events, mutation{
		Seq:        mutationCount + 2,
		ActorID:    3,
		Entity:     "order",
		EntityID:   deadOrderID,
		Kind:       "status",
		Value:      "accepted",
		Projection: orderProjection,
		At:         fixedTime(mutationCount + 2),
	})
	events = append(events, mutation{
		Seq:        mutationCount + 3,
		ActorID:    0,
		Entity:     "order",
		EntityID:   "ord-ghost-001",
		Kind:       "ghost",
		Projection: orderProjection,
		At:         fixedTime(mutationCount + 3),
	})

	return &scenario{source: source, target: target, events: events}, nil
}

func runWriters(ctx context.Context, db *sql.DB, deltaStore *pgstore.DeltaStore, source *crmStore, events []mutation) (writerRunResult, error) {
	result := writerRunResult{Planned: len(events)}
	actorEvents := make([][]mutation, writerCount)
	for _, event := range events {
		if event.ActorID < 0 || event.ActorID >= len(actorEvents) {
			return result, fmt.Errorf("invalid actor id %d for WRITER_COUNT=%d", event.ActorID, writerCount)
		}
		actorEvents[event.ActorID] = append(actorEvents[event.ActorID], event)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ackCh := make(chan writerAck)
	var wg sync.WaitGroup
	for _, eventsForActor := range actorEvents {
		wg.Add(1)
		go func(eventsForActor []mutation) {
			defer wg.Done()
			for _, event := range eventsForActor {
				if runCtx.Err() != nil {
					return
				}
				err := applyAndEnqueue(runCtx, db, deltaStore, source, event)
				ackCh <- writerAck{event: event, err: err}
				if err != nil {
					cancel()
					return
				}
			}
		}(eventsForActor)
	}

	go func() {
		wg.Wait()
		close(ackCh)
	}()

	var firstFailure writerAck
	for ack := range ackCh {
		if ack.err == nil {
			result.Enqueued++
			continue
		}
		if firstFailure.err == nil {
			firstFailure = ack
			cancel()
		}
	}

	if firstFailure.err != nil {
		return result, fmt.Errorf(
			"writer stage failed after %d/%d committed deltas (actor %s sequence %d): %w",
			result.Enqueued,
			result.Planned,
			actorName(firstFailure.event.ActorID),
			firstFailure.event.Seq,
			firstFailure.err,
		)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("writer stage stopped after %d/%d committed deltas: %w", result.Enqueued, result.Planned, err)
	}
	return result, nil
}

func applyAndEnqueue(ctx context.Context, db *sql.DB, deltaStore *pgstore.DeltaStore, source *crmStore, event mutation) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if event.Kind != "ghost" {
		if err := source.applyMutation(ctx, tx, event); err != nil {
			return err
		}
	}

	origin := deltaflow.OriginOperationUpdated
	if event.Entity == "order" && event.Kind == "status" && event.Value == "accepted" {
		origin = deltaflow.OriginOperationInserted
	}
	_, err = deltaStore.EnqueueInTx(ctx, tx, hostpkg.NewDelta(
		syncID,
		event.Projection,
		hostpkg.StringKey("id", event.EntityID),
		origin,
		event.At,
		map[string]any{
			"actor":    actorName(event.ActorID),
			"entity":   event.Entity,
			"change":   event.Kind,
			"sequence": event.Seq,
		},
	))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func fixedTime(step int) time.Time {
	return time.Date(2026, time.June, 4, 16, 0, 0, 0, time.UTC).Add(time.Duration(step) * time.Second)
}
