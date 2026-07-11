package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	es "github.com/lemenendez/deltaflow/pkg/connectors/elasticsearch"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

const elasticsearchIndex = "deltaflow_crm"
const elasticsearchHTTPTimeout = 10 * time.Second

func newCRMTarget(ctx context.Context, failOnce map[string]bool, deadLetters map[string]bool) (crmTarget, error) {
	if elasticsearchEndpoint == "" {
		return newCRMTargetSimulator(failOnce, deadLetters), nil
	}
	target, err := newElasticsearchCRMTarget(elasticsearchEndpoint, elasticsearchIndex, failOnce, deadLetters)
	if err != nil {
		return nil, err
	}
	if err := target.reset(ctx); err != nil {
		return nil, err
	}
	return target, nil
}

type elasticsearchCRMTarget struct {
	client   *http.Client
	endpoint string
	index    string
	applier  *es.Applier

	mu              sync.Mutex
	searchQueue     []string
	orderQueue      []string
	failOnce        map[string]bool
	deadLetters     map[string]bool
	upserts         int
	deletes         int
	failures        int
}

func newElasticsearchCRMTarget(endpoint, index string, failOnce map[string]bool, deadLetters map[string]bool) (*elasticsearchCRMTarget, error) {
	client := &http.Client{Timeout: elasticsearchHTTPTimeout}
	applier, err := es.NewApplier(es.ApplierConfig{
		Client:   client,
		Endpoint: endpoint,
		Index:    index,
		DocumentID: func(identity deltaflow.ProjectionIdentity) (string, error) {
			id, err := hostpkg.StringFromKey(identity.Key, "id")
			if err != nil {
				return "", err
			}
			return string(identity.Type) + "/" + id, nil
		},
		Refresh: "wait_for",
	})
	if err != nil {
		return nil, err
	}
	return &elasticsearchCRMTarget{
		client:      client,
		endpoint:    strings.TrimRight(endpoint, "/"),
		index:       index,
		applier:     applier,
		failOnce:    copyBoolMap(failOnce),
		deadLetters: copyBoolMap(deadLetters),
	}, nil
}

func (t *elasticsearchCRMTarget) reset(ctx context.Context) error {
	deleteReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, t.indexURL(), nil)
	if err != nil {
		return err
	}
	deleteResp, err := t.client.Do(deleteReq)
	if err != nil {
		return err
	}
	if err := closeResponse(deleteResp); err != nil && deleteResp.StatusCode != http.StatusNotFound {
		return err
	}

	createReq, err := http.NewRequestWithContext(ctx, http.MethodPut, t.indexURL(), strings.NewReader(crmIndexMapping))
	if err != nil {
		return err
	}
	createReq.Header.Set("Content-Type", "application/json")
	createResp, err := t.client.Do(createReq)
	if err != nil {
		return err
	}
	if err := closeResponse(createResp); err != nil {
		return err
	}

	return t.seedGhost(ctx)
}

func (t *elasticsearchCRMTarget) seedGhost(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, t.documentURL(string(custProjection)+"/cus-ghost-001"), bytes.NewReader([]byte(`{"customer":{"id":"cus-ghost-001"},"stale":true}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	return closeResponse(resp)
}

func (t *elasticsearchCRMTarget) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	id, err := hostpkg.StringFromKey(op.Identity.Key, "id")
	if err != nil {
		return err
	}
	queueKey := fmt.Sprintf("%s/%s", op.Identity.Type, id)

	t.mu.Lock()
	if op.Type == deltaflow.ProjectionOpUpsert {
		if t.deadLetters[queueKey] {
			t.failures++
			t.mu.Unlock()
			return fmt.Errorf("crm target rejected %s: invalid downstream payload", queueKey)
		}
		if t.failOnce[queueKey] {
			delete(t.failOnce, queueKey)
			t.failures++
			t.mu.Unlock()
			return fmt.Errorf("elasticsearch temporary timeout for %s", queueKey)
		}
	}
	t.mu.Unlock()

	if err := t.applier.Apply(ctx, op); err != nil {
		t.mu.Lock()
		t.failures++
		t.mu.Unlock()
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		t.applyUpsert(op, id)
		t.upserts++
	case deltaflow.ProjectionOpDelete:
		t.searchQueue = append(t.searchQueue, "delete:"+string(op.Identity.Type)+":"+id)
		t.deletes++
	}
	return nil
}

func (t *elasticsearchCRMTarget) applyUpsert(op deltaflow.ProjectionOperation, id string) {
	switch op.Identity.Type {
	case userProjection:
		t.searchQueue = append(t.searchQueue, "upsert:user:"+id)
	case custProjection:
		t.searchQueue = append(t.searchQueue, "upsert:customer:"+id)
	case orderProjection:
		t.orderQueue = append(t.orderQueue, "publish:order:"+id)
		t.searchQueue = append(t.searchQueue, "upsert:order:"+id)
	}
}

func (t *elasticsearchCRMTarget) snapshot(ctx context.Context) ([]string, []string, map[string][]byte, int, int, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.indexURL()+"/_search?size=10000", nil)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil, nil, 0, 0, 0, fmt.Errorf("elasticsearch snapshot failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	searchDocs := make(map[string][]byte, len(payload.Hits.Hits))
	for _, hit := range payload.Hits.Hits {
		searchDocs[hit.ID] = append([]byte(nil), hit.Source...)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	searchQueue := append([]string(nil), t.searchQueue...)
	orderQueue := append([]string(nil), t.orderQueue...)
	return searchQueue, orderQueue, searchDocs, t.upserts, t.deletes, t.failures, nil
}

func (t *elasticsearchCRMTarget) indexURL() string {
	return t.endpoint + "/" + url.PathEscape(t.index)
}

func (t *elasticsearchCRMTarget) documentURL(documentID string) string {
	return t.indexURL() + "/_doc/" + url.PathEscape(documentID) + "?refresh=wait_for"
}

func closeResponse(resp *http.Response) error {
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = resp.Status
	}
	return errors.New(message)
}

func copyBoolMap(in map[string]bool) map[string]bool {
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

const crmIndexMapping = `{
  "mappings": {
    "properties": {
      "user": { "type": "object", "enabled": true },
      "customer": { "type": "object", "enabled": true },
      "order": { "type": "object", "enabled": true },
      "updated_at": { "type": "date" }
    }
  }
}`
