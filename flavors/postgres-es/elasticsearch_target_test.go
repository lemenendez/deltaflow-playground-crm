package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	hostpkg "github.com/lemenendez/deltaflow-playground-crm/internal/scenario/host"
	es "github.com/lemenendez/deltaflow/pkg/connectors/elasticsearch"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

func TestNewElasticsearchCRMTargetUsesTimeoutClient(t *testing.T) {
	target, err := newElasticsearchCRMTarget("http://localhost:9200", "crm", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if target.client.Timeout != 10*time.Second {
		t.Fatalf("Timeout = %s, want 10s", target.client.Timeout)
	}
}

func TestElasticsearchCRMTargetRetryErrorNamesElasticsearch(t *testing.T) {
	target, err := newElasticsearchCRMTarget(
		"http://localhost:9200",
		"crm",
		map[string]bool{"CRMCustomerView/cus-001": true},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	err = target.Apply(context.Background(), customerUpsertOperation(t, "cus-001"))
	if err == nil {
		t.Fatal("err = nil, want retry error")
	}
	var responseErr *es.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("error = %T, want *elasticsearch.ResponseError", err)
	}
	if responseErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("StatusCode = %d, want %d", responseErr.StatusCode, http.StatusTooManyRequests)
	}
	if !responseErr.Retryable {
		t.Fatal("Retryable = false, want true")
	}
	if !strings.Contains(responseErr.Body, "elasticsearch temporary timeout") {
		t.Fatalf("body = %q, want temporary timeout text", responseErr.Body)
	}
}

func TestCloseResponseDrainsSuccessfulBody(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(`{"acknowledged":true}`)}
	err := closeResponse(&http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       body,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !body.readEOF {
		t.Fatal("successful response body was not drained")
	}
	if !body.closed {
		t.Fatal("successful response body was not closed")
	}
}

func TestCloseResponseUsesStatusWhenBodyIsEmpty(t *testing.T) {
	err := closeResponse(&http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "500 Internal Server Error",
		Body:       io.NopCloser(strings.NewReader("")),
	})
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if err.Error() != "500 Internal Server Error" {
		t.Fatalf("error = %q, want status text", err.Error())
	}
}

func TestCloseResponseUsesBodyWhenPresent(t *testing.T) {
	err := closeResponse(&http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Body:       io.NopCloser(strings.NewReader("  mapping rejected  ")),
	})
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if err.Error() != "mapping rejected" {
		t.Fatalf("error = %q, want body text", err.Error())
	}
}

func TestCloseResponseDrainsErrorBody(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(strings.Repeat("x", 5000))}
	err := closeResponse(&http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Body:       body,
	})
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if !body.readEOF {
		t.Fatal("error response body was not drained")
	}
	if !body.closed {
		t.Fatal("error response body was not closed")
	}
}

func TestElasticsearchCRMTargetSnapshotDrainsErrorBody(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(strings.Repeat("x", 5000))}
	target := &elasticsearchCRMTarget{
		client: &http.Client{
			Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Status:     "500 Internal Server Error",
					Body:       body,
				}, nil
			}),
		},
		endpoint: "http://localhost:9200",
		index:    "crm",
	}

	_, _, _, _, _, _, err := target.snapshot(context.Background())
	if err == nil {
		t.Fatal("err = nil, want error")
	}
	if !body.readEOF {
		t.Fatal("error response body was not drained")
	}
	if !body.closed {
		t.Fatal("error response body was not closed")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type trackingReadCloser struct {
	reader  *strings.Reader
	readEOF bool
	closed  bool
}

func (b *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if err == io.EOF {
		b.readEOF = true
	}
	return n, err
}

func (b *trackingReadCloser) Close() error {
	b.closed = true
	return nil
}

func customerUpsertOperation(t *testing.T, id string) deltaflow.ProjectionOperation {
	t.Helper()
	key := hostpkg.StringKey("id", id)
	return deltaflow.ProjectionOperation{
		Type:     deltaflow.ProjectionOpUpsert,
		Identity: deltaflow.ProjectionIdentity{Type: custProjection, Key: key},
		Projection: &deltaflow.Projection{
			Payload:   []byte(fmt.Sprintf(`{"customer":{"id":%q}}`, id)),
			MediaType: "application/json",
		},
	}
}
