package contactsruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lemenendez/deltaflow/pkg/connectors/elasticsearch"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
)

type RegisterConfig struct {
	ProjectorName string
	TargetType    string
	SourceTable   string

	ESEndpointEnv string
	ESRefreshEnv  string

	HTTPClient *http.Client
}

func Register(registry *runtimepkg.Registry, cfg RegisterConfig) error {
	if registry == nil {
		return errors.New("contact runtime register: registry is required")
	}

	projectorName := cfg.ProjectorName
	if projectorName == "" {
		projectorName = "contact-projector"
	}
	targetType := cfg.TargetType
	if targetType == "" {
		targetType = "elasticsearch"
	}
	sourceTable := cfg.SourceTable
	if sourceTable == "" {
		sourceTable = "app_contacts"
	}
	esEndpointEnv := cfg.ESEndpointEnv
	if esEndpointEnv == "" {
		esEndpointEnv = "DELTAFLOW_ES_ENDPOINT"
	}
	esRefreshEnv := cfg.ESRefreshEnv
	if esRefreshEnv == "" {
		esRefreshEnv = "DELTAFLOW_ES_REFRESH"
	}

	registry.RegisterProjector(projectorName, func(_ context.Context, spec runtimepkg.PipelineSpec) (deltaflow.Projector, error) {
		if spec.StoreType != "postgres" {
			return nil, fmt.Errorf("contact projector requires postgres store, got %q", spec.StoreType)
		}
		if spec.StoreDB == nil {
			return nil, errors.New("contact projector requires shared store db handle")
		}
		return NewContactProjector(spec.StoreDB, sourceTable)
	})

	registry.RegisterApplier(targetType, func(_ context.Context, spec runtimepkg.PipelineSpec) (deltaflow.ProjectionApplier, error) {
		endpoint := os.Getenv(esEndpointEnv)
		if endpoint == "" {
			endpoint = "http://localhost:9200"
		}
		if spec.TargetIndex == "" {
			return nil, errors.New("contact runtime applier requires target index")
		}
		return elasticsearch.NewApplier(elasticsearch.ApplierConfig{
			Client:   cfg.HTTPClient,
			Endpoint: endpoint,
			Index:    spec.TargetIndex,
			Refresh:  os.Getenv(esRefreshEnv),
		})
	})

	return nil
}
