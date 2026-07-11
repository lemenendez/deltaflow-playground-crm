package contactsruntime

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	runtimepkg "github.com/lemenendez/deltaflow/pkg/runtime"
)

func TestRegisterProjectorRequiresSharedStoreDBHandle(t *testing.T) {
	registry := runtimepkg.NewRegistry()
	if err := Register(registry, RegisterConfig{}); err != nil {
		t.Fatalf("Register error: %v", err)
	}

	_, err := registry.ResolvePipeline(context.Background(), runtimepkg.PipelineSpec{
		Name:          "contacts",
		ProjectorName: "contact-projector",
		TargetType:    "elasticsearch",
		TargetIndex:   "contacts",
		StoreType:     "postgres",
		StoreDSN:      "postgres://unused",
	})
	if err == nil {
		t.Fatal("ResolvePipeline error = nil")
	}
	if !strings.Contains(err.Error(), "requires shared store db handle") {
		t.Fatalf("error = %v", err)
	}
}

func TestRegisterProjectorUsesProvidedSharedStoreDBHandle(t *testing.T) {
	registry := runtimepkg.NewRegistry()
	if err := Register(registry, RegisterConfig{}); err != nil {
		t.Fatalf("Register error: %v", err)
	}

	_, err := registry.ResolvePipeline(context.Background(), runtimepkg.PipelineSpec{
		Name:          "contacts",
		ProjectorName: "contact-projector",
		TargetType:    "elasticsearch",
		TargetIndex:   "contacts",
		StoreType:     "postgres",
		StoreDSN:      "postgres://unused",
		StoreDB:       &sql.DB{},
	})
	if err != nil {
		t.Fatalf("ResolvePipeline error: %v", err)
	}
}
