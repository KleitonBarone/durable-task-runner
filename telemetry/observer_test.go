package telemetry

import (
	"context"
	"strings"
	"testing"
	"time"

	durable "github.com/KleitonBarone/durable-task-runner"
	"go.opentelemetry.io/otel/attribute"
	metricSDK "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	traceSDK "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestAdapterRecordsMeasurementsWithoutIdentityLabels(t *testing.T) {
	reader := metricSDK.NewManualReader()
	metrics := metricSDK.NewMeterProvider(metricSDK.WithReader(reader))
	exporter := tracetest.NewInMemoryExporter()
	traces := traceSDK.NewTracerProvider(traceSDK.WithSyncer(exporter))
	t.Cleanup(func() { _ = metrics.Shutdown(context.Background()); _ = traces.Shutdown(context.Background()) })
	observer, err := New(metrics, traces)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1000, 0)
	observer.Observe(t.Context(), durable.Observation{Type: "activity.claimed", Outcome: "claimed", At: at, WorkflowID: "workflow-identity", ActivityID: "activity-identity", WorkerID: "worker-identity", Version: "v1", Kind: durable.ActivityForward, Duration: 150 * time.Millisecond, ReadyAge: 3 * time.Second})
	observer.Observe(t.Context(), durable.Observation{Type: "private-dynamic-event", Outcome: "private-outcome", Reason: "private-reason", Kind: "private-phase", At: at})
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	checkLabels := func(set attribute.Set) {
		t.Helper()
		for _, value := range set.ToSlice() {
			switch string(value.Key) {
			case "event", "phase", "outcome", "reason":
			default:
				t.Fatalf("identity metric label: %s", value.Key)
			}
			if strings.Contains(value.Value.AsString(), "private-") || strings.Contains(value.Value.AsString(), "identity") {
				t.Fatalf("unbounded label: %v", value)
			}
		}
	}
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			seen[measurement.Name] = true
			switch aggregate := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				var count int64
				for _, point := range aggregate.DataPoints {
					checkLabels(point.Attributes)
					count += point.Value
				}
				if count != 2 {
					t.Fatal(count)
				}
			case metricdata.Histogram[float64]:
				if len(aggregate.DataPoints) != 1 {
					t.Fatal(aggregate.DataPoints)
				}
				point := aggregate.DataPoints[0]
				checkLabels(point.Attributes)
				want := 0.15
				if measurement.Name == "durable.ready_age" {
					want = 3
				}
				if point.Count != 1 || point.Sum != want {
					t.Fatalf("measurement %s: %#v", measurement.Name, point)
				}
			default:
				t.Fatalf("unexpected aggregation: %T", aggregate)
			}
		}
	}
	if !seen["durable.events"] || !seen["durable.duration"] || !seen["durable.ready_age"] {
		t.Fatal(seen)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].Name != "durable.activity.claimed" || spans[0].EndTime.Sub(spans[0].StartTime) != 150*time.Millisecond || spans[1].Name != "durable.unknown" {
		t.Fatalf("spans: %#v", spans)
	}
	for _, span := range spans {
		for _, field := range span.Attributes {
			if strings.Contains(field.Value.AsString(), "private-") {
				t.Fatal("private diagnostics in span")
			}
		}
	}
}
