# Metrics Crate

A lightweight, async-friendly metrics library for Rust applications. It supports counters, gauges, and histograms with thread-safe storage using sharding. Metrics can be exposed in Prometheus format for easy integration with monitoring systems.

## Features

- **Counters**: Monotonically increasing values for counting events.
- **Gauges**: Values that can go up or down, useful for measurements like memory usage.
- **Histograms**: Track distributions of values (e.g., request durations) with configurable buckets.
- **Thread-Safe**: Uses sharded `RwLock<HashMap>` for concurrent access without blocking.
- **Async Updates**: Changes are sent via a bounded/unbounded channel and processed asynchronously.
- **Freshness Management**: Automatically removes stale metrics based on configurable durations.
- **Prometheus Exposition**: Render metrics in Prometheus text format.
- **Macros**: Convenient `counter!`, `gauge!`, and `histogram!` macros for quick metric creation with labels.
- **Configurable**: Customize shards, buffer sizes, freshness intervals, and histogram buckets via `DaveBuilder`.

### Initialization

Use `DaveBuilder` to configure and install the global recorder:

```rust
use metrics::{DaveBuilder, Duration};
use std::time::Duration;

DaveBuilder::default()
    .num_shards(std::num::NonZeroUsize::new(32).unwrap())  // Optional: increase shards for high concurrency
    .buffer_size(1024)  // Optional: larger channel buffer
    .default_freshness(Duration::from_secs(10 * 60))  // Optional: default staleness timeout
    .metric_freshness("http_requests", Duration::from_secs(2 * 60))  // Per-metric freshness
    .histogram_buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, f64::INFINITY])  // Default buckets
    .metric_histogram_buckets("http_request_duration_seconds", vec![0.001, 0.005, 0.01])  // Per-metric buckets
    .build_install();
```

### Using Metrics

Create and update metrics with macros:

```rust
use metrics::{counter, histogram};

// Counter with labels
let req_counter = counter!("http_requests", "method" => "GET", "path" => "/api/users");
req_counter.increment(1.0);

// Gauge
let memory_gauge = metrics::gauge!("memory_usage_bytes");
memory_gauge.absolute(1024.0 * 1024.0);

// Histogram observation
let latency = 0.05;  // seconds
let duration_hist = histogram!("http_request_duration_seconds", "method" => "GET", "path" => "/api/users");
duration_hist.observe(latency);
```

You can also use the full `Metric` API:

```rust
use metrics::Metric;

let metric = Metric::new_counter("my_counter".to_string(), &[("label".to_string(), "value".to_string())]);
metric.increment(1.0);
```

### Exposing Metrics

To render Prometheus-compatible metrics:

```rust
use metrics::MetricRecorder;

let prometheus_text = MetricRecorder::render_prometheus_string();
println!(&prometheus_text);
```

## Configuration

- **Shards**: Number of storage shards for concurrency. Default: 16. Higher values reduce contention but increase memory.
- **Buffer Size**: Channel capacity for pending metric changes. Default: 256. Set to 0 for unbounded.
- **Freshness Scan Interval**: How often to check for stale metrics. Default: 30s.
- **Default Freshness**: Time after which untouched metrics are removed. Default: 5m.
- **Per-Metric Freshness**: Override default for specific metrics.
- **Histogram Buckets**: Default Prometheus-style buckets. Customizable globally or per-metric.
- **Runtime Updates**: Use `MetricRecorder::set_metric_freshness` and `set_metric_histogram_buckets` after init.

## API Overview

- `Metric`: Core struct holding name, labels, and type. Methods: `increment`, `absolute`, `observe`, `reset`, `as_prometheus`.
- `Value`: Enum for metric values (Counter/Gauge/Histogram).
- `DaveBuilder`: Fluent builder for initialization.
- `MetricRecorder`: Global singleton for rendering and config updates.
- `Storage`: Internal sharded storage (not public).

## Example

See the `example/` directory for a full Actix-web server that tracks HTTP requests and exposes metrics at `/render`.

Run with `cargo run --bin example` (assuming setup in Cargo.toml).

## License

This crate is licensed under the MIT License. See LICENSE file for details (add if needed).
