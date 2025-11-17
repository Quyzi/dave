use crate::{Metric, MetricString, Storage};
use flume::{Receiver, Sender};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, OnceLock, RwLock},
};
use tokio::time::{Duration, interval};

pub enum MetricChange {
    Delta { metric: Metric, delta: f64 },
    Absolute { metric: Metric, value: f64 },
    Reset { metric: Metric },
    Observe { metric: Metric, value: f64 },
}

#[derive(Debug, Clone)]
pub struct FreshnessConfig {
    pub default_duration: Duration,
    pub scan_interval: Duration,
    pub per_metric_durations: HashMap<MetricString, Duration>,
}

impl Default for FreshnessConfig {
    fn default() -> Self {
        Self {
            default_duration: Duration::from_secs(5 * 60),
            scan_interval: Duration::from_secs(30),
            per_metric_durations: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistogramConfig {
    pub default_buckets: Vec<f64>,
    pub per_metric_buckets: HashMap<MetricString, Vec<f64>>,
}

impl Default for HistogramConfig {
    fn default() -> Self {
        Self {
            default_buckets: vec![
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
                10.0,
                f64::INFINITY,
            ],
            per_metric_buckets: HashMap::new(),
        }
    }
}

pub(crate) static GLOBAL_RECORDER: OnceLock<MetricRecorder> = OnceLock::new();

pub struct MetricRecorder {
    pub tx: Sender<MetricChange>,
    storage: Storage,
    freshness_config: Arc<RwLock<FreshnessConfig>>,
    histogram_config: Arc<RwLock<HistogramConfig>>,
}

impl MetricRecorder {
    pub fn initialize(
        num_shards: NonZeroUsize,
        chan_cap: usize,
        freshness_config: FreshnessConfig,
        histogram_config: HistogramConfig,
    ) {
        let (tx, rx) = if chan_cap == 0 {
            flume::unbounded()
        } else {
            flume::bounded(chan_cap)
        };
        let storage = Storage::new(num_shards);
        let this = Self {
            tx,
            storage,
            freshness_config: Arc::new(RwLock::new(freshness_config)),
            histogram_config: Arc::new(RwLock::new(histogram_config)),
        };

        let _ = GLOBAL_RECORDER.set(this);
        tokio::spawn(Self::recorder(rx));
        tokio::spawn(Self::invalidation_task());
    }

    pub fn set_metric_freshness<D: Into<Duration>>(metric_name: String, duration: D) {
        let Some(this) = GLOBAL_RECORDER.get() else {
            return;
        };
        let duration = duration.into();
        if let Ok(mut config) = this.freshness_config.write() {
            config
                .per_metric_durations
                .insert(MetricString::new(metric_name), duration);
        }
    }

    pub fn get_metric_freshness(metric_name: &str) -> Option<Duration> {
        let this = GLOBAL_RECORDER.get()?;
        let config = this.freshness_config.read().ok()?;

        config
            .per_metric_durations
            .get(&MetricString::new(metric_name.to_string()))
            .copied()
            .or(Some(config.default_duration))
    }

    pub fn set_metric_histogram_buckets(metric_name: String, mut buckets: Vec<f64>) {
        let Some(this) = GLOBAL_RECORDER.get() else {
            return;
        };
        buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());

        if let Ok(mut config) = this.histogram_config.write() {
            config
                .per_metric_buckets
                .insert(MetricString::new(metric_name.clone()), buckets);
        }
    }

    pub fn get_metric_histogram_buckets(metric_name: &str) -> Option<Vec<f64>> {
        let this = GLOBAL_RECORDER.get()?;
        let config = this.histogram_config.read().ok()?;

        config
            .per_metric_buckets
            .get(&MetricString::new(metric_name.to_string()))
            .cloned()
            .or_else(|| Some(config.default_buckets.clone()))
    }

    async fn recorder(rx: Receiver<MetricChange>) {
        let Some(this) = GLOBAL_RECORDER.get() else {
            return;
        };
        while let Ok(change) = rx.recv_async().await {
            match change {
                MetricChange::Delta { metric, delta } => {
                    this.storage.increment(&metric, delta);
                }
                MetricChange::Absolute { metric, value } => {
                    this.storage.absolute(&metric, value);
                }
                MetricChange::Reset { metric } => {
                    this.storage.absolute(&metric, 0.0);
                }
                MetricChange::Observe { metric, value } => {
                    // Get the appropriate bucket boundaries for this metric
                    let buckets = {
                        let config = this.histogram_config.read().unwrap();
                        config
                            .per_metric_buckets
                            .get(&metric.name)
                            .cloned()
                            .unwrap_or_else(|| config.default_buckets.clone())
                    };
                    this.storage.observe(&metric, value, &buckets);
                }
            }
        }
    }

    async fn invalidation_task() {
        let Some(this) = GLOBAL_RECORDER.get() else {
            return;
        };

        let scan_interval = {
            let config = this.freshness_config.read().unwrap();
            config.scan_interval
        };

        let mut interval_timer = interval(scan_interval);
        interval_timer.tick().await; // First tick completes immediately

        loop {
            interval_timer.tick().await;

            let (default_duration, per_metric_durations) = {
                let config = this.freshness_config.read().unwrap();
                (config.default_duration, config.per_metric_durations.clone())
            };

            let num_shards = this.storage.num_shards();
            for shard_idx in 0..num_shards {
                this.storage.remove_stale_metrics(
                    shard_idx,
                    default_duration,
                    &per_metric_durations,
                );
            }
        }
    }

    pub fn render_prometheus_string() -> String {
        let Some(this) = GLOBAL_RECORDER.get() else {
            return String::new();
        };

        let mut output = vec![];
        for shard in &*this.storage.shards {
            let Ok(guard) = shard.read() else {
                continue;
            };
            for (metric, (value, _timestamp)) in &*guard {
                match value {
                    crate::Value::Counter(_) | crate::Value::Gauge(_) => {
                        let mut buf = metric.as_prometheus();
                        buf.push_str(&format!(" {}", value.get()));
                        output.push(buf);
                    }
                    crate::Value::Histogram {
                        buckets,
                        sum,
                        count,
                    } => {
                        // Get bucket boundaries for this metric
                        let bucket_boundaries = {
                            let config = this.histogram_config.read().unwrap();
                            config
                                .per_metric_buckets
                                .get(&metric.name)
                                .cloned()
                                .unwrap_or_else(|| config.default_buckets.clone())
                        };

                        for (boundary, sum) in bucket_boundaries.iter().zip(buckets.iter()) {
                            let mut m = metric.clone();
                            m.labels.push((
                                MetricString::new("le".to_string()),
                                MetricString::new(boundary.to_string()),
                            ));
                            let mut buf = m.as_prometheus();
                            buf.push_str(&format!(" {}", sum));
                            output.push(buf);
                        }

                        // Render _sum
                        let mut sum_metric = metric.clone();
                        sum_metric.name = crate::MetricString::new(format!("{}_sum", metric.name));
                        let mut buf = sum_metric.as_prometheus();
                        buf.push_str(&format!(" {}", sum));
                        output.push(buf);

                        // Render _count
                        let mut count_metric = metric.clone();
                        count_metric.name =
                            crate::MetricString::new(format!("{}_count", metric.name));
                        let mut buf = count_metric.as_prometheus();
                        buf.push_str(&format!(" {}", count));
                        output.push(buf);
                    }
                }
            }
        }

        format!("{}\n", output.join("\n"))
    }
}
