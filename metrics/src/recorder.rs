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

pub(crate) static GLOBAL_RECORDER: OnceLock<MetricRecorder> = OnceLock::new();

pub struct MetricRecorder {
    pub tx: Sender<MetricChange>,
    storage: Storage,
    freshness_config: Arc<RwLock<FreshnessConfig>>,
}

impl MetricRecorder {
    pub fn initialize(
        num_shards: NonZeroUsize,
        chan_cap: usize,
        freshness_config: FreshnessConfig,
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
                let mut buf = metric.as_prometheus();
                buf.push_str(&format!(" {}", value.get()));
                output.push(buf);
            }
        }

        format!("{}\n", output.join("\n"))
    }
}
