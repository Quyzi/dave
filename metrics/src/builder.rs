use std::{collections::HashMap, num::NonZeroUsize};

use tokio::time::Duration;

use crate::{
    MetricString,
    recorder::{FreshnessConfig, MetricRecorder},
};

pub struct DaveBuilder {
    /// The number of shards to use for storing metrics
    ///
    /// Default: 16
    shards: usize,

    /// The size of the buffer channel for accepting metrics changes
    ///
    /// Default: 256
    channel_buffer_size: usize,

    /// The frequency to scan for old metrics to be invalidated
    ///
    /// Default: 30s
    freshness_scan_interval: Duration,

    /// The default freshness for metrics.  Metrics that have not been
    /// updated within this duration are invalidated and removed.
    ///
    /// Default: 5m
    freshness_duration: Duration,

    /// Metric freshness can be customized by metric name.  Dave will use
    /// the custom metric freshness if it exists, or the default otherwise.
    per_metric_freshness_durations: HashMap<String, Duration>,
}

impl Default for DaveBuilder {
    fn default() -> Self {
        Self {
            shards: 16,
            channel_buffer_size: 256,
            freshness_scan_interval: Duration::from_secs(30),
            freshness_duration: Duration::from_secs(5 * 60),
            per_metric_freshness_durations: Default::default(),
        }
    }
}

impl DaveBuilder {
    pub fn build_install(self) {
        let mut per_metric_durations = HashMap::new();
        for (name, dur) in self.per_metric_freshness_durations {
            per_metric_durations.insert(MetricString::new(name), dur);
        }
        let freshness_config = FreshnessConfig {
            default_duration: self.freshness_duration,
            scan_interval: self.freshness_scan_interval,
            per_metric_durations,
        };
        MetricRecorder::initialize(
            NonZeroUsize::new(self.shards).unwrap(),
            self.channel_buffer_size,
            freshness_config,
        );
    }

    pub fn num_shards(mut self, shards: NonZeroUsize) -> Self {
        self.shards = shards.get();
        self
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.channel_buffer_size = size;
        self
    }

    pub fn freshness_scan<D: Into<Duration>>(mut self, dur: D) -> Self {
        self.freshness_scan_interval = dur.into();
        self
    }

    pub fn default_freshness<D: Into<Duration>>(mut self, dur: D) -> Self {
        self.freshness_duration = dur.into();
        self
    }

    pub fn metric_freshness<D: Into<Duration>>(mut self, metric: &str, dur: D) -> Self {
        let _ = self
            .per_metric_freshness_durations
            .insert(metric.to_string(), dur.into());
        self
    }
}
