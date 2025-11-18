use std::{collections::HashMap, num::NonZeroUsize};

use tokio::time::Duration;

use crate::{
    MetricString,
    recorder::{FreshnessConfig, HistogramConfig, MetricDescriptionConfig, MetricRecorder},
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

    /// Default histogram buckets (Prometheus defaults + Inf)
    ///
    /// Default: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, +Inf]
    default_histogram_buckets: Vec<f64>,

    /// Per-metric histogram bucket overrides
    per_metric_histogram_buckets: HashMap<String, Vec<f64>>,

    /// Per-metric descriptions for Prometheus HELP comments
    per_metric_descriptions: HashMap<String, String>,
}

impl Default for DaveBuilder {
    fn default() -> Self {
        Self {
            shards: 16,
            channel_buffer_size: 256,
            freshness_scan_interval: Duration::from_secs(30),
            freshness_duration: Duration::from_secs(5 * 60),
            per_metric_freshness_durations: Default::default(),
            default_histogram_buckets: vec![
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
            per_metric_histogram_buckets: Default::default(),
            per_metric_descriptions: Default::default(),
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

        let mut per_metric_buckets = HashMap::new();
        for (name, buckets) in self.per_metric_histogram_buckets {
            let mut sorted_buckets = buckets;
            sorted_buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if !sorted_buckets.contains(&f64::INFINITY) {
                sorted_buckets.push(f64::INFINITY);
            }
            per_metric_buckets.insert(MetricString::new(name), sorted_buckets);
        }

        let mut default_buckets = self.default_histogram_buckets;
        default_buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if !default_buckets.contains(&f64::INFINITY) {
            default_buckets.push(f64::INFINITY);
        }

        let histogram_config = HistogramConfig {
            default_buckets,
            per_metric_buckets,
        };

        let mut per_metric_descriptions = HashMap::new();
        for (name, description) in self.per_metric_descriptions {
            per_metric_descriptions.insert(MetricString::new(name), description);
        }
        let description_config = MetricDescriptionConfig {
            per_metric_descriptions,
        };

        MetricRecorder::initialize(
            NonZeroUsize::new(self.shards).unwrap(),
            self.channel_buffer_size,
            freshness_config,
            histogram_config,
            description_config,
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

    pub fn histogram_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.default_histogram_buckets = buckets;
        self
    }

    pub fn metric_histogram_buckets(mut self, metric: &str, buckets: Vec<f64>) -> Self {
        let _ = self
            .per_metric_histogram_buckets
            .insert(metric.to_string(), buckets);
        self
    }

    pub fn metric_description(mut self, metric: &str, description: &str) -> Self {
        let _ = self
            .per_metric_descriptions
            .insert(metric.to_string(), description.to_string());
        self
    }
}
