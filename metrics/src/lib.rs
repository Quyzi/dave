use internment::ArcIntern;
use recorder::{GLOBAL_RECORDER, MetricChange};
use std::hash::Hasher;
use std::ops::AddAssign;
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash},
    num::NonZeroUsize,
    sync::{Arc, RwLock},
};
use tokio::time::{Duration, Instant};

pub mod recorder;

pub type MetricString = ArcIntern<String>;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
}

#[derive(Debug, Clone)]
pub enum Value {
    Counter(f64),
    Gauge(f64),
}

impl Value {
    pub fn get(&self) -> f64 {
        match self {
            Value::Counter(f) => *f,
            Value::Gauge(f) => *f,
        }
    }
    pub fn increment(&mut self, by: f64) {
        match self {
            Value::Counter(v) => v.add_assign(by),
            Value::Gauge(v) => v.add_assign(by),
        }
    }

    pub fn absolute(&mut self, to: f64) {
        match self {
            Value::Counter(v) => *v = to,
            Value::Gauge(v) => *v = to,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Metric {
    pub name: MetricString,
    pub labels: Vec<(MetricString, MetricString)>,
    pub metric_type: MetricType,
}

impl Metric {
    pub fn new_counter(name: String, labels: &[(String, String)]) -> Self {
        Self::new(name, labels, MetricType::Counter)
    }

    pub fn new_gauge(name: String, labels: &[(String, String)]) -> Self {
        Self::new(name, labels, MetricType::Gauge)
    }

    pub fn new(name: String, labels: &[(String, String)], metric_type: MetricType) -> Self {
        let name = MetricString::new(name);
        let mut labels = labels
            .iter()
            .map(|(k, v)| {
                (
                    MetricString::new(k.to_string()),
                    MetricString::new(v.to_string()),
                )
            })
            .collect::<Vec<_>>();
        labels.sort();
        Self {
            name,
            labels,
            metric_type,
        }
    }

    pub fn increment(&self, by: f64) {
        if let Some(tx) = GLOBAL_RECORDER.get() {
            let _ = tx.tx.send(MetricChange::Delta {
                metric: self.clone(),
                delta: by,
            });
        }
    }

    pub fn absolute(&self, value: f64) {
        if let Some(tx) = GLOBAL_RECORDER.get() {
            let _ = tx.tx.send(MetricChange::Absolute {
                metric: self.clone(),
                value,
            });
        }
    }

    pub fn reset(&self) {
        if let Some(tx) = GLOBAL_RECORDER.get() {
            let _ = tx.tx.send(MetricChange::Reset {
                metric: self.clone(),
            });
        }
    }

    pub fn as_prometheus(&self) -> String {
        let mut buf = self.name.to_string();
        if !self.labels.is_empty() {
            buf.push('{');
            for (i, (label, value)) in self.labels.iter().enumerate() {
                let l = format!(r#"{}="{}""#, label, value);
                buf.push_str(&l);
                if i < self.labels.len() - 1 {
                    buf.push(',');
                }
            }
            buf.push('}');
        }

        buf
    }
}

pub(crate) struct Storage {
    pub(crate) shards: Arc<Vec<RwLock<HashMap<Metric, (Value, Instant)>>>>,
}

impl Storage {
    pub fn new(num_shards: NonZeroUsize) -> Self {
        let shards = (0..num_shards.get())
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self {
            shards: Arc::new(shards),
        }
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    pub(crate) fn which_shard(&self, key: &Metric) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards()
    }

    pub(crate) fn increment(&self, key: &Metric, by: f64) {
        let shard = self.which_shard(key);
        let mut guard = self.shards[shard].write().unwrap();
        let now = Instant::now();
        guard
            .entry(key.clone())
            .and_modify(|(value, timestamp)| {
                value.increment(by);
                *timestamp = now;
            })
            .or_insert({
                let value = match key.metric_type {
                    MetricType::Counter => Value::Counter(by),
                    MetricType::Gauge => Value::Gauge(by),
                };
                (value, now)
            });
    }

    pub(crate) fn absolute(&self, key: &Metric, to: f64) {
        let shard = self.which_shard(key);
        let mut guard = self.shards[shard].write().unwrap();
        let now = Instant::now();
        guard
            .entry(key.clone())
            .and_modify(|(value, timestamp)| {
                value.absolute(to);
                *timestamp = now;
            })
            .or_insert({
                let value = match key.metric_type {
                    MetricType::Counter => Value::Counter(to),
                    MetricType::Gauge => Value::Gauge(to),
                };
                (value, now)
            });
    }

    pub(crate) fn remove_stale_metrics(
        &self,
        shard_idx: usize,
        default_duration: Duration,
        per_metric_durations: &HashMap<MetricString, Duration>,
    ) {
        let mut guard = self.shards[shard_idx].write().unwrap();
        let now = Instant::now();
        guard.retain(|metric, (_value, timestamp)| {
            let freshness_duration = per_metric_durations
                .get(&metric.name)
                .copied()
                .unwrap_or(default_duration);
            now.duration_since(*timestamp) < freshness_duration
        });
    }
}

#[macro_export]
macro_rules! counter {
    ($name:expr $(, $label:expr => $lvalue:expr)*) => {{
        let mut labels = vec![];
        $(
            labels.push(($label.to_string(), $lvalue.to_string()));
        )*
        Metric::new_counter($name.to_string(), &labels)
    }};
}

#[macro_export]
macro_rules! gauge {
    ($name:expr $(, $label:expr => $lvalue:expr)*) => {{
        let mut labels = vec![];
        $(
            labels.push(($label.to_string(), $lvalue.to_string()));
        )*
        Metric::new_gauge($name.to_string(), &labels)
    }};
}
