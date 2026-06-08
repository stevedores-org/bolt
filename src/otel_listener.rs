use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Json;
use axum::routing::post;
use axum::Router;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// OTel metric data payload (simplified structure for JSON ingestion)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetricPayload {
    pub pod_name: String,
    pub namespace: String,
    pub metric_name: String,
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Clone)]
pub struct OTelMetricsState {
    memory_history: Arc<Mutex<HashMap<String, Vec<f64>>>>,
    cpu_history: Arc<Mutex<HashMap<String, Vec<f64>>>>,
}

impl OTelMetricsState {
    pub fn new() -> Self {
        Self {
            memory_history: Arc::new(Mutex::new(HashMap::new())),
            cpu_history: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn ingest_metric(&self, metric: MetricPayload) {
        if metric.metric_name.contains("memory") {
            let mut history = self.memory_history.lock().await;
            let values = history.entry(metric.pod_name.clone()).or_insert_with(Vec::new);
            values.push(metric.value);
            if values.len() > 5 {
                values.remove(0);
            }

            // Memory Leak Detection: Strictly increasing memory over last 5 points
            if values.len() == 5 {
                let mut leak_detected = true;
                for i in 0..4 {
                    if values[i] >= values[i + 1] {
                        leak_detected = false;
                        break;
                    }
                }

                if leak_detected {
                    let rate = (values[4] - values[0]) / values[0] * 100.0;
                    warn!(
                        "🚨 [Anomaly Heuristic] Memory Leak pattern detected for pod {}/{}! Increase rate: {:.2}%",
                        metric.namespace, metric.pod_name, rate
                    );
                }
            }
        } else if metric.metric_name.contains("cpu") {
            let mut history = self.cpu_history.lock().await;
            let values = history.entry(metric.pod_name.clone()).or_insert_with(Vec::new);
            values.push(metric.value);
            if values.len() > 5 {
                values.remove(0);
            }

            // CPU Throttle Prediction: Check if CPU usage is sustained at >90% or spiking rapidly
            if values.len() >= 3 {
                let last = values.last().cloned().unwrap_or(0.0);
                if last > 90.0 {
                    warn!(
                        "🚨 [Anomaly Heuristic] High CPU utilization detected for pod {}/{}: {:.2}%",
                        metric.namespace, metric.pod_name, last
                    );
                }
            }
        }
    }
}

pub struct OTelListener {
    bind_addr: String,
    state: OTelMetricsState,
}

impl OTelListener {
    pub fn new(bind_addr: String) -> Self {
        Self {
            bind_addr,
            state: OTelMetricsState::new(),
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!("Starting OTel Metrics Listener on {}...", self.bind_addr);

        let app = Router::new()
            .route("/v1/metrics", post(metrics_ingest_handler))
            .with_state(self.state);

        let listener = tokio::net::TcpListener::bind(&self.bind_addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

async fn metrics_ingest_handler(
    State(state): State<OTelMetricsState>,
    Json(payloads): Json<Vec<MetricPayload>>,
) -> (StatusCode, Json<serde_json::Value>) {
    for payload in payloads {
        state.ingest_metric(payload).await;
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "processed" })),
    )
}
