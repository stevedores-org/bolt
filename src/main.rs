use anyhow::Result;
use clap::Parser;
use tokio::signal;
use tracing::{error, info};

mod k8s_watcher;
mod otel_listener;

#[derive(Parser, Debug)]
#[command(name = "bolt", version = "0.1.0", author = "Stevedores")]
struct Args {
    /// Kubernetes namespaces to monitor (comma separated)
    #[arg(
        long,
        env = "BOLT_NAMESPACES",
        default_value = "lornu-ai-dev,lornu-ai-staging,lornu-ai-prod"
    )]
    namespaces: String,

    /// Address to bind the OTel metrics ingestion listener
    #[arg(long, env = "BOLT_OTEL_ADDR", default_value = "0.0.0.0:8090")]
    otel_addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging using tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "bolt=info,tower_http=debug".into()),
        )
        .init();

    info!("⚡ Bolt: Proactive Rust-Native SRE Operator starting...");

    let args = Args::parse();
    let namespaces: Vec<String> = args
        .namespaces
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    info!("Monitoring namespaces: {:?}", namespaces);

    // Initialize Kubernetes client
    let client = match kube::Client::try_default().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to initialize Kubernetes client: {:?}", e);
            anyhow::bail!("Kubernetes connection failed: {}", e);
        }
    };

    // Instantiate Watcher
    let watcher = k8s_watcher::K8sWatcher::new(client.clone(), namespaces);

    // Instantiate OTel Listener
    let otel_listener = otel_listener::OTelListener::new(args.otel_addr);

    // Spawn Kubernetes Watcher task
    let watcher_handle = tokio::spawn(async move {
        if let Err(e) = watcher.run().await {
            error!("Kubernetes Watcher loop exited with error: {:?}", e);
        }
    });

    // Spawn OTel Listener task
    let otel_handle = tokio::spawn(async move {
        if let Err(e) = otel_listener.run().await {
            error!("OTel Metrics Listener exited with error: {:?}", e);
        }
    });

    // Wait for termination signal
    info!("Bolt Operator fully initialized. Waiting for termination signals...");
    
    signal::ctrl_c().await?;
    info!("Received Ctrl+C signal. Initiating graceful shutdown...");

    // Abort active tasks
    watcher_handle.abort();
    otel_handle.abort();

    info!("Gracefully stopped all monitoring loops. Goodbye!");
    Ok(())
}
