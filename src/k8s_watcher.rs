use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;
use kube::runtime::watcher::{self, Event};
use kube::Client;
use std::collections::HashSet;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub struct PodAnomaly {
    pub name: String,
    pub namespace: String,
    pub reason: String,
    pub message: String,
    pub restart_count: i32,
}

pub struct K8sWatcher {
    client: Client,
    namespaces: HashSet<String>,
}

impl K8sWatcher {
    pub fn new(client: Client, namespaces: Vec<String>) -> Self {
        let namespaces = namespaces.into_iter().collect();
        Self { client, namespaces }
    }

    pub async fn run(self) -> Result<()> {
        info!("Starting Kubernetes Pod Watcher loop...");
        let pods: Api<Pod> = Api::all(self.client.clone());
        let watcher_config = watcher::Config::default();

        let stream = watcher::watcher(pods, watcher_config);
        tokio::pin!(stream);

        while let Some(event) = stream.next().await {
            match event {
                Ok(Event::Apply(pod)) | Ok(Event::InitApply(pod)) => {
                    if let Err(e) = self.handle_pod_update(pod).await {
                        error!("Error handling pod update: {:?}", e);
                    }
                }
                Ok(Event::Delete(pod)) => {
                    let name = pod.metadata.name.clone().unwrap_or_default();
                    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
                    if self.namespaces.contains(&namespace) {
                        info!("Pod deleted: {}/{}", namespace, name);
                    }
                }
                Ok(Event::Init) | Ok(Event::InitDone) => {
                    info!("Watcher initialization event received");
                }
                Err(e) => {
                    error!("Watcher stream error: {:?}", e);
                }
            }
        }

        Ok(())
    }

    async fn handle_pod_update(&self, pod: Pod) -> Result<()> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let namespace = pod.metadata.namespace.clone().unwrap_or_default();

        // Only watch targeted namespaces
        if !self.namespaces.contains(&namespace) {
            return Ok(());
        }

        if let Some(anomaly) = self.detect_anomaly(&pod) {
            warn!(
                "🚨 Anomaly detected in pod {}/{}: {} (Restarts: {}, Message: {})",
                namespace, name, anomaly.reason, anomaly.restart_count, anomaly.message
            );

            // Trigger tactical SRE action
            self.execute_tactical_remediation(anomaly).await?;
        }

        Ok(())
    }

    fn detect_anomaly(&self, pod: &Pod) -> Option<PodAnomaly> {
        let name = pod.metadata.name.clone().unwrap_or_default();
        let namespace = pod.metadata.namespace.clone().unwrap_or_default();
        let status = pod.status.as_ref()?;
        let phase = status.phase.as_deref().unwrap_or_default();

        if phase == "Failed" || phase == "Unknown" {
            return Some(PodAnomaly {
                name,
                namespace,
                reason: format!("Phase: {}", phase),
                message: status.reason.clone().unwrap_or_else(|| "Unknown failure".to_string()),
                restart_count: 0,
            });
        }

        if let Some(container_statuses) = &status.container_statuses {
            for cs in container_statuses {
                let restart_count = cs.restart_count;

                if let Some(state) = &cs.state {
                    // Check if waiting for problematic reasons
                    if let Some(waiting) = &state.waiting {
                        let reason = waiting.reason.as_deref().unwrap_or_default();
                        if reason == "CrashLoopBackOff"
                            || reason == "ImagePullBackOff"
                            || reason == "ErrImagePull"
                            || reason == "CreateContainerConfigError"
                        {
                            return Some(PodAnomaly {
                                name,
                                namespace,
                                reason: reason.to_string(),
                                message: waiting.message.clone().unwrap_or_default(),
                                restart_count,
                            });
                        }
                    }

                    // Check if terminated due to OOM
                    if let Some(terminated) = &state.terminated {
                        let reason = terminated.reason.as_deref().unwrap_or_default();
                        if reason == "OOMKilled" {
                            return Some(PodAnomaly {
                                name,
                                namespace,
                                reason: "OOMKilled".to_string(),
                                message: format!("Exit code: {}", terminated.exit_code),
                                restart_count,
                            });
                        }
                    }
                }

                // Check for high restart counts in running containers
                if restart_count > 5 {
                    return Some(PodAnomaly {
                        name,
                        namespace,
                        reason: "HighRestartCount".to_string(),
                        message: format!("Container has restarted {} times", restart_count),
                        restart_count,
                    });
                }
            }
        }

        None
    }

    async fn execute_tactical_remediation(&self, anomaly: PodAnomaly) -> Result<()> {
        info!(
            "Executing tactical remediation for {}/{} (Reason: {})",
            anomaly.namespace, anomaly.name, anomaly.reason
        );

        if anomaly.reason == "CrashLoopBackOff" || anomaly.reason == "OOMKilled" {
            info!(
                "Initiating pod delete/restart for {}/{}...",
                anomaly.namespace, anomaly.name
            );
            
            let pods: Api<Pod> = Api::namespaced(self.client.clone(), &anomaly.namespace);
            let delete_params = kube::api::DeleteParams::default();
            
            match pods.delete(&anomaly.name, &delete_params).await {
                Ok(_) => info!("Successfully requested deletion of failing pod {}", anomaly.name),
                Err(e) => error!("Failed to delete pod {}: {:?}", anomaly.name, e),
            }
        } else if anomaly.reason == "ImagePullBackOff" {
            warn!(
                "ImagePullBackOff in {}/{} - manual verification or GitOps fix required.",
                anomaly.namespace, anomaly.name
            );
        }

        Ok(())
    }
}
