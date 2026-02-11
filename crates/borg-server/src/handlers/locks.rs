use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::error::ServerError;
use crate::state::{AppState, LockInfo};

#[derive(serde::Deserialize)]
pub struct LockRequest {
    pub hostname: String,
    #[serde(default)]
    pub pid: u64,
}

/// POST /{repo}/locks/{id} — acquire a lock.
pub async fn acquire_lock(
    State(state): State<AppState>,
    Path((repo, id)): Path<(String, String)>,
    axum::Json(body): axum::Json<LockRequest>,
) -> Result<Response, ServerError> {
    let ttl = state.inner.config.lock_ttl_seconds;

    let mut locks = state.inner.locks.write().unwrap();
    let repo_locks = locks.entry(repo.clone()).or_default();

    // Check if lock already exists and is not expired
    if let Some(existing) = repo_locks.get(&id) {
        if !existing.is_expired() {
            return Err(ServerError::Conflict(format!(
                "lock '{id}' already held by {}",
                existing.hostname
            )));
        }
    }

    repo_locks.insert(
        id.clone(),
        LockInfo {
            hostname: body.hostname,
            pid: body.pid,
            acquired_at: chrono::Utc::now(),
            ttl_seconds: ttl,
        },
    );

    Ok(StatusCode::CREATED.into_response())
}

/// DELETE /{repo}/locks/{id} — release a lock.
pub async fn release_lock(
    State(state): State<AppState>,
    Path((repo, id)): Path<(String, String)>,
) -> Result<Response, ServerError> {
    let mut locks = state.inner.locks.write().unwrap();
    if let Some(repo_locks) = locks.get_mut(&repo) {
        if repo_locks.remove(&id).is_some() {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
    }
    Err(ServerError::NotFound(format!("lock '{id}' not found")))
}

/// GET /{repo}/locks?list — list active locks.
pub async fn list_locks(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Response, ServerError> {
    let locks = state.inner.locks.read().unwrap();
    let repo_locks = locks.get(&repo);

    let result: Vec<serde_json::Value> = repo_locks
        .map(|rl| {
            rl.iter()
                .filter(|(_, info)| !info.is_expired())
                .map(|(id, info)| {
                    serde_json::json!({
                        "id": id,
                        "hostname": info.hostname,
                        "pid": info.pid,
                        "acquired_at": info.acquired_at,
                        "ttl_seconds": info.ttl_seconds,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(axum::Json(result).into_response())
}
