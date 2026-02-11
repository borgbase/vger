pub mod admin;
pub mod locks;
pub mod objects;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

pub fn router(state: AppState) -> Router {
    let authed = Router::new()
        // Lock endpoints
        .route(
            "/{repo}/locks/{id}",
            axum::routing::post(locks::acquire_lock)
                .delete(locks::release_lock),
        )
        .route(
            "/{repo}/locks",
            axum::routing::get(locks::list_locks),
        )
        // Admin endpoints (query-string dispatched)
        .route(
            "/{repo}",
            axum::routing::get(admin::repo_dispatch)
                .post(admin::repo_action_dispatch),
        )
        // Repo listing
        .route("/", axum::routing::get(admin::list_repos))
        // Storage object endpoints — wildcard path
        .route(
            "/{repo}/{*path}",
            axum::routing::get(objects::get_or_list)
                .head(objects::head_object)
                .put(objects::put_object)
                .delete(objects::delete_object)
                .post(objects::post_object),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // Health endpoint is unauthenticated
    let public = Router::new().route("/health", axum::routing::get(admin::health));

    public
        .merge(authed)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let expected = state.inner.config.token.as_bytes();

    let provided = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    if provided.as_bytes().ct_eq(expected).into() {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "invalid or missing token").into_response()
    }
}
