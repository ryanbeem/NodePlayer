//! Serves files a node added to the playlist so other nodes can stream them.
//! Range requests are supported, so players can seek without downloading.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::net::TcpListener;
use tower::ServiceExt;
use tower_http::services::ServeFile;

/// file_id -> path on disk, for files this node shares.
#[derive(Clone, Default)]
pub struct SharedFiles(Arc<Mutex<HashMap<String, PathBuf>>>);

impl SharedFiles {
    pub fn insert(&self, file_id: String, path: PathBuf) {
        self.0.lock().unwrap().insert(file_id, path);
    }

    pub fn get(&self, file_id: &str) -> Option<PathBuf> {
        self.0.lock().unwrap().get(file_id).cloned()
    }
}

pub async fn serve(listener: TcpListener, files: SharedFiles) {
    let app = Router::new()
        .route("/media/{file_id}", get(serve_file))
        .with_state(files);
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("media server stopped: {e}");
    }
}

async fn serve_file(
    State(files): State<SharedFiles>,
    Path(file_id): Path<String>,
    req: Request,
) -> Response {
    match files.get(&file_id) {
        Some(path) => match ServeFile::new(path).oneshot(req).await {
            Ok(resp) => resp.into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
