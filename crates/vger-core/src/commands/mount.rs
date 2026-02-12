//! WebDAV server for browsing snapshot contents.
//!
//! Starts a read-only WebDAV server exposing snapshot contents as a virtual
//! filesystem. macOS Finder can mount it natively via "Connect to Server".

use std::collections::HashMap;
use std::convert::Infallible;
use std::fmt;
use std::io::SeekFrom;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::*;
use dav_server::DavHandler;
use futures_util::stream;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lru::LruCache;
use tokio::net::TcpListener;

use crate::commands::list as list_cmd;
use crate::config::VgerConfig;
use crate::crypto::chunk_id::ChunkId;
use crate::error::{Result, VgerError};
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use crate::storage;

// ─── VFS tree ──────────────────────────────────────────────────────────────

/// In-memory virtual filesystem node built from snapshot items.
enum VfsNode {
    Dir {
        children: HashMap<String, VfsNode>,
        meta: VfsMeta,
    },
    File {
        chunks: Vec<ChunkRef>,
        meta: VfsMeta,
    },
    Symlink {
        _target: String,
        meta: VfsMeta,
    },
}

/// Metadata for a VFS node, compatible with WebDAV metadata requirements.
#[derive(Debug, Clone)]
struct VfsMeta {
    size: u64,
    mtime: SystemTime,
    is_dir: bool,
    is_symlink: bool,
}

impl VfsMeta {
    fn from_item(item: &Item) -> Self {
        let mtime = if item.mtime >= 0 {
            UNIX_EPOCH + std::time::Duration::from_nanos(item.mtime as u64)
        } else {
            UNIX_EPOCH
        };
        Self {
            size: item.size,
            mtime,
            is_dir: item.entry_type == ItemType::Directory,
            is_symlink: item.entry_type == ItemType::Symlink,
        }
    }

    fn dir_default() -> Self {
        Self {
            size: 0,
            mtime: UNIX_EPOCH,
            is_dir: true,
            is_symlink: false,
        }
    }
}

fn node_meta(node: &VfsNode) -> &VfsMeta {
    match node {
        VfsNode::Dir { meta, .. } => meta,
        VfsNode::File { meta, .. } => meta,
        VfsNode::Symlink { meta, .. } => meta,
    }
}

/// Build a VFS tree from a list of snapshot items.
fn build_vfs_tree(items: &[Item]) -> VfsNode {
    let mut root_children = HashMap::new();

    for item in items {
        let path = item.path.trim_start_matches('/');
        if path.is_empty() {
            continue;
        }
        let components: Vec<&str> = path.split('/').collect();
        insert_into_tree(&mut root_children, &components, item);
    }

    VfsNode::Dir {
        children: root_children,
        meta: VfsMeta::dir_default(),
    }
}

fn insert_into_tree(children: &mut HashMap<String, VfsNode>, components: &[&str], item: &Item) {
    if components.is_empty() {
        return;
    }

    if components.len() == 1 {
        let name = components[0].to_string();
        // If this is a directory and one already exists as an intermediate,
        // just update its metadata rather than replacing it (which would lose children).
        if item.entry_type == ItemType::Directory {
            if let Some(VfsNode::Dir { meta, .. }) = children.get_mut(&name) {
                *meta = VfsMeta::from_item(item);
                return;
            }
        }
        let node = match item.entry_type {
            ItemType::Directory => VfsNode::Dir {
                children: HashMap::new(),
                meta: VfsMeta::from_item(item),
            },
            ItemType::RegularFile => VfsNode::File {
                chunks: item.chunks.clone(),
                meta: VfsMeta::from_item(item),
            },
            ItemType::Symlink => VfsNode::Symlink {
                _target: item.link_target.clone().unwrap_or_default(),
                meta: VfsMeta::from_item(item),
            },
        };
        children.insert(name, node);
    } else {
        let dir_name = components[0].to_string();
        let entry = children
            .entry(dir_name)
            .or_insert_with(|| VfsNode::Dir {
                children: HashMap::new(),
                meta: VfsMeta::dir_default(),
            });
        if let VfsNode::Dir {
            children: ref mut dir_children,
            ..
        } = entry
        {
            insert_into_tree(dir_children, &components[1..], item);
        }
    }
}

/// Lookup a node by its path bytes (as returned by `DavPath::as_bytes()`).
fn lookup<'a>(root: &'a VfsNode, path: &[u8]) -> Option<&'a VfsNode> {
    let path_str = std::str::from_utf8(path).ok()?;
    let path_str = path_str.trim_start_matches('/');

    if path_str.is_empty() {
        return Some(root);
    }

    let mut current = root;
    for component in path_str.split('/') {
        if component.is_empty() {
            continue;
        }
        match current {
            VfsNode::Dir { children, .. } => {
                current = children.get(component)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

// ─── DavMetaData ───────────────────────────────────────────────────────────

impl DavMetaData for VfsMeta {
    fn len(&self) -> u64 {
        self.size
    }

    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.mtime)
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }

    fn is_symlink(&self) -> bool {
        self.is_symlink
    }
}

// ─── DavDirEntry ───────────────────────────────────────────────────────────

struct VgerDirEntry {
    name: String,
    meta: VfsMeta,
}

impl DavDirEntry for VgerDirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.as_bytes().to_vec()
    }

    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.meta.clone();
        Box::pin(async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) })
    }
}

// ─── DavFileSystem ─────────────────────────────────────────────────────────

/// Read-only WebDAV filesystem backed by vger snapshot data.
#[derive(Clone)]
struct VgerDavFs {
    tree: Arc<VfsNode>,
    repo: Arc<Mutex<Repository>>,
    cache: Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>,
}

impl DavFileSystem for VgerDavFs {
    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        Box::pin(async move {
            let node = lookup(&self.tree, path.as_bytes()).ok_or(FsError::NotFound)?;
            Ok(Box::new(node_meta(node).clone()) as Box<dyn DavMetaData>)
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        Box::pin(async move {
            let node = lookup(&self.tree, path.as_bytes()).ok_or(FsError::NotFound)?;
            match node {
                VfsNode::Dir { children, .. } => {
                    let entries: Vec<_> = children
                        .iter()
                        .map(|(name, child)| {
                            Ok(Box::new(VgerDirEntry {
                                name: name.clone(),
                                meta: node_meta(child).clone(),
                            }) as Box<dyn DavDirEntry>)
                        })
                        .collect();
                    Ok(Box::pin(stream::iter(entries)) as FsStream<Box<dyn DavDirEntry>>)
                }
                _ => Err(FsError::Forbidden),
            }
        })
    }

    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        Box::pin(async move {
            if options.write || options.append || options.create || options.create_new {
                return Err(FsError::Forbidden);
            }

            let node = lookup(&self.tree, path.as_bytes()).ok_or(FsError::NotFound)?;
            match node {
                VfsNode::File { chunks, meta } => {
                    Ok(Box::new(VgerDavFile {
                        chunks: chunks.clone(),
                        meta: meta.clone(),
                        pos: 0,
                        repo: self.repo.clone(),
                        cache: self.cache.clone(),
                    }) as Box<dyn DavFile>)
                }
                _ => Err(FsError::Forbidden),
            }
        })
    }
}

// ─── DavFile ───────────────────────────────────────────────────────────────

struct VgerDavFile {
    chunks: Vec<ChunkRef>,
    meta: VfsMeta,
    pos: u64,
    repo: Arc<Mutex<Repository>>,
    cache: Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>,
}

impl fmt::Debug for VgerDavFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VgerDavFile")
            .field("pos", &self.pos)
            .field("size", &self.meta.size)
            .field("chunks", &self.chunks.len())
            .finish()
    }
}

/// Read a chunk via the LRU cache, falling back to the repository.
fn read_chunk_cached(
    repo: &Arc<Mutex<Repository>>,
    cache: &Arc<Mutex<LruCache<ChunkId, Arc<Vec<u8>>>>>,
    chunk_id: &ChunkId,
) -> FsResult<Arc<Vec<u8>>> {
    // Fast path: cache hit
    {
        let mut guard = cache.lock().unwrap();
        if let Some(data) = guard.get(chunk_id) {
            return Ok(data.clone());
        }
    }

    // Slow path: read from repository
    let data = {
        let guard = repo.lock().unwrap();
        guard
            .read_chunk(chunk_id)
            .map_err(|_| FsError::GeneralFailure)?
    };

    let data = Arc::new(data);

    {
        let mut guard = cache.lock().unwrap();
        guard.put(*chunk_id, data.clone());
    }

    Ok(data)
}

impl DavFile for VgerDavFile {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.meta.clone();
        Box::pin(async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) })
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        let repo = self.repo.clone();
        let cache = self.cache.clone();
        let chunks = self.chunks.clone();
        let start_pos = self.pos;
        let file_size = self.meta.size;

        Box::pin(async move {
            if start_pos >= file_size {
                return Ok(Bytes::new());
            }

            let count = count.min((file_size - start_pos) as usize);

            let result = tokio::task::spawn_blocking(move || -> FsResult<Vec<u8>> {
                let mut buf = Vec::with_capacity(count);
                let mut remaining = count;
                let mut offset = start_pos;
                let mut chunk_start: u64 = 0;

                for chunk_ref in &chunks {
                    let chunk_end = chunk_start + chunk_ref.size as u64;

                    if offset >= chunk_end {
                        chunk_start = chunk_end;
                        continue;
                    }
                    if remaining == 0 {
                        break;
                    }

                    let chunk_data = read_chunk_cached(&repo, &cache, &chunk_ref.id)?;

                    let start_in_chunk = (offset - chunk_start) as usize;
                    let available = chunk_data.len() - start_in_chunk;
                    let to_copy = remaining.min(available);

                    buf.extend_from_slice(
                        &chunk_data[start_in_chunk..start_in_chunk + to_copy],
                    );

                    remaining -= to_copy;
                    offset += to_copy as u64;
                    chunk_start = chunk_end;
                }

                Ok(buf)
            })
            .await
            .map_err(|_| FsError::GeneralFailure)??;

            let bytes_read = result.len() as u64;
            self.pos += bytes_read;
            Ok(Bytes::from(result))
        })
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        Box::pin(async move {
            let new_pos = match pos {
                SeekFrom::Start(p) => p,
                SeekFrom::Current(p) => {
                    if p >= 0 {
                        self.pos.saturating_add(p as u64)
                    } else {
                        self.pos
                            .checked_sub((-p) as u64)
                            .ok_or(FsError::GeneralFailure)?
                    }
                }
                SeekFrom::End(p) => {
                    if p >= 0 {
                        self.meta.size.saturating_add(p as u64)
                    } else {
                        self.meta
                            .size
                            .checked_sub((-p) as u64)
                            .ok_or(FsError::GeneralFailure)?
                    }
                }
            };
            self.pos = new_pos;
            Ok(new_pos)
        })
    }

    fn write_buf(&mut self, _buf: Box<dyn bytes::Buf + Send>) -> FsFuture<'_, ()> {
        Box::pin(async { Err(FsError::Forbidden) })
    }

    fn write_bytes(&mut self, _buf: Bytes) -> FsFuture<'_, ()> {
        Box::pin(async { Err(FsError::Forbidden) })
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

// ─── Public API ────────────────────────────────────────────────────────────

/// Start a read-only WebDAV server exposing snapshot contents.
///
/// If `snapshot_name` is given, serves that single snapshot at the root.
/// Otherwise, serves all snapshots as top-level directories.
pub fn run(
    config: &VgerConfig,
    passphrase: Option<&str>,
    snapshot_name: Option<&str>,
    address: &str,
    cache_size: usize,
) -> Result<()> {
    let backend = storage::backend_from_config(&config.repository)?;
    let repo = Repository::open(backend, passphrase)?;

    // Build the VFS tree from snapshot items
    eprintln!("Loading snapshot data...");
    let tree = if let Some(name) = snapshot_name {
        let items = list_cmd::load_snapshot_items(&repo, name)?;
        eprintln!("Loaded {} items from snapshot '{name}'", items.len());
        build_vfs_tree(&items)
    } else {
        let mut root_children = HashMap::new();
        for entry in &repo.manifest.snapshots {
            let items = list_cmd::load_snapshot_items(&repo, &entry.name)?;
            eprintln!(
                "Loaded {} items from snapshot '{}'",
                items.len(),
                entry.name
            );
            root_children.insert(entry.name.clone(), build_vfs_tree(&items));
        }
        VfsNode::Dir {
            children: root_children,
            meta: VfsMeta::dir_default(),
        }
    };

    let repo = Arc::new(Mutex::new(repo));
    let cache_size = NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(256).unwrap());
    let cache = Arc::new(Mutex::new(LruCache::new(cache_size)));

    let fs = VgerDavFs {
        tree: Arc::new(tree),
        repo,
        cache,
    };

    let handler = DavHandler::builder()
        .filesystem(Box::new(fs))
        .build_handler();

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| VgerError::Other(format!("failed to create tokio runtime: {e}")))?;

    rt.block_on(async { serve(handler, address).await })
}

async fn serve(handler: DavHandler, address: &str) -> Result<()> {
    let addr: std::net::SocketAddr = address
        .parse()
        .map_err(|e| VgerError::Config(format!("invalid address '{address}': {e}")))?;

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| VgerError::Other(format!("failed to bind to {addr}: {e}")))?;

    eprintln!("WebDAV server listening on http://{addr}");
    eprintln!("Connect with: Finder → Go → Connect to Server → http://{addr}");
    eprintln!("Press Ctrl+C to stop.");

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result
                    .map_err(|e| VgerError::Other(format!("accept error: {e}")))?;
                let io = TokioIo::new(stream);
                let handler = handler.clone();

                tokio::spawn(async move {
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(
                            io,
                            service_fn(move |req| {
                                let handler = handler.clone();
                                async move {
                                    Ok::<_, Infallible>(handler.handle(req).await)
                                }
                            }),
                        )
                        .await
                    {
                        tracing::debug!("connection error: {e}");
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("\nShutting down.");
                break;
            }
        }
    }

    Ok(())
}
