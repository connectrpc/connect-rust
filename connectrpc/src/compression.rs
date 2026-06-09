//! Pluggable compression support for ConnectRPC.
//!
//! This module provides a trait-based compression system that allows users to
//! register custom compression providers. Built-in providers are available
//! for common algorithms when the corresponding features are enabled:
//!
//! - `gzip` - Gzip compression via flate2 (enabled by default)
//! - `zstd` - Zstandard compression via zstd (enabled by default)
//!
//! # Streaming Compression
//!
//! When the `streaming` feature is enabled (default), providers can also
//! support streaming compression/decompression for handling large payloads
//! without buffering the entire message in memory.
//!
//! # Example
//!
//! ```rust,ignore
//! use connectrpc::compression::{CompressionRegistry, GzipProvider, ZstdProvider};
//!
//! // Create a registry with built-in providers
//! let registry = CompressionRegistry::new()
//!     .register(GzipProvider::default())
//!     .register(ZstdProvider::default());
//!
//! // Or use the default registry (includes all feature-enabled providers)
//! let registry = CompressionRegistry::default();
//!
//! // Use with server
//! let server = Server::new(router).with_compression(registry);
//! ```
//!
//! # Custom Providers
//!
//! ```rust,ignore
//! use connectrpc::compression::CompressionProvider;
//!
//! struct MyCompression;
//!
//! impl CompressionProvider for MyCompression {
//!     fn name(&self) -> &'static str { "my-algo" }
//!     fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError> { ... }
//!     fn decompressor<'a>(&self, data: &'a [u8]) -> Result<Box<dyn std::io::Read + 'a>, ConnectError> {
//!         // Return a reader that yields decompressed bytes.
//!         // The framework controls how much is read, so you get safe
//!         // `decompress_with_limit` for free via `Read::take`.
//!         ...
//!     }
//! }
//!
//! let registry = CompressionRegistry::new()
//!     .register(MyCompression);
//! ```

use std::collections::HashMap;
#[cfg(feature = "streaming")]
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
#[cfg(feature = "streaming")]
use tokio::io::AsyncBufRead;
#[cfg(feature = "streaming")]
use tokio::io::AsyncRead;

use crate::error::ConnectError;

#[cfg(any(feature = "gzip", feature = "zstd"))]
fn malformed_compressed_payload(message: impl Into<String>) -> ConnectError {
    ConnectError::invalid_argument(message)
}

// ============================================================================
// Streaming Types
// ============================================================================

/// A boxed async reader for streaming compression/decompression.
#[cfg(feature = "streaming")]
#[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
pub type BoxedAsyncRead = Pin<Box<dyn AsyncRead + Send>>;

/// A boxed async buffered reader for streaming input.
#[cfg(feature = "streaming")]
#[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
pub type BoxedAsyncBufRead = Pin<Box<dyn AsyncBufRead + Send>>;

/// Trait for compression algorithm implementations.
///
/// Implement this trait to provide custom compression support. The only
/// required methods are [`name`](Self::name), [`compress`](Self::compress),
/// and [`decompressor`](Self::decompressor). The provided default for
/// [`decompress_with_limit`](Self::decompress_with_limit) is structurally
/// safe — the framework controls how many bytes are read from the
/// decompressor, using [`Read::take`](std::io::Read::take) to cap output and prevent unbounded
/// memory allocation from compression bombs.
///
/// All decompression goes through `decompress_with_limit` — there is no
/// unbounded `decompress` method, by design.
pub trait CompressionProvider: Send + Sync + 'static {
    /// The encoding name for this algorithm.
    ///
    /// This should match the value used in Content-Encoding headers
    /// (e.g., "gzip", "zstd", "br").
    fn name(&self) -> &'static str;

    /// Compress the given data.
    fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError>;

    /// Return a reader that yields decompressed bytes from `data`.
    ///
    /// This is the core decompression method that implementations must
    /// provide. Most decompression libraries provide a `Read` adapter
    /// (e.g. `flate2::read::GzDecoder`, `zstd::Decoder`,
    /// `brotli::Decompressor`) — just wrap the input and return it.
    ///
    /// The framework controls the read loop, so implementations do not
    /// need to worry about size limits. The default
    /// [`decompress_with_limit`](Self::decompress_with_limit) uses
    /// `Read::take(max_size + 1)` to structurally bound memory.
    fn decompressor<'a>(&self, data: &'a [u8])
    -> Result<Box<dyn std::io::Read + 'a>, ConnectError>;

    /// Decompress the given data with a size limit.
    ///
    /// Returns an error if the decompressed data exceeds `max_size`.
    /// This protects against compression bomb attacks.
    ///
    /// The default implementation uses `Read::take(max_size + 1)` on the
    /// reader from [`decompressor`](Self::decompressor) to structurally
    /// bound memory — custom providers are safe without any extra work.
    /// Built-in providers override this for performance.
    fn decompress_with_limit(&self, data: &[u8], max_size: usize) -> Result<Bytes, ConnectError> {
        use std::io::Read;
        let reader = self.decompressor(data)?;
        let capacity = initial_decompress_capacity(data.len(), 2, Some(max_size));
        let mut buf = Vec::with_capacity(capacity);
        reader
            .take((max_size as u64).saturating_add(1))
            .read_to_end(&mut buf)
            .map_err(|e| ConnectError::internal(format!("decompression failed: {e}")))?;
        if buf.len() > max_size {
            return Err(ConnectError::resource_exhausted(format!(
                "decompressed size exceeds limit {max_size}"
            )));
        }
        Ok(Bytes::from(buf))
    }
}

/// Trait for streaming compression support.
///
/// This trait extends [`CompressionProvider`] with streaming methods that
/// process data incrementally without buffering the entire payload in memory.
///
/// Available when the `streaming` feature is enabled (default).
#[cfg(feature = "streaming")]
#[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
pub trait StreamingCompressionProvider: CompressionProvider {
    /// Create a streaming decompressor.
    ///
    /// Returns an `AsyncRead` that decompresses data from the input reader.
    fn decompress_stream(&self, reader: BoxedAsyncBufRead) -> BoxedAsyncRead;

    /// Create a streaming compressor.
    ///
    /// Returns an `AsyncRead` that compresses data from the input reader.
    fn compress_stream(&self, reader: BoxedAsyncBufRead) -> BoxedAsyncRead;
}

/// Registry of compression providers.
///
/// The registry maps encoding names to their provider implementations.
/// Use [`CompressionRegistry::default()`] to get a registry with all
/// feature-enabled built-in providers.
#[derive(Clone)]
pub struct CompressionRegistry {
    providers: Arc<HashMap<&'static str, Arc<dyn CompressionProvider>>>,
    #[cfg(feature = "streaming")]
    streaming_providers: Arc<HashMap<&'static str, Arc<dyn StreamingCompressionProvider>>>,
    /// Cached, sorted, comma-joined list of supported encodings for
    /// Accept-Encoding headers. Recomputed when providers are registered
    /// (rather than on every request).
    accept_encoding: Arc<str>,
}

impl std::fmt::Debug for CompressionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionRegistry")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl CompressionRegistry {
    /// Create an empty compression registry.
    ///
    /// Use [`register`](Self::register) to add providers, or use
    /// [`default`](Self::default) to get a registry with built-in providers.
    pub fn new() -> Self {
        Self {
            providers: Arc::new(HashMap::new()),
            #[cfg(feature = "streaming")]
            streaming_providers: Arc::new(HashMap::new()),
            accept_encoding: Arc::from(""),
        }
    }

    /// Recompute the cached accept-encoding string from the current provider set.
    fn rebuild_accept_encoding(&mut self) {
        let mut encodings: Vec<_> = self.providers.keys().copied().collect();
        encodings.sort_unstable();
        self.accept_encoding = Arc::from(encodings.join(", "));
    }

    /// Register a compression provider.
    ///
    /// Returns self for method chaining.
    ///
    /// # Example
    ///
    /// ```rust
    /// # #[cfg(all(feature = "gzip", feature = "zstd"))] {
    /// use connectrpc::compression::{CompressionRegistry, GzipProvider, ZstdProvider};
    /// let registry = CompressionRegistry::new()
    ///     .register(GzipProvider::default())
    ///     .register(ZstdProvider::default());
    /// assert!(registry.supports("gzip"));
    /// assert!(registry.supports("zstd"));
    /// # }
    /// ```
    #[must_use]
    pub fn register<P: CompressionProvider>(mut self, provider: P) -> Self {
        let providers = Arc::make_mut(&mut self.providers);
        providers.insert(provider.name(), Arc::new(provider));
        self.rebuild_accept_encoding();
        self
    }

    /// Get a provider by encoding name.
    ///
    /// Returns `None` if no provider is registered for the given name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn CompressionProvider>> {
        self.providers.get(name).cloned()
    }

    /// Check if a provider is registered for the given encoding name.
    pub fn supports(&self, name: &str) -> bool {
        self.providers.contains_key(name)
    }

    /// List all supported encoding names.
    pub fn supported_encodings(&self) -> Vec<&'static str> {
        self.providers.keys().copied().collect()
    }

    /// Get a comma-separated string of supported encodings.
    ///
    /// Useful for Accept-Encoding headers. The string is computed once when
    /// providers are registered and cached, so this is a cheap lookup.
    pub fn accept_encoding_header(&self) -> &str {
        &self.accept_encoding
    }

    /// Negotiate a response encoding based on the client's accept-encoding header.
    ///
    /// Returns the first encoding from the client's preference list that this
    /// registry supports, or None if only identity is acceptable.
    ///
    /// Per the Connect spec, the accept-encoding header is treated as an ordered
    /// list with most preferred first (no quality values).
    ///
    /// If the client omits accept-encoding, the spec says the server may assume
    /// the client accepts the same encoding used for the request (if any), plus identity.
    pub fn negotiate_encoding(
        &self,
        accept_encoding: Option<&str>,
        request_encoding: Option<&str>,
    ) -> Option<&'static str> {
        // If client sent Accept-Encoding, use it
        if let Some(accept) = accept_encoding {
            for encoding in accept.split(',').map(|s| s.trim()) {
                if encoding == "identity" {
                    continue; // identity means no compression
                }
                if let Some((key, _)) = self.providers.get_key_value(encoding) {
                    return Some(*key);
                }
            }
            return None; // Client listed encodings but none supported
        }

        // Spec: if client omits accept-encoding, assume it accepts the request encoding
        if let Some(req_enc) = request_encoding
            && req_enc != "identity"
            && let Some((key, _)) = self.providers.get_key_value(req_enc)
        {
            return Some(*key);
        }

        None // No compression
    }

    /// Decompress data using the specified encoding with a size limit.
    ///
    /// Returns an error if the encoding is not supported or if the decompressed
    /// data exceeds `max_size`. This protects against compression bomb attacks.
    ///
    /// For identity encoding, returns the data without copying.
    pub fn decompress_with_limit(
        &self,
        encoding: &str,
        data: Bytes,
        max_size: usize,
    ) -> Result<Bytes, ConnectError> {
        // "identity" means no compression — return data without copying
        if encoding == "identity" {
            if data.len() > max_size {
                return Err(ConnectError::resource_exhausted(format!(
                    "message size {} exceeds limit {}",
                    data.len(),
                    max_size
                )));
            }
            return Ok(data);
        }

        let provider = self.get(encoding).ok_or_else(|| {
            ConnectError::unimplemented(format!("unsupported compression encoding: {encoding}"))
        })?;

        // Per the Connect spec: "Servers must not attempt to decompress
        // zero-length HTTP request content" (and the symmetric rule for
        // clients). A zero-length body with Content-Encoding set is valid —
        // clients may skip compressing empty payloads but still advertise
        // the encoding. Return empty without invoking the decoder, which
        // may reject empty input as an incomplete frame (zstd does).
        // Checked AFTER resolving the encoding so unsupported encodings
        // still error (conformance "unexpected-compression" test).
        if data.is_empty() {
            return Ok(data);
        }

        provider.decompress_with_limit(&data, max_size)
    }

    /// Compress data using the specified encoding.
    ///
    /// Returns an error if the encoding is not supported.
    pub fn compress(&self, encoding: &str, data: &[u8]) -> Result<Bytes, ConnectError> {
        // "identity" means no compression
        if encoding == "identity" {
            return Ok(Bytes::copy_from_slice(data));
        }

        let provider = self.get(encoding).ok_or_else(|| {
            ConnectError::unimplemented(format!("unsupported compression encoding: {encoding}"))
        })?;

        provider.compress(data)
    }

    /// Register a streaming compression provider.
    ///
    /// This also registers the provider for buffered compression.
    /// Returns self for method chaining.
    ///
    /// Available when the `streaming` feature is enabled.
    #[cfg(feature = "streaming")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
    #[must_use]
    pub fn register_streaming<P: StreamingCompressionProvider>(mut self, provider: P) -> Self {
        let name = provider.name();
        let provider = Arc::new(provider);

        // Register for both buffered and streaming
        let providers = Arc::make_mut(&mut self.providers);
        providers.insert(name, provider.clone());

        let streaming_providers = Arc::make_mut(&mut self.streaming_providers);
        streaming_providers.insert(name, provider);

        self.rebuild_accept_encoding();
        self
    }

    /// Get a streaming provider by encoding name.
    ///
    /// Returns `None` if no streaming provider is registered for the given name.
    #[cfg(feature = "streaming")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
    pub fn get_streaming(&self, name: &str) -> Option<Arc<dyn StreamingCompressionProvider>> {
        self.streaming_providers.get(name).cloned()
    }

    /// Check if streaming compression is supported for the given encoding name.
    #[cfg(feature = "streaming")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
    pub fn supports_streaming(&self, name: &str) -> bool {
        self.streaming_providers.contains_key(name)
    }

    /// Create a streaming decompressor for the specified encoding.
    ///
    /// Returns an `AsyncRead` that decompresses data from the input reader.
    /// Returns an error if the encoding is not supported for streaming.
    #[cfg(feature = "streaming")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
    pub fn decompress_stream(
        &self,
        encoding: &str,
        reader: BoxedAsyncBufRead,
    ) -> Result<BoxedAsyncRead, ConnectError> {
        // "identity" means no compression - just return the reader as-is
        if encoding == "identity" {
            return Ok(reader);
        }

        let provider = self.get_streaming(encoding).ok_or_else(|| {
            ConnectError::unimplemented(format!(
                "streaming decompression not supported for encoding: {encoding}"
            ))
        })?;

        Ok(provider.decompress_stream(reader))
    }

    /// Create a streaming compressor for the specified encoding.
    ///
    /// Returns an `AsyncRead` that compresses data from the input reader.
    /// Returns an error if the encoding is not supported for streaming.
    #[cfg(feature = "streaming")]
    #[cfg_attr(docsrs, doc(cfg(feature = "streaming")))]
    pub fn compress_stream(
        &self,
        encoding: &str,
        reader: BoxedAsyncBufRead,
    ) -> Result<BoxedAsyncRead, ConnectError> {
        // "identity" means no compression - just return the reader as-is
        if encoding == "identity" {
            return Ok(reader);
        }

        let provider = self.get_streaming(encoding).ok_or_else(|| {
            ConnectError::unimplemented(format!(
                "streaming compression not supported for encoding: {encoding}"
            ))
        })?;

        Ok(provider.compress_stream(reader))
    }
}

/// Policy controlling when compression is applied.
///
/// By default, messages below 1 KiB are not compressed — at that size,
/// compression overhead (headers, Huffman tables, checksums) typically
/// exceeds the space savings, and the CPU cost of initializing the
/// compressor dominates.
///
/// # Example
///
/// ```rust
/// use connectrpc::CompressionPolicy;
///
/// // Only compress messages >= 4 KiB
/// let policy = CompressionPolicy::default().min_size(4096);
/// assert!(!policy.should_compress(1024));
/// assert!(policy.should_compress(8192));
///
/// // Disable compression entirely
/// let policy = CompressionPolicy::disabled();
/// assert!(!policy.should_compress(1_000_000));
/// ```
#[derive(Debug, Clone, Copy)]
pub struct CompressionPolicy {
    /// Whether compression is enabled at all.
    enabled: bool,
    /// Minimum message size in bytes before compression is applied.
    /// Messages smaller than this are sent uncompressed.
    min_size: usize,
}

/// Default minimum message size for compression (1 KiB).
///
/// Below this threshold, compression typically adds overhead without
/// meaningful size reduction. This matches common defaults in HTTP
/// servers and gRPC implementations (gRPC-Java uses 1 KiB).
pub const DEFAULT_COMPRESSION_MIN_SIZE: usize = 1024;

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            min_size: DEFAULT_COMPRESSION_MIN_SIZE,
        }
    }
}

impl CompressionPolicy {
    /// Create a policy that disables compression entirely.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            min_size: 0,
        }
    }

    /// Set the minimum message size for compression.
    ///
    /// Messages smaller than this (in bytes, before compression) will
    /// be sent uncompressed even if compression is negotiated.
    #[must_use]
    pub fn min_size(mut self, size: usize) -> Self {
        self.min_size = size;
        self
    }

    /// Check whether compression should be applied for a message of the given size.
    ///
    /// Zero-length bodies are compressed when `min_size == 0` (useful for
    /// conformance testing where the runner checks that advertised encodings
    /// are used even for empty payloads). The Connect spec requires receivers
    /// to skip decompression for zero-length content, so this is safe.
    #[inline]
    pub fn should_compress(&self, message_size: usize) -> bool {
        self.enabled && message_size >= self.min_size
    }

    /// Return an effective policy that accounts for a per-call override.
    ///
    /// - `None` → use this policy as-is.
    /// - `Some(true)` → force compression (min_size = 0, enabled = true).
    /// - `Some(false)` → disable compression.
    pub(crate) fn with_override(&self, override_compress: Option<bool>) -> Self {
        match override_compress {
            None => *self,
            Some(true) => Self {
                enabled: true,
                min_size: 0,
            },
            Some(false) => Self::disabled(),
        }
    }
}

impl Default for CompressionRegistry {
    /// Create a registry with all feature-enabled built-in providers.
    #[allow(unused_mut)]
    fn default() -> Self {
        let mut registry = Self::new();

        // When streaming is enabled, use register_streaming to get both capabilities
        #[cfg(all(feature = "gzip", feature = "streaming"))]
        {
            registry = registry.register_streaming(GzipProvider::default());
        }

        #[cfg(all(feature = "gzip", not(feature = "streaming")))]
        {
            registry = registry.register(GzipProvider::default());
        }

        #[cfg(all(feature = "zstd", feature = "streaming"))]
        {
            registry = registry.register_streaming(ZstdProvider::default());
        }

        #[cfg(all(feature = "zstd", not(feature = "streaming")))]
        {
            registry = registry.register(ZstdProvider::default());
        }

        registry
    }
}

// ============================================================================
// Built-in Providers
// ============================================================================

/// Gzip compression provider with internal state pooling.
///
/// Pools `flate2::Compress` and `flate2::Decompress` objects to avoid the
/// ~200 KB allocation overhead per request that comes from creating fresh
/// gzip state tables. The pool is shared across all clones of the
/// `CompressionRegistry` that holds this provider (via `Arc`).
///
/// # Defaults
///
/// The default compression level is **1** (fastest). RPC payloads are
/// latency-sensitive and short-lived; level 1 typically captures most of
/// the size reduction at a fraction of the CPU cost of level 6. Use
/// [`GzipProvider::with_level`] for a different speed/ratio trade-off, or
/// prefer `ZstdProvider` when the peer supports it — zstd at its default
/// level is typically both faster and smaller than gzip on RPC payloads.
///
/// This crate enables `flate2`'s `zlib-rs` backend (a pure-Rust port of
/// zlib-ng), which is substantially faster than the `miniz_oxide` default.
/// Because Cargo features are additive, this selection also applies to any
/// other `flate2` use in the same dependency graph.
///
/// Available when the `gzip` feature is enabled (default).
#[cfg(feature = "gzip")]
#[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
pub struct GzipProvider {
    /// Compression level (0-9, default is 1).
    level: u32,
    compressors: std::sync::Mutex<Vec<flate2::Compress>>,
    decompressors: std::sync::Mutex<Vec<flate2::Decompress>>,
}

#[cfg(feature = "gzip")]
#[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
impl std::fmt::Debug for GzipProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GzipProvider")
            .field("level", &self.level)
            .field(
                "pool_compressors",
                &self.compressors.lock().map(|v| v.len()).unwrap_or(0),
            )
            .field(
                "pool_decompressors",
                &self.decompressors.lock().map(|v| v.len()).unwrap_or(0),
            )
            .finish()
    }
}

#[cfg(feature = "gzip")]
#[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
impl Default for GzipProvider {
    fn default() -> Self {
        Self::with_level(Self::DEFAULT_LEVEL)
    }
}

#[cfg(feature = "gzip")]
#[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
impl GzipProvider {
    /// Default compression level: 1 (fastest).
    ///
    /// See the [type-level docs](GzipProvider#defaults) for rationale.
    pub const DEFAULT_LEVEL: u32 = 1;

    /// Create a new Gzip provider with the default compression level (1).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new Gzip provider with the specified compression level.
    ///
    /// Level should be 0-9, where 0 is no compression and 9 is maximum.
    /// The default is 1 (fastest); use 6 for the conventional zlib default
    /// trade-off, or 9 for maximum compression.
    ///
    /// # Panics
    ///
    /// `flate2` panics at compress time if `level > 9`.
    pub fn with_level(level: u32) -> Self {
        debug_assert!(level <= 9, "gzip level must be 0-9, got {level}");
        Self {
            level,
            compressors: std::sync::Mutex::new(Vec::new()),
            decompressors: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Maximum number of compressor/decompressor instances to retain in
    /// the pool. Excess instances are dropped to bound memory usage.
    const MAX_POOL_SIZE: usize = 64;

    fn take_compressor(&self) -> flate2::Compress {
        self.compressors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop()
            .unwrap_or_else(|| flate2::Compress::new(flate2::Compression::new(self.level), false))
    }

    fn return_compressor(&self, mut c: flate2::Compress) {
        c.reset();
        let mut pool = self.compressors.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < Self::MAX_POOL_SIZE {
            pool.push(c);
        }
    }

    fn take_decompressor(&self) -> flate2::Decompress {
        self.decompressors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop()
            .unwrap_or_else(|| flate2::Decompress::new(false))
    }

    fn return_decompressor(&self, mut d: flate2::Decompress) {
        d.reset(false);
        let mut pool = self.decompressors.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < Self::MAX_POOL_SIZE {
            pool.push(d);
        }
    }

    fn compress_inner(
        compressor: &mut flate2::Compress,
        data: &[u8],
    ) -> Result<Bytes, ConnectError> {
        let mut output = Vec::with_capacity(data.len() + 32);

        // Gzip header (RFC 1952): fixed 10 bytes, no optional fields
        output.extend_from_slice(&[
            0x1f, 0x8b, // magic
            0x08, // method = deflate
            0x00, // flags = none
            0x00, 0x00, 0x00, 0x00, // mtime = 0
            0x00, // extra flags
            0xff, // OS = unknown
        ]);

        // Deflate-compress the data
        let start_in = compressor.total_in();
        loop {
            let consumed = (compressor.total_in() - start_in) as usize;
            output.reserve(output.capacity().max(4096));
            let status = compressor
                .compress_vec(
                    &data[consumed..],
                    &mut output,
                    flate2::FlushCompress::Finish,
                )
                .map_err(|e| ConnectError::internal(format!("gzip compression failed: {e}")))?;
            if status == flate2::Status::StreamEnd {
                break;
            }
        }

        // Gzip trailer: CRC32 + ISIZE (original size mod 2^32)
        let mut crc = flate2::Crc::new();
        crc.update(data);
        output.extend_from_slice(&crc.sum().to_le_bytes());
        output.extend_from_slice(&(data.len() as u32).to_le_bytes());

        Ok(Bytes::from(output))
    }

    fn decompress_inner(
        decompressor: &mut flate2::Decompress,
        data: &[u8],
        max_size: Option<usize>,
    ) -> Result<Bytes, ConnectError> {
        let deflate_start = gzip_header_len(data)?;
        let stream_data = &data[deflate_start..];

        let mut output = Vec::with_capacity(initial_decompress_capacity(data.len(), 2, max_size));

        // Decompress the deflate stream, letting the decompressor find its
        // own end-of-stream marker rather than pre-slicing.
        let start_in = decompressor.total_in();
        loop {
            let consumed = (decompressor.total_in() - start_in) as usize;
            if output.capacity() == output.len() {
                if let Some(limit) = max_size
                    && output.len() > limit
                {
                    return Err(ConnectError::resource_exhausted(format!(
                        "decompressed size exceeds limit {limit}"
                    )));
                }
                // Grow on demand, but never reserve past `limit + 1`: once the
                // buffer fills at that point the over-limit check above fires,
                // so the peak allocation for an over-limit payload stays the
                // same as it was with a limit-sized pre-allocation.
                let mut additional = output.len().max(4096);
                if let Some(limit) = max_size {
                    additional =
                        additional.min(limit.saturating_add(1).saturating_sub(output.capacity()));
                }
                output.reserve_exact(additional);
            }
            let status = decompressor
                .decompress_vec(
                    &stream_data[consumed..],
                    &mut output,
                    flate2::FlushDecompress::None,
                )
                .map_err(|e| {
                    malformed_compressed_payload(format!("gzip decompression failed: {e}"))
                })?;
            match status {
                flate2::Status::StreamEnd => break,
                flate2::Status::Ok => {}
                // Output capacity is always available at this point (ensured
                // above), so `BufError` means the decompressor cannot make
                // progress with the remaining input: the deflate stream ended
                // without an end-of-stream marker. Without this check the
                // loop would never terminate on such input.
                flate2::Status::BufError => {
                    return Err(malformed_compressed_payload(
                        "gzip decompression stalled: truncated or invalid deflate stream",
                    ));
                }
            }
        }

        if let Some(limit) = max_size
            && output.len() > limit
        {
            return Err(ConnectError::resource_exhausted(format!(
                "decompressed size exceeds limit {limit}"
            )));
        }

        // The 8-byte trailer (CRC32 + ISIZE) follows the deflate stream.
        let deflate_consumed = (decompressor.total_in() - start_in) as usize;
        let trailer_start = deflate_consumed;
        if stream_data.len() < trailer_start + 8 {
            return Err(malformed_compressed_payload(
                "gzip data too short for trailer",
            ));
        }
        let trailer = &stream_data[trailer_start..trailer_start + 8];

        let expected_crc = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]);
        let expected_size = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);

        let mut crc = flate2::Crc::new();
        crc.update(&output);
        if crc.sum() != expected_crc {
            return Err(malformed_compressed_payload("gzip CRC32 mismatch"));
        }
        if expected_size != (output.len() as u32) {
            return Err(malformed_compressed_payload("gzip size mismatch"));
        }

        Ok(Bytes::from(output))
    }
}

/// Parse a gzip header (RFC 1952) and return the byte offset where the
/// deflate stream begins.
#[cfg(feature = "gzip")]
fn gzip_header_len(data: &[u8]) -> Result<usize, ConnectError> {
    if data.len() < 10 {
        return Err(malformed_compressed_payload(
            "gzip data too short for header",
        ));
    }
    if data[0] != 0x1f || data[1] != 0x8b {
        return Err(malformed_compressed_payload("invalid gzip magic"));
    }
    if data[2] != 0x08 {
        return Err(malformed_compressed_payload(
            "unsupported gzip compression method",
        ));
    }
    let flags = data[3];
    let mut pos = 10;

    // FEXTRA
    if flags & 0x04 != 0 {
        if pos + 2 > data.len() {
            return Err(malformed_compressed_payload("truncated gzip header"));
        }
        let xlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + xlen;
    }

    // FNAME (null-terminated)
    if flags & 0x08 != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        if pos >= data.len() {
            return Err(malformed_compressed_payload("truncated gzip header"));
        }
        pos += 1; // skip null terminator
    }

    // FCOMMENT (null-terminated)
    if flags & 0x10 != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        if pos >= data.len() {
            return Err(malformed_compressed_payload("truncated gzip header"));
        }
        pos += 1; // skip null terminator
    }

    // FHCRC
    if flags & 0x02 != 0 {
        pos += 2;
    }

    if pos > data.len() {
        return Err(malformed_compressed_payload("truncated gzip header"));
    }
    Ok(pos)
}

#[cfg(feature = "gzip")]
#[cfg_attr(docsrs, doc(cfg(feature = "gzip")))]
impl CompressionProvider for GzipProvider {
    fn name(&self) -> &'static str {
        "gzip"
    }

    fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError> {
        let mut compressor = self.take_compressor();
        let result = Self::compress_inner(&mut compressor, data);
        self.return_compressor(compressor);
        result
    }

    fn decompressor<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn std::io::Read + 'a>, ConnectError> {
        Ok(Box::new(flate2::read::GzDecoder::new(data)))
    }

    fn decompress_with_limit(&self, data: &[u8], max_size: usize) -> Result<Bytes, ConnectError> {
        let mut decompressor = self.take_decompressor();
        let result = Self::decompress_inner(&mut decompressor, data, Some(max_size));
        self.return_decompressor(decompressor);
        result
    }
}

#[cfg(all(feature = "gzip", feature = "streaming"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "gzip", feature = "streaming"))))]
impl StreamingCompressionProvider for GzipProvider {
    fn decompress_stream(&self, reader: BoxedAsyncBufRead) -> BoxedAsyncRead {
        Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader))
    }

    fn compress_stream(&self, reader: BoxedAsyncBufRead) -> BoxedAsyncRead {
        Box::pin(
            async_compression::tokio::bufread::GzipEncoder::with_quality(
                reader,
                async_compression::Level::Precise(self.level as i32),
            ),
        )
    }
}

/// Zstandard compression provider with internal compressor pooling.
///
/// Pools `zstd::bulk::Compressor` objects to avoid repeated allocation of
/// zstd compression contexts. Decompression uses the streaming decoder
/// (`zstd::Decoder`) which handles arbitrary compression ratios without
/// guessing output buffer sizes.
///
/// Available when the `zstd` feature is enabled (default).
#[cfg(feature = "zstd")]
#[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
pub struct ZstdProvider {
    /// Compression level (1-22, default is 3).
    level: i32,
    compressors: std::sync::Mutex<Vec<zstd::bulk::Compressor<'static>>>,
}

#[cfg(feature = "zstd")]
#[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
impl std::fmt::Debug for ZstdProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZstdProvider")
            .field("level", &self.level)
            .field(
                "pool_compressors",
                &self.compressors.lock().map(|v| v.len()).unwrap_or(0),
            )
            .finish()
    }
}

#[cfg(feature = "zstd")]
#[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
impl ZstdProvider {
    /// Default compression level: 3 (the zstd library default).
    pub const DEFAULT_LEVEL: i32 = 3;

    /// Create a new Zstd provider with default compression level.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new Zstd provider with the specified compression level.
    ///
    /// Level should be 1-22, where higher values give better compression
    /// but are slower.
    pub fn with_level(level: i32) -> Self {
        Self {
            level,
            compressors: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[cfg(feature = "zstd")]
#[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
impl Default for ZstdProvider {
    fn default() -> Self {
        Self {
            level: Self::DEFAULT_LEVEL,
            compressors: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[cfg(feature = "zstd")]
impl ZstdProvider {
    /// Maximum number of compressor instances to retain in the pool.
    const MAX_POOL_SIZE: usize = 64;

    fn take_compressor(&self) -> Result<zstd::bulk::Compressor<'static>, ConnectError> {
        if let Some(c) = self
            .compressors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop()
        {
            return Ok(c);
        }
        zstd::bulk::Compressor::new(self.level)
            .map_err(|e| ConnectError::internal(format!("failed to create zstd compressor: {e}")))
    }

    fn return_compressor(&self, c: zstd::bulk::Compressor<'static>) {
        let mut pool = self.compressors.lock().unwrap_or_else(|e| e.into_inner());
        if pool.len() < Self::MAX_POOL_SIZE {
            pool.push(c);
        }
    }

    /// Decompress zstd data with an optional output-size cap.
    ///
    /// Uses the streaming decoder rather than `bulk::Decompressor` because
    /// the bulk API requires guessing an output buffer size up front (and
    /// fails if the guess is too small). The streaming decoder handles any
    /// compression ratio and allows precise `Read::take()` bounding.
    fn decompress_impl(data: &[u8], max_size: Option<usize>) -> Result<Bytes, ConnectError> {
        use std::io::Read;

        let mut decoder = zstd::Decoder::new(data)
            .map_err(|e| malformed_compressed_payload(format!("zstd decompression failed: {e}")))?;

        let mut decompressed =
            Vec::with_capacity(initial_decompress_capacity(data.len(), 4, max_size));

        match max_size {
            Some(limit) => {
                // Read at most limit+1 so we can detect overflow without
                // allocating the entire stream.
                decoder
                    .take((limit as u64).saturating_add(1))
                    .read_to_end(&mut decompressed)
                    .map_err(|e| {
                        malformed_compressed_payload(format!("zstd decompression failed: {e}"))
                    })?;
                if decompressed.len() > limit {
                    return Err(ConnectError::resource_exhausted(format!(
                        "decompressed size exceeds limit {limit}"
                    )));
                }
            }
            None => {
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    malformed_compressed_payload(format!("zstd decompression failed: {e}"))
                })?;
            }
        }
        Ok(Bytes::from(decompressed))
    }
}

#[cfg(feature = "zstd")]
#[cfg_attr(docsrs, doc(cfg(feature = "zstd")))]
impl CompressionProvider for ZstdProvider {
    fn name(&self) -> &'static str {
        "zstd"
    }

    fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError> {
        let mut compressor = self.take_compressor()?;
        let result = compressor
            .compress(data)
            .map(Bytes::from)
            .map_err(|e| ConnectError::internal(format!("zstd compression failed: {e}")));
        self.return_compressor(compressor);
        result
    }

    fn decompressor<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn std::io::Read + 'a>, ConnectError> {
        let decoder = zstd::Decoder::new(data)
            .map_err(|e| malformed_compressed_payload(format!("zstd decompression failed: {e}")))?;
        Ok(Box::new(decoder))
    }

    fn decompress_with_limit(&self, data: &[u8], max_size: usize) -> Result<Bytes, ConnectError> {
        Self::decompress_impl(data, Some(max_size))
    }
}

#[cfg(all(feature = "zstd", feature = "streaming"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "zstd", feature = "streaming"))))]
impl StreamingCompressionProvider for ZstdProvider {
    fn decompress_stream(&self, reader: BoxedAsyncBufRead) -> BoxedAsyncRead {
        Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader))
    }

    fn compress_stream(&self, reader: BoxedAsyncBufRead) -> BoxedAsyncRead {
        Box::pin(
            async_compression::tokio::bufread::ZstdEncoder::with_quality(
                reader,
                async_compression::Level::Precise(self.level),
            ),
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Initial output-buffer capacity for buffered decompression.
///
/// The output buffer becomes the backing allocation of the returned `Bytes`,
/// so it is sized from the compressed input rather than from the configured
/// limit — a limit-sized allocation would stay resident for the lifetime of
/// every (possibly tiny) message. The guess is `input_len × multiplier`
/// (gzip and the trait default use 2; zstd uses 4 because it typically
/// achieves higher ratios on RPC payloads), with a 256-byte floor, capped at
/// `limit + 1` so the initial allocation never exceeds what the limit allows.
///
/// Callers grow the buffer on demand and enforce the limit as it grows; the
/// `read_to_end`-based callers may transiently reserve up to roughly twice
/// the bytes actually written (amortized growth), still bounded by their
/// `Read::take(limit + 1)` readers.
fn initial_decompress_capacity(
    input_len: usize,
    multiplier: usize,
    max_size: Option<usize>,
) -> usize {
    let mut capacity = input_len.saturating_mul(multiplier).max(256);
    if let Some(limit) = max_size {
        capacity = capacity.min(limit.saturating_add(1));
    }
    capacity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(feature = "gzip", feature = "zstd"))]
    fn assert_invalid_argument(err: &ConnectError) {
        assert_eq!(
            err.code,
            crate::error::ErrorCode::InvalidArgument,
            "{err:?}"
        );
    }

    #[test]
    fn test_empty_registry() {
        let registry = CompressionRegistry::new();
        assert!(!registry.supports("gzip"));
        assert!(!registry.supports("zstd"));
        assert!(registry.supported_encodings().is_empty());
    }

    #[test]
    fn test_identity_always_works() {
        let registry = CompressionRegistry::new();
        let data = b"hello world";
        let result = registry
            .decompress_with_limit("identity", Bytes::from_static(data), usize::MAX)
            .unwrap();
        assert_eq!(&result[..], data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_large_roundtrip() {
        let provider = GzipProvider::default();
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let compressed = provider.compress(&data).unwrap();
        let decompressed = provider
            .decompress_with_limit(&compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_pooled_cross_compat_with_gz_decoder() {
        use std::io::Read;
        let provider = GzipProvider::default();
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let compressed = provider.compress(&data).unwrap();
        // Verify standard GzDecoder can read our output
        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_pooled_cross_compat_with_gz_encoder() {
        use std::io::Write;
        let provider = GzipProvider::default();
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        // Compress with standard GzEncoder
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(6));
        encoder.write_all(&data).unwrap();
        let compressed = encoder.finish().unwrap();
        // Decompress with our pooled provider
        let decompressed = provider
            .decompress_with_limit(&compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_default_level_is_fast() {
        assert_eq!(GzipProvider::DEFAULT_LEVEL, 1);
        // Round-trip at the default (fastest) level.
        let provider = GzipProvider::default();
        let data = vec![b'x'; 50_000];
        let compressed = provider.compress(&data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = provider
            .decompress_with_limit(&compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_cross_level_decode() {
        // Output from any level must decode with any provider instance
        // (level only affects encode); also exercises pool reuse across
        // two compress calls on the level-6 provider.
        let fast = GzipProvider::default();
        let slow = GzipProvider::with_level(6);
        let data: Vec<u8> = (0..32_768).map(|i| (i % 251) as u8).collect();
        for src in [&fast, &slow] {
            let _ = src.compress(&data).unwrap();
            let compressed = src.compress(&data).unwrap();
            for dst in [&fast, &slow] {
                let out = dst.decompress_with_limit(&compressed, usize::MAX).unwrap();
                assert_eq!(&out[..], &data[..]);
            }
        }
    }

    #[cfg(all(feature = "gzip", feature = "streaming"))]
    #[tokio::test]
    async fn test_gzip_streaming_honors_level() {
        use tokio::io::AsyncReadExt;
        async fn stream_compress(p: &GzipProvider, data: &[u8]) -> Vec<u8> {
            let reader: BoxedAsyncBufRead = Box::pin(std::io::Cursor::new(data.to_vec()));
            let mut enc = p.compress_stream(reader);
            let mut out = Vec::new();
            enc.read_to_end(&mut out).await.unwrap();
            out
        }
        let data = b"hello world, streaming gzip at the configured level".repeat(200);
        let fast = stream_compress(&GzipProvider::with_level(1), &data).await;
        let best = stream_compress(&GzipProvider::with_level(9), &data).await;
        // Both must round-trip via the buffered decoder.
        for c in [&fast, &best] {
            let out = GzipProvider::default()
                .decompress_with_limit(c, usize::MAX)
                .unwrap();
            assert_eq!(&out[..], &data[..]);
        }
        // Level must actually affect output (previously ignored): level 9 on
        // highly repetitive input compresses strictly smaller than level 1.
        assert!(
            best.len() < fast.len(),
            "level 9 ({}) should be smaller than level 1 ({})",
            best.len(),
            fast.len()
        );
    }

    // ── gzip_header_len tests (RFC 1952 flag parsing) ────────────────

    #[cfg(feature = "gzip")]
    /// Build a gzip header with the given flag byte and optional extra fields.
    /// Returns the header bytes. Does NOT append deflate stream or trailer.
    fn gz_hdr(flags: u8, extra: &[u8]) -> Vec<u8> {
        let mut h = vec![
            0x1f, 0x8b, // magic
            0x08, // method = deflate
            flags, 0, 0, 0, 0,    // mtime
            0,    // XFL
            0xff, // OS = unknown
        ];
        h.extend_from_slice(extra);
        h
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_basic() {
        // No flags → 10-byte fixed header
        assert_eq!(gzip_header_len(&gz_hdr(0x00, &[])).unwrap(), 10);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fextra() {
        // FEXTRA (0x04): 2-byte LE length + that many bytes
        let extra = [3u8, 0, 0xAA, 0xBB, 0xCC]; // xlen=3, then 3 bytes
        assert_eq!(gzip_header_len(&gz_hdr(0x04, &extra)).unwrap(), 10 + 2 + 3);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fextra_truncated() {
        // FEXTRA declares xlen=100 but only 2 bytes follow
        let extra = [100u8, 0, 0xAA, 0xBB];
        assert!(gzip_header_len(&gz_hdr(0x04, &extra)).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fname() {
        // FNAME (0x08): null-terminated string
        let extra = b"test.txt\0";
        assert_eq!(gzip_header_len(&gz_hdr(0x08, extra)).unwrap(), 10 + 9);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fname_truncated() {
        // FNAME with no null terminator
        assert!(gzip_header_len(&gz_hdr(0x08, b"nonul")).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fcomment() {
        // FCOMMENT (0x10): null-terminated string
        let extra = b"a comment\0";
        assert_eq!(gzip_header_len(&gz_hdr(0x10, extra)).unwrap(), 10 + 10);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fcomment_truncated() {
        assert!(gzip_header_len(&gz_hdr(0x10, b"nonul")).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fhcrc() {
        // FHCRC (0x02): 2-byte CRC of header
        assert_eq!(gzip_header_len(&gz_hdr(0x02, &[0xAB, 0xCD])).unwrap(), 12);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_fhcrc_truncated() {
        // FHCRC but only 1 byte follows
        assert!(gzip_header_len(&gz_hdr(0x02, &[0xAB])).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_all_flags() {
        // FEXTRA + FNAME + FCOMMENT + FHCRC, in that order per RFC 1952
        let mut extra = Vec::new();
        extra.extend_from_slice(&[2u8, 0, 0xAA, 0xBB]); // FEXTRA: xlen=2, 2 bytes
        extra.extend_from_slice(b"name\0"); // FNAME: 5 bytes
        extra.extend_from_slice(b"cmt\0"); // FCOMMENT: 4 bytes
        extra.extend_from_slice(&[0x12, 0x34]); // FHCRC: 2 bytes
        let flags = 0x04 | 0x08 | 0x10 | 0x02;
        assert_eq!(
            gzip_header_len(&gz_hdr(flags, &extra)).unwrap(),
            10 + 4 + 5 + 4 + 2
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_bad_magic() {
        let mut hdr = gz_hdr(0x00, &[]);
        hdr[0] = 0x00;
        assert!(gzip_header_len(&hdr).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_bad_method() {
        let mut hdr = gz_hdr(0x00, &[]);
        hdr[2] = 0x07; // not deflate
        assert!(gzip_header_len(&hdr).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_header_len_too_short() {
        assert!(gzip_header_len(&[0x1f, 0x8b, 0x08]).is_err());
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_provider() {
        let provider = GzipProvider::default();
        let data = b"hello world, this is a test of gzip compression";

        let compressed = provider.compress(data).unwrap();
        assert_ne!(&compressed[..], data);

        let decompressed = provider
            .decompress_with_limit(&compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], data);
    }

    /// Limit used by the small-message allocation tests: the default
    /// per-message limit configured by `Limits::default()`.
    const ALLOCATION_TEST_LIMIT: usize = 4 * 1024 * 1024;

    /// Returns the capacity of the allocation backing `bytes`.
    ///
    /// `Bytes::try_into_mut` reuses the original allocation when the handle
    /// is unique, so the resulting `BytesMut::capacity()` exposes how much
    /// memory the decompressed message actually retains.
    fn backing_capacity(bytes: Bytes) -> usize {
        bytes
            .try_into_mut()
            .expect("freshly decompressed Bytes has no other references")
            .capacity()
    }

    /// Upper bound on the backing allocation accepted for a tiny decompressed
    /// message. The sizing heuristic yields 256 bytes today; this leaves
    /// headroom for modest changes while still failing if a limit-sized (or
    /// even tens-of-KiB) buffer is retained per message.
    const SMALL_MESSAGE_RETENTION_BOUND: usize = 4096;

    /// `backing_capacity` must actually observe over-allocation — otherwise
    /// the small-message tests below could pass vacuously if `Bytes::from`
    /// ever started shrinking the allocation itself.
    #[test]
    fn test_backing_capacity_observes_overallocation() {
        let mut vec = Vec::with_capacity(1024 * 1024);
        vec.extend_from_slice(b"tiny payload");
        let capacity = backing_capacity(Bytes::from(vec));
        assert!(
            capacity >= 1024 * 1024,
            "expected the over-allocated backing buffer to be visible, got {capacity}"
        );
    }

    /// Decompressing a small gzip message must not retain a buffer sized by
    /// the configured limit: the returned `Bytes` should be backed by an
    /// allocation proportional to the actual message.
    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_small_message_allocation() {
        let provider = GzipProvider::default();
        let compressed = provider.compress(b"tiny payload").unwrap();
        let out = provider
            .decompress_with_limit(&compressed, ALLOCATION_TEST_LIMIT)
            .unwrap();
        assert_eq!(&out[..], b"tiny payload");
        let capacity = backing_capacity(out);
        assert!(
            capacity < SMALL_MESSAGE_RETENTION_BOUND,
            "small gzip message retained a {capacity}-byte backing buffer"
        );
    }

    /// Same as the gzip allocation test, for the zstd provider.
    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_decompress_small_message_allocation() {
        let provider = ZstdProvider::default();
        let compressed = provider.compress(b"tiny payload").unwrap();
        let out = provider
            .decompress_with_limit(&compressed, ALLOCATION_TEST_LIMIT)
            .unwrap();
        assert_eq!(&out[..], b"tiny payload");
        let capacity = backing_capacity(out);
        assert!(
            capacity < SMALL_MESSAGE_RETENTION_BOUND,
            "small zstd message retained a {capacity}-byte backing buffer"
        );
    }

    /// Same as the gzip allocation test, for the trait's default
    /// `decompress_with_limit` implementation (used by custom providers).
    #[test]
    fn test_default_trait_decompress_small_message_allocation() {
        let provider = MockProvider;
        let compressed = provider.compress(b"tiny payload").unwrap();
        let out = provider
            .decompress_with_limit(&compressed, ALLOCATION_TEST_LIMIT)
            .unwrap();
        assert_eq!(&out[..], b"tiny payload");
        let capacity = backing_capacity(out);
        assert!(
            capacity < SMALL_MESSAGE_RETENTION_BOUND,
            "small message retained a {capacity}-byte backing buffer via the default impl"
        );
    }

    /// Run `f` on a separate thread and require it to produce a result within
    /// `timeout`, failing the test immediately otherwise.
    ///
    /// Threads cannot be killed in Rust, so on timeout the worker is simply
    /// abandoned (it ends when the test process exits). The point is that the
    /// test itself fails fast with a clear message, instead of hanging until
    /// the CI job timeout, if decompression of the input ever stops
    /// terminating.
    #[cfg(feature = "gzip")]
    fn run_with_timeout<T, F>(timeout: std::time::Duration, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(f());
        });
        match rx.recv_timeout(timeout) {
            Ok(value) => value,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("operation did not complete within {timeout:?}; decompression appears stuck")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("worker thread panicked before producing a result")
            }
        }
    }

    /// Deadline for the truncated-input decompression tests; generous so the
    /// tests stay deterministic on slow CI runners while still failing fast
    /// compared to the job timeout.
    #[cfg(feature = "gzip")]
    const TRUNCATION_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

    /// A minimal, valid gzip member header with nothing after it:
    /// id1, id2, CM=8 (deflate), FLG=0, MTIME=0, XFL=0, OS=0xff (unknown).
    #[cfg(feature = "gzip")]
    const MINIMAL_GZIP_HEADER: [u8; 10] =
        [0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff];

    /// A gzip member that is only a header — no deflate data, no trailer —
    /// must be rejected rather than treated as an incomplete stream to wait
    /// on.
    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_header_only() {
        let err = run_with_timeout(TRUNCATION_TEST_TIMEOUT, move || {
            GzipProvider::default().decompress_with_limit(&MINIMAL_GZIP_HEADER, 1024)
        })
        .expect_err("header-only gzip member must be rejected");
        assert_invalid_argument(&err);
        assert!(
            err.to_string()
                .contains("truncated or invalid deflate stream"),
            "unexpected error message: {err}"
        );
    }

    /// A gzip stream cut off in the middle of the deflate data must produce
    /// an error.
    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_truncated_deflate_stream() {
        let provider = GzipProvider::default();
        let data = b"hello world, this is a test of gzip compression";
        let compressed = provider.compress(data).unwrap();

        let err = run_with_timeout(TRUNCATION_TEST_TIMEOUT, move || {
            // Keep the 10-byte header plus a prefix of the deflate stream,
            // drop the rest (including the 8-byte trailer).
            provider.decompress_with_limit(&compressed[..14], 1024)
        })
        .expect_err("truncated deflate stream must be rejected");
        assert_invalid_argument(&err);
        // Which check rejects the prefix depends on where the deflate encoder
        // happened to place block boundaries: an incomplete block is caught by
        // the stalled-stream handling, while a prefix that ends on a complete
        // block is caught by the trailer-length check. Either way the
        // truncated payload must be rejected.
        let msg = err.to_string();
        assert!(
            msg.contains("truncated or invalid deflate stream")
                || msg.contains("too short for trailer"),
            "unexpected error message: {msg}"
        );
    }

    /// A truncated gzip payload is also rejected when it arrives through the
    /// registry (the path the request/response handling code uses).
    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_registry_decompress_truncated() {
        let registry = CompressionRegistry::new().register(GzipProvider::default());
        let err = run_with_timeout(TRUNCATION_TEST_TIMEOUT, move || {
            registry.decompress_with_limit(
                "gzip",
                Bytes::copy_from_slice(&MINIMAL_GZIP_HEADER),
                1024,
            )
        })
        .expect_err("truncated gzip payload must be rejected via the registry");
        assert_invalid_argument(&err);
        assert!(
            err.to_string()
                .contains("truncated or invalid deflate stream"),
            "unexpected error message: {err}"
        );
    }

    /// A complete deflate stream with the 8-byte CRC/length trailer cut off
    /// must produce an error. (The deflate stream itself decodes fully here;
    /// this is rejected by the trailer-length check rather than the
    /// truncated-stream handling.)
    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_missing_trailer() {
        let provider = GzipProvider::default();
        let data = b"hello world, this is a test of gzip compression";
        let compressed = provider.compress(data).unwrap();

        let missing_trailer = &compressed[..compressed.len() - 8];
        let err = provider
            .decompress_with_limit(missing_trailer, 1024)
            .expect_err("gzip member without its trailer must be rejected");
        assert_invalid_argument(&err);
        assert!(
            err.to_string().contains("too short for trailer"),
            "unexpected error message: {err}"
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_malformed_payloads_are_invalid_argument() {
        let provider = GzipProvider::default();

        let err = provider
            .decompress_with_limit(b"not gzip", 1024)
            .expect_err("bad gzip header must be rejected");
        assert_invalid_argument(&err);

        let data = b"hello world, this is a test of gzip compression";
        let compressed = provider.compress(data).unwrap();
        let trailer_start = compressed.len() - 8;

        let mut bad_crc = compressed.to_vec();
        bad_crc[trailer_start] ^= 0xff;
        let err = provider
            .decompress_with_limit(&bad_crc, 1024)
            .expect_err("gzip CRC mismatch must be rejected");
        assert_invalid_argument(&err);

        let mut bad_size = compressed.to_vec();
        let last = bad_size.len() - 1;
        bad_size[last] ^= 0xff;
        let err = provider
            .decompress_with_limit(&bad_size, 1024)
            .expect_err("gzip size mismatch must be rejected");
        assert_invalid_argument(&err);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_registry() {
        let registry = CompressionRegistry::new().register(GzipProvider::default());

        assert!(registry.supports("gzip"));
        assert!(!registry.supports("zstd"));

        let data = b"test data";
        let compressed = registry.compress("gzip", data).unwrap();
        let decompressed = registry
            .decompress_with_limit("gzip", compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_provider() {
        let provider = ZstdProvider::default();
        let data = b"hello world, this is a test of zstd compression";

        let compressed = provider.compress(data).unwrap();
        assert_ne!(&compressed[..], data);

        let decompressed = provider
            .decompress_with_limit(&compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_malformed_payload_is_invalid_argument() {
        let err = ZstdProvider::default()
            .decompress_with_limit(b"not zstd", 1024)
            .expect_err("malformed zstd payload must be rejected");
        assert_invalid_argument(&err);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_high_compression_ratio() {
        // Regression test for the old bulk::Decompressor path which sized
        // the output buffer at `input.len() * 4` — highly-compressible data
        // (e.g. zeroes) can compress >100×, making the guess far too small.
        // The streaming decoder handles any ratio.
        let provider = ZstdProvider::default();
        let data = vec![0u8; 100_000];
        let compressed = provider.compress(&data).unwrap();
        // Sanity: compression ratio should be well above 4×
        assert!(
            compressed.len() * 4 < data.len(),
            "expected high compression ratio; got {} bytes -> {} bytes",
            data.len(),
            compressed.len()
        );
        let decompressed = provider
            .decompress_with_limit(&compressed, usize::MAX)
            .unwrap();
        assert_eq!(decompressed.len(), data.len());
        assert!(decompressed.iter().all(|&b| b == 0));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_registry() {
        let registry = CompressionRegistry::new().register(ZstdProvider::default());

        assert!(registry.supports("zstd"));
        assert!(!registry.supports("gzip"));

        let data = b"test data";
        let compressed = registry.compress("zstd", data).unwrap();
        let decompressed = registry
            .decompress_with_limit("zstd", compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], data);
    }

    #[test]
    fn test_unsupported_encoding() {
        let registry = CompressionRegistry::new();
        let result =
            registry.decompress_with_limit("unknown", Bytes::from_static(b"data"), usize::MAX);
        assert!(result.is_err());
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_decompress_empty_body_with_encoding_header() {
        // Connect spec: "Servers must not attempt to decompress zero-length
        // HTTP request content." Clients may set Content-Encoding but skip
        // compressing empty payloads. The decoder (especially zstd) would
        // reject this as an incomplete frame — the registry must short-circuit.
        let registry = CompressionRegistry::default();
        let result = registry.decompress_with_limit("zstd", Bytes::new(), usize::MAX);
        assert_eq!(result.unwrap().len(), 0);

        let result = registry.decompress_with_limit("gzip", Bytes::new(), usize::MAX);
        assert_eq!(result.unwrap().len(), 0);

        // Also works via decompress_with_limit
        let result = registry.decompress_with_limit("zstd", Bytes::new(), 1024);
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_decompress_empty_body_unknown_encoding_still_errors() {
        // The empty-body short-circuit must NOT mask unsupported encodings.
        // Content-Encoding: foo (unknown) should error even with empty body
        // (conformance "unexpected-compression" test).
        let registry = CompressionRegistry::default();
        let result = registry.decompress_with_limit("foo", Bytes::new(), usize::MAX);
        let err = result.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Unimplemented);
    }

    #[cfg(all(feature = "gzip", feature = "zstd"))]
    #[test]
    fn test_default_registry() {
        let registry = CompressionRegistry::default();
        assert!(registry.supports("gzip"));
        assert!(registry.supports("zstd"));
    }

    #[test]
    fn test_accept_encoding_header() {
        let registry = CompressionRegistry::new();
        assert_eq!(registry.accept_encoding_header(), "");

        #[cfg(feature = "gzip")]
        {
            let registry = CompressionRegistry::new().register(GzipProvider::default());
            assert_eq!(registry.accept_encoding_header(), "gzip");
        }
    }

    #[cfg(all(feature = "gzip", feature = "zstd"))]
    #[test]
    fn test_accept_encoding_header_sorted_deterministic() {
        // Cached string must be identical regardless of registration order.
        let r1 = CompressionRegistry::new()
            .register(GzipProvider::default())
            .register(ZstdProvider::default());
        let r2 = CompressionRegistry::new()
            .register(ZstdProvider::default())
            .register(GzipProvider::default());
        assert_eq!(r1.accept_encoding_header(), "gzip, zstd");
        assert_eq!(r2.accept_encoding_header(), "gzip, zstd");
    }

    // Test custom provider
    struct MockProvider;

    impl CompressionProvider for MockProvider {
        fn name(&self) -> &'static str {
            "mock"
        }

        fn compress(&self, data: &[u8]) -> Result<Bytes, ConnectError> {
            // Just reverse the bytes as a mock "compression"
            Ok(Bytes::from(data.iter().rev().copied().collect::<Vec<_>>()))
        }

        fn decompressor<'a>(
            &self,
            data: &'a [u8],
        ) -> Result<Box<dyn std::io::Read + 'a>, ConnectError> {
            // Reverse bytes and return a reader over the result
            let reversed: Vec<u8> = data.iter().rev().copied().collect();
            Ok(Box::new(std::io::Cursor::new(reversed)))
        }
    }

    #[test]
    fn test_custom_provider() {
        let registry = CompressionRegistry::new().register(MockProvider);

        assert!(registry.supports("mock"));

        let data = b"hello";
        let compressed = registry.compress("mock", data).unwrap();
        assert_eq!(&compressed[..], b"olleh");

        let decompressed = registry
            .decompress_with_limit("mock", compressed, usize::MAX)
            .unwrap();
        assert_eq!(&decompressed[..], data);
    }

    #[cfg(all(feature = "gzip", feature = "streaming"))]
    #[tokio::test]
    async fn test_gzip_streaming() {
        use tokio::io::AsyncReadExt;

        let registry = CompressionRegistry::default();
        assert!(registry.supports_streaming("gzip"));

        // Create test data
        let data = b"hello world, this is a test of streaming gzip compression";

        // Compress using buffered method
        let compressed = registry.compress("gzip", data).unwrap();

        // Decompress using streaming
        let reader: BoxedAsyncBufRead = Box::pin(std::io::Cursor::new(compressed.to_vec()));
        let mut decompressor = registry.decompress_stream("gzip", reader).unwrap();

        let mut decompressed = Vec::new();
        decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed[..], data);
    }

    #[cfg(all(feature = "zstd", feature = "streaming"))]
    #[tokio::test]
    async fn test_zstd_streaming() {
        use tokio::io::AsyncReadExt;

        let registry = CompressionRegistry::default();
        assert!(registry.supports_streaming("zstd"));

        // Create test data
        let data = b"hello world, this is a test of streaming zstd compression";

        // Compress using buffered method
        let compressed = registry.compress("zstd", data).unwrap();

        // Decompress using streaming
        let reader: BoxedAsyncBufRead = Box::pin(std::io::Cursor::new(compressed.to_vec()));
        let mut decompressor = registry.decompress_stream("zstd", reader).unwrap();

        let mut decompressed = Vec::new();
        decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed[..], data);
    }

    #[cfg(all(feature = "gzip", feature = "streaming"))]
    #[tokio::test]
    async fn test_streaming_compress_decompress_roundtrip() {
        use tokio::io::AsyncReadExt;

        let registry = CompressionRegistry::default();

        // Create test data
        let data = b"hello world, this is a roundtrip test of streaming compression";

        // Compress using streaming
        let input: BoxedAsyncBufRead = Box::pin(std::io::Cursor::new(data.to_vec()));
        let mut compressor = registry.compress_stream("gzip", input).unwrap();

        let mut compressed = Vec::new();
        compressor.read_to_end(&mut compressed).await.unwrap();

        // Decompress using streaming
        let reader: BoxedAsyncBufRead = Box::pin(std::io::Cursor::new(compressed));
        let mut decompressor = registry.decompress_stream("gzip", reader).unwrap();

        let mut decompressed = Vec::new();
        decompressor.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed[..], data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_with_limit_under() {
        let provider = GzipProvider::default();
        let data = b"hello world";
        let compressed = provider.compress(data).unwrap();

        // Limit larger than data — succeeds
        let result = provider.decompress_with_limit(&compressed, 1024);
        assert!(result.is_ok());
        assert_eq!(&result.unwrap()[..], data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_with_limit_exact() {
        let provider = GzipProvider::default();
        let data = b"hello world";
        let compressed = provider.compress(data).unwrap();

        // Limit exactly equal to data length — succeeds
        let result = provider.decompress_with_limit(&compressed, data.len());
        assert!(result.is_ok());
        assert_eq!(&result.unwrap()[..], data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_gzip_decompress_with_limit_exceeded() {
        let provider = GzipProvider::default();
        // Create data larger than our limit
        let data = vec![0u8; 1024];
        let compressed = provider.compress(&data).unwrap();

        // Limit smaller than decompressed size — fails
        let result = provider.decompress_with_limit(&compressed, 512);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::ResourceExhausted);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_decompress_with_limit_under() {
        let provider = ZstdProvider::default();
        let data = b"hello world";
        let compressed = provider.compress(data).unwrap();

        let result = provider.decompress_with_limit(&compressed, 1024);
        assert!(result.is_ok());
        assert_eq!(&result.unwrap()[..], data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_decompress_with_limit_exact() {
        let provider = ZstdProvider::default();
        let data = b"hello world";
        let compressed = provider.compress(data).unwrap();

        let result = provider.decompress_with_limit(&compressed, data.len());
        assert!(result.is_ok());
        assert_eq!(&result.unwrap()[..], data);
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn test_zstd_decompress_with_limit_exceeded() {
        let provider = ZstdProvider::default();
        let data = vec![0u8; 1024];
        let compressed = provider.compress(&data).unwrap();

        let result = provider.decompress_with_limit(&compressed, 512);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::ResourceExhausted);
    }

    #[test]
    fn test_compression_policy_default() {
        let policy = CompressionPolicy::default();
        // Below threshold — should not compress
        assert!(!policy.should_compress(512));
        assert!(!policy.should_compress(1023));
        // At and above threshold — should compress
        assert!(policy.should_compress(1024));
        assert!(policy.should_compress(4096));
    }

    #[test]
    fn test_compression_policy_disabled() {
        let policy = CompressionPolicy::disabled();
        assert!(!policy.should_compress(0));
        assert!(!policy.should_compress(1024));
        assert!(!policy.should_compress(1_000_000));
    }

    #[test]
    fn test_compression_policy_custom_min_size() {
        let policy = CompressionPolicy::default().min_size(4096);
        assert!(!policy.should_compress(1024));
        assert!(!policy.should_compress(4095));
        assert!(policy.should_compress(4096));
        assert!(policy.should_compress(8192));
    }

    #[test]
    fn test_compression_policy_empty_message() {
        // Default policy (min_size=1024) skips empty bodies.
        let default_policy = CompressionPolicy::default();
        assert!(!default_policy.should_compress(0));

        // min_size=0 compresses even empty bodies — the Connect spec permits
        // this (receivers skip decompression for zero-length content), and
        // conformance runners check that advertised encodings are applied.
        let zero_min = CompressionPolicy::default().min_size(0);
        assert!(zero_min.should_compress(0));

        let disabled = CompressionPolicy::disabled();
        assert!(!disabled.should_compress(0));
    }

    #[test]
    fn test_compression_policy_with_override() {
        let policy = CompressionPolicy::default();

        // No override — uses policy as-is
        let effective = policy.with_override(None);
        assert!(!effective.should_compress(512));
        assert!(effective.should_compress(2048));

        // Force compression — min_size = 0 means even empty bodies compress
        // (conformance runners verify advertised encodings are applied).
        let forced = policy.with_override(Some(true));
        assert!(forced.should_compress(0));
        assert!(forced.should_compress(1));

        // Disable compression
        let disabled = policy.with_override(Some(false));
        assert!(!disabled.should_compress(0));
        assert!(!disabled.should_compress(1_000_000));
    }

    #[test]
    fn test_identity_decompress_with_limit() {
        let registry = CompressionRegistry::new();
        let data = Bytes::from_static(b"hello world");

        // Under limit
        let result = registry.decompress_with_limit("identity", data.clone(), 1024);
        assert!(result.is_ok());

        // Exact limit
        let result = registry.decompress_with_limit("identity", data.clone(), data.len());
        assert!(result.is_ok());

        // Over limit
        let result = registry.decompress_with_limit("identity", data, 5);
        assert!(result.is_err());
    }
}
