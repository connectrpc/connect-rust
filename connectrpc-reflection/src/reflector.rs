//! The descriptor index behind the reflection service.
//!
//! A [`Reflector`] answers the five queries of the gRPC server reflection
//! protocol — file by name, file containing symbol, file containing
//! extension, extension numbers of a type, and the service list — by
//! delegating name resolution to a [`buffa_descriptor::DescriptorPool`].
//!
//! Two descriptor sources are supported:
//!
//! - **Wire-format `FileDescriptorSet` bytes**
//!   ([`from_descriptor_set_bytes`](Reflector::from_descriptor_set_bytes)),
//!   e.g. the output of `connectrpc_build::Config::emit_descriptor_set`.
//!   Responses carry the **original** per-file `FileDescriptorProto` bytes
//!   sliced out of the input — never a re-encode — so descriptor payloads
//!   produced by newer compilers survive byte-for-byte.
//! - **An existing [`DescriptorPool`]**
//!   ([`from_descriptor_pool`](Reflector::from_descriptor_pool)), e.g. the
//!   `descriptor_pool()` a buffa-generated package exposes when reflection
//!   is enabled. Responses re-encode the pool's parsed
//!   `FileDescriptorProto`s; buffa retains unknown fields, so the bytes
//!   are semantically faithful but not guaranteed byte-identical to the
//!   compiler's output.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use buffa::Message;
use buffa_descriptor::generated::descriptor::{FileDescriptorProto, FileDescriptorSet};
use buffa_descriptor::{DescriptorPool, PoolError};

/// Errors from building a [`Reflector`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReflectionError {
    /// The bytes did not decode as a `FileDescriptorSet`.
    ///
    /// Exceeding the element-memory budget is reported separately as
    /// [`ElementBudget`](Self::ElementBudget), because there the bytes are
    /// fine; this variant is every other wire-level failure.
    #[error("failed to decode FileDescriptorSet: {0}")]
    Decode(buffa::DecodeError),
    /// The descriptor set was well-formed but exceeded the decode's
    /// element-memory budget.
    ///
    /// Split apart from [`Decode`](Self::Decode) because the remedy is
    /// different: the bytes are fine, and a reflection service is normally
    /// handed its own server's descriptors, which are trusted. The remedy is
    /// a smaller set — strip `source_code_info`, or narrow it to the files
    /// this server reflects.
    #[error(
        "FileDescriptorSet exceeds the decode element-memory budget; the bytes \
         are well-formed, the schema is simply large. Strip source_code_info \
         or reduce the set to the files this server reflects."
    )]
    ElementBudget,
    /// The decoded descriptors did not link into a valid pool (dangling
    /// type reference, duplicate symbol, malformed map entry, ...).
    #[error("invalid descriptor set: {0}")]
    Pool(#[from] PoolError),
    /// The top-level wire structure of the set was malformed (e.g. a
    /// truncated length prefix), so per-file byte ranges could not be
    /// sliced out.
    #[error("malformed FileDescriptorSet framing at byte {offset}")]
    MalformedFraming {
        /// Byte offset of the unreadable tag or length.
        offset: usize,
    },
    /// A file in the set has no `name` field; the reflection protocol
    /// keys every file query by name.
    #[error("FileDescriptorProto at index {index} has no name")]
    UnnamedFile {
        /// Position of the nameless file within the set.
        index: usize,
    },
    /// The framing walk and the message decoder disagreed on how many
    /// files the set contains — the bytes are not a coherent
    /// `FileDescriptorSet`.
    #[error("FileDescriptorSet framing yields {framed} files but decoding yields {decoded}")]
    CountMismatch {
        /// Files found by the top-level framing walk.
        framed: usize,
        /// Files in the decoded `FileDescriptorSet`.
        decoded: usize,
    },
    /// [`add_descriptor_set_bytes`](Reflector::add_descriptor_set_bytes)
    /// was called on a reflector whose pool is shared (adopted via
    /// [`from_descriptor_pool`](Reflector::from_descriptor_pool) with
    /// other outstanding references). Merge sets before sharing, or build
    /// the reflector from bytes.
    #[error("cannot add to a descriptor pool with outstanding references")]
    SharedPool,
}

/// Written out rather than derived with `#[from]`, so that the budget/corrupt
/// split cannot be bypassed. A derived conversion sends every
/// [`buffa::DecodeError`] to [`Decode`](ReflectionError::Decode), so the next
/// decode path added here — reached with `?`, which compiles fine — would
/// report an over-budget set as corruption again, silently undoing the reason
/// [`ElementBudget`](ReflectionError::ElementBudget) exists.
impl From<buffa::DecodeError> for ReflectionError {
    fn from(e: buffa::DecodeError) -> Self {
        match e {
            buffa::DecodeError::ElementMemoryLimitExceeded => Self::ElementBudget,
            other => Self::Decode(other),
        }
    }
}

/// The answer to a single reflection query, protocol-version agnostic.
///
/// `service.rs` maps this onto the generated `v1` / `v1alpha` response
/// messages, which are structurally identical.
pub(crate) enum Answer {
    /// Serialized `FileDescriptorProto`s: the matched file followed by its
    /// transitive import closure.
    Files(Vec<Vec<u8>>),
    /// Extension field numbers registered on `base_type`.
    ExtensionNumbers {
        base_type: String,
        numbers: Vec<i32>,
    },
    /// Fully-qualified names of the advertised services.
    Services(Vec<String>),
    /// The queried entity does not exist; carries the error message.
    NotFound(String),
}

/// Descriptor index serving gRPC server reflection queries.
///
/// Build one from the wire bytes of a `FileDescriptorSet` (typically
/// embedded with `include_bytes!` from
/// `connectrpc_build::Config::emit_descriptor_set` output) or from an
/// existing [`DescriptorPool`], and hand it to
/// [`ReflectionService`](crate::ReflectionService).
///
/// ```no_run
/// use connectrpc_reflection::Reflector;
///
/// // In real code: include_bytes!(concat!(env!("OUT_DIR"), "/app.fds.bin"))
/// # fn descriptor_set_bytes() -> &'static [u8] { &[] }
/// let reflector = Reflector::from_descriptor_set_bytes(descriptor_set_bytes()).unwrap();
/// ```
///
/// # Multiple descriptor sets
///
/// [`add_descriptor_set_bytes`](Self::add_descriptor_set_bytes) merges
/// further sets into the index. Files whose name is already registered
/// are skipped (first registration wins), so sets that each carry their
/// own copy of shared imports — `google/protobuf/*.proto`, common
/// vendored protos — merge cleanly.
pub struct Reflector {
    pool: Arc<DescriptorPool>,
    /// Per-file response payloads keyed by file name: the original input
    /// bytes for sets loaded from wire bytes, a canonical re-encode for
    /// pools adopted via [`from_descriptor_pool`](Self::from_descriptor_pool).
    response_bytes: HashMap<String, Vec<u8>>,
    /// `ListServices` override installed by [`with_services`](Self::with_services);
    /// `None` advertises every service in the pool.
    services_override: Option<Vec<String>>,
}

impl std::fmt::Debug for Reflector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reflector")
            .field("files", &self.pool.files().len())
            .field("services", &self.service_names())
            .finish_non_exhaustive()
    }
}

impl Reflector {
    /// Build a reflector from wire-format `FileDescriptorSet` bytes.
    ///
    /// The set should carry the transitive import closure of the files
    /// it contains (both `protoc --include_imports` and
    /// `Config::emit_descriptor_set` guarantee this); imports missing
    /// from the set are silently omitted from file-closure responses.
    ///
    /// # Errors
    ///
    /// Returns [`ReflectionError`] when the bytes do not decode as a
    /// `FileDescriptorSet`, the descriptors do not link, or a contained
    /// file has no name. A set too large for the decode's element-memory
    /// budget reports [`ElementBudget`](ReflectionError::ElementBudget)
    /// rather than a decode failure.
    pub fn from_descriptor_set_bytes(bytes: &[u8]) -> Result<Self, ReflectionError> {
        let mut reflector = Self {
            pool: Arc::new(DescriptorPool::default()),
            response_bytes: HashMap::new(),
            services_override: None,
        };
        reflector.add_descriptor_set_bytes(bytes)?;
        Ok(reflector)
    }

    /// Serve reflection from an existing [`DescriptorPool`] — typically
    /// the lazily-built `descriptor_pool()` that a buffa-generated package
    /// exposes when reflection codegen is enabled, which spares the build
    /// script a separate `emit_descriptor_set` step.
    ///
    /// The pool must cover **every** proto you want resolvable. Under
    /// `buf generate`'s default per-directory plugin strategy, each
    /// generated package embeds only its own package's closure — set
    /// `strategy: all` on the buffa plugin so any one package's pool
    /// spans the whole codegen run.
    ///
    /// Response payloads are re-encoded from the pool's parsed
    /// `FileDescriptorProto`s. buffa preserves unknown fields, so the
    /// bytes are semantically faithful to the compiler's output but not
    /// guaranteed byte-identical (field ordering is canonicalized). For
    /// byte-exact responses, build from
    /// [`from_descriptor_set_bytes`](Self::from_descriptor_set_bytes).
    ///
    /// A reflector adopting a pool that other references point at — a
    /// generated package's pool always qualifies, since the lazy static
    /// keeps one — cannot be extended with
    /// [`add_descriptor_set_bytes`](Self::add_descriptor_set_bytes);
    /// build from bytes if you need to merge sets.
    ///
    /// # Errors
    ///
    /// Returns [`ReflectionError::UnnamedFile`] when a pool file has no
    /// name.
    pub fn from_descriptor_pool(pool: Arc<DescriptorPool>) -> Result<Self, ReflectionError> {
        let mut response_bytes = HashMap::with_capacity(pool.files().len());
        for (index, fd) in pool.files().iter().enumerate() {
            let name = fd
                .name
                .clone()
                .ok_or(ReflectionError::UnnamedFile { index })?;
            response_bytes
                .entry(name)
                .or_insert_with(|| fd.encode_to_vec());
        }
        Ok(Self {
            pool,
            response_bytes,
            services_override: None,
        })
    }

    /// Merge another wire-format `FileDescriptorSet` into the index.
    ///
    /// Files whose name is already registered are skipped, so shared
    /// imports duplicated across sets do not conflict.
    ///
    /// # Errors
    ///
    /// Returns [`ReflectionError`] when the bytes do not decode or link
    /// (including [`ElementBudget`](ReflectionError::ElementBudget) for a set
    /// over the decode's element-memory budget), a contained file has no
    /// name, or any other reference to the backing pool exists
    /// ([`ReflectionError::SharedPool`]) — which is
    /// always the case for reflectors built with
    /// [`from_descriptor_pool`](Self::from_descriptor_pool) from a
    /// long-lived pool. On error the reflector should be discarded: the
    /// pool may have absorbed part of the failed set.
    pub fn add_descriptor_set_bytes(&mut self, bytes: &[u8]) -> Result<(), ReflectionError> {
        let raw_files = split_descriptor_set(bytes)?;
        let set = FileDescriptorSet::decode_from_slice(bytes)?;
        // The framing walk and buffa's decode are independent parsers of
        // the same bytes; if they disagree on the file count, zipping
        // them would silently pair names with the wrong raw bytes.
        if raw_files.len() != set.file.len() {
            return Err(ReflectionError::CountMismatch {
                framed: raw_files.len(),
                decoded: set.file.len(),
            });
        }
        let mut names = Vec::with_capacity(set.file.len());
        for (index, fd) in set.file.iter().enumerate() {
            names.push(
                fd.name
                    .clone()
                    .ok_or(ReflectionError::UnnamedFile { index })?,
            );
        }

        let pool = Arc::get_mut(&mut self.pool).ok_or(ReflectionError::SharedPool)?;
        pool.add_file_descriptor_set(set)?;

        for (name, raw) in names.into_iter().zip(raw_files) {
            self.response_bytes
                .entry(name)
                .or_insert_with(|| raw.to_vec());
        }
        Ok(())
    }

    /// Restrict the service list advertised by `ListServices` to the
    /// given fully-qualified names, in the given order.
    ///
    /// Like the Go `grpcreflect` `Namer`, this affects only
    /// `ListServices`; files and symbols in the descriptor set stay
    /// resolvable. Use it when the set's import closure carries services
    /// you do not actually mount. Names absent from the descriptor set
    /// are advertised as given — the protocol does not require the list
    /// to be resolvable.
    ///
    /// Calling this more than once **replaces** the previous list; it
    /// does not accumulate (unlike tonic's `with_service_name`).
    #[must_use]
    pub fn with_services<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.services_override = Some(names.into_iter().map(Into::into).collect());
        self
    }

    /// The fully-qualified service names `ListServices` will advertise:
    /// every service in the descriptor pool in registration order plus
    /// the reflection services themselves (matching grpc-go, which
    /// always lists them) — or, verbatim, the override installed by
    /// [`with_services`](Self::with_services).
    #[must_use]
    pub fn service_names(&self) -> Vec<String> {
        self.services_override.clone().unwrap_or_else(|| {
            let mut names: Vec<String> = self
                .pool
                .services()
                .iter()
                .map(|svc| svc.full_name().to_owned())
                .collect();
            for own in self_descriptors().pool.services() {
                if !names.iter().any(|name| name == own.full_name()) {
                    names.push(own.full_name().to_owned());
                }
            }
            names
        })
    }

    /// The descriptor pool backing this reflector, for read-only
    /// inspection (listing files, resolving descriptors).
    #[must_use]
    pub fn pool(&self) -> &DescriptorPool {
        &self.pool
    }

    // ── Queries ─────────────────────────────────────────────────────────
    //
    // Each query consults the user's pool first and the crate's own
    // descriptors second, so every reflector is self-describing: the
    // reflection service can answer queries about `grpc.reflection.*`
    // itself, which schema-free clients (`buf curl`, `grpcurl` without
    // proto files) need to invoke `ServerReflectionInfo`. This matches
    // grpc-go, where the reflection proto is always registered.

    pub(crate) fn file_by_filename(&self, name: &str) -> Answer {
        for source in self.sources() {
            if let Some(fd) = source.pool.file_by_name(name) {
                return Answer::Files(source.closure(fd));
            }
        }
        Answer::NotFound(format!("file {name:?} not found"))
    }

    pub(crate) fn file_containing_symbol(&self, symbol: &str) -> Answer {
        for source in self.sources() {
            if let Some(fd) = source.pool.file_containing_symbol(symbol) {
                return Answer::Files(source.closure(fd));
            }
        }
        Answer::NotFound(format!("symbol {symbol:?} not found"))
    }

    pub(crate) fn file_containing_extension(&self, containing_type: &str, number: i32) -> Answer {
        let not_found = || {
            Answer::NotFound(format!(
                "extension {number} of type {containing_type:?} not found"
            ))
        };
        let Ok(number) = u32::try_from(number) else {
            return not_found();
        };
        for source in self.sources() {
            let Some(extendee) = source.pool.message_index(containing_type) else {
                continue;
            };
            let Some(extension) = source.pool.extension_for(extendee, number) else {
                return not_found();
            };
            return match source.pool.file_containing_symbol(extension.full_name()) {
                Some(fd) => Answer::Files(source.closure(fd)),
                None => not_found(),
            };
        }
        not_found()
    }

    pub(crate) fn all_extension_numbers_of_type(&self, name: &str) -> Answer {
        let normalized = name.strip_prefix('.').unwrap_or(name);
        for source in self.sources() {
            let Some(extendee) = source.pool.message_index(normalized) else {
                continue;
            };
            // `extensions_of` iterates a (extendee, number)-keyed map
            // range, so the numbers come out unique and ascending.
            let numbers = source
                .pool
                .extensions_of(extendee)
                .filter_map(|ext| i32::try_from(ext.field().number()).ok())
                .collect();
            return Answer::ExtensionNumbers {
                base_type: normalized.to_owned(),
                numbers,
            };
        }
        Answer::NotFound(format!("message {normalized:?} not found"))
    }

    pub(crate) fn list_services(&self) -> Answer {
        Answer::Services(self.service_names())
    }

    /// The user's descriptors followed by the crate's own — the lookup
    /// order for every query.
    fn sources(&self) -> [DescriptorSource<'_>; 2] {
        let own = self_descriptors();
        [
            DescriptorSource {
                pool: &self.pool,
                response_bytes: &self.response_bytes,
            },
            DescriptorSource {
                pool: &own.pool,
                response_bytes: &own.response_bytes,
            },
        ]
    }
}

/// One pool plus the per-file response payloads sliced or encoded from
/// its input — either the user's descriptors or the crate's own.
struct DescriptorSource<'a> {
    pool: &'a DescriptorPool,
    response_bytes: &'a HashMap<String, Vec<u8>>,
}

impl DescriptorSource<'_> {
    /// The serialized bytes of `fd` followed by its transitive imports,
    /// deduplicated; after the requested file, the import order is
    /// unspecified (clients assemble a set). Imports missing from the
    /// pool are skipped.
    fn closure(&self, fd: &FileDescriptorProto) -> Vec<Vec<u8>> {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        let mut stack = vec![fd];
        while let Some(fd) = stack.pop() {
            let Some(name) = fd.name.as_deref() else {
                continue;
            };
            if !seen.insert(name) {
                continue;
            }
            if let Some(bytes) = self.response_bytes.get(name) {
                out.push(bytes.clone());
            }
            stack.extend(
                fd.dependency
                    .iter()
                    .filter_map(|dep| self.pool.file_by_name(dep)),
            );
        }
        out
    }
}

/// The crate's own descriptors (`grpc/reflection/{v1,v1alpha}`), built
/// once per process from [`crate::FILE_DESCRIPTOR_SET`] and consulted as
/// the fallback source behind every user pool.
struct SelfDescriptors {
    pool: DescriptorPool,
    response_bytes: HashMap<String, Vec<u8>>,
}

fn self_descriptors() -> &'static SelfDescriptors {
    static SELF: std::sync::OnceLock<SelfDescriptors> = std::sync::OnceLock::new();
    SELF.get_or_init(|| {
        // The bytes are embedded at compile time and validated by this
        // crate's tests, so a failure here is a build defect, not input.
        let bytes = crate::FILE_DESCRIPTOR_SET;
        let raw_files = split_descriptor_set(bytes).expect("embedded descriptor set is framed");
        // This crate's own descriptors, nothing user-controlled, and far
        // below any budget — so the default limits stay.
        let set = FileDescriptorSet::decode_from_slice(bytes)
            .expect("this crate's embedded descriptor set decodes");
        let response_bytes = set
            .file
            .iter()
            .zip(&raw_files)
            .filter_map(|(fd, raw)| Some((fd.name.clone()?, raw.to_vec())))
            .collect();
        let pool = DescriptorPool::new(set).expect("embedded descriptor set links");
        SelfDescriptors {
            pool,
            response_bytes,
        }
    })
}

/// Slice the original per-file `FileDescriptorProto` byte ranges out of a
/// wire-format `FileDescriptorSet` (`repeated FileDescriptorProto file = 1`).
fn split_descriptor_set(bytes: &[u8]) -> Result<Vec<&[u8]>, ReflectionError> {
    let mut files = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let tag_offset = pos;
        let tag = read_varint(bytes, &mut pos)
            .ok_or(ReflectionError::MalformedFraming { offset: tag_offset })?;
        let (field, wire_type) = (tag >> 3, tag & 0x7);
        match wire_type {
            0 => {
                read_varint(bytes, &mut pos)
                    .ok_or(ReflectionError::MalformedFraming { offset: tag_offset })?;
            }
            1 => pos += 8,
            2 => {
                let len = read_varint(bytes, &mut pos)
                    .ok_or(ReflectionError::MalformedFraming { offset: tag_offset })?
                    as usize;
                let end = pos
                    .checked_add(len)
                    .filter(|&end| end <= bytes.len())
                    .ok_or(ReflectionError::MalformedFraming { offset: tag_offset })?;
                if field == 1 {
                    files.push(&bytes[pos..end]);
                }
                pos = end;
            }
            5 => pos += 4,
            _ => return Err(ReflectionError::MalformedFraming { offset: tag_offset }),
        }
        if pos > bytes.len() {
            return Err(ReflectionError::MalformedFraming { offset: tag_offset });
        }
    }
    Ok(files)
}

/// Read one base-128 varint. Assumes canonical encodings (a non-canonical
/// 10th byte has its high bits silently dropped, matching common protobuf
/// decoders); returns `None` on truncation or an unterminated varint.
fn read_varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use buffa_descriptor::generated::descriptor::field_descriptor_proto::{Label, Type};
    use buffa_descriptor::generated::descriptor::{
        DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
        MethodDescriptorProto, OneofDescriptorProto, ServiceDescriptorProto,
    };

    use super::*;

    const SELF_V1: &str = "grpc.reflection.v1.ServerReflection";
    const SELF_V1ALPHA: &str = "grpc.reflection.v1alpha.ServerReflection";

    /// A two-file set: `acme/base.proto` (imported) and `acme/api.proto`
    /// exercising every symbol kind the index covers.
    fn test_set() -> FileDescriptorSet {
        let base = FileDescriptorProto {
            name: Some("acme/base.proto".into()),
            package: Some("acme.base".into()),
            message_type: vec![DescriptorProto {
                name: Some("Shared".into()),
                extension_range: vec![
                    buffa_descriptor::generated::descriptor::descriptor_proto::ExtensionRange {
                        start: Some(100),
                        end: Some(200),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let api = FileDescriptorProto {
            name: Some("acme/api.proto".into()),
            package: Some("acme.api".into()),
            dependency: vec!["acme/base.proto".into()],
            message_type: vec![DescriptorProto {
                name: Some("Request".into()),
                field: vec![FieldDescriptorProto {
                    name: Some("query".into()),
                    number: Some(1),
                    label: Some(Label::LABEL_OPTIONAL),
                    r#type: Some(Type::TYPE_STRING),
                    ..Default::default()
                }],
                oneof_decl: vec![OneofDescriptorProto {
                    name: Some("variant".into()),
                    ..Default::default()
                }],
                nested_type: vec![DescriptorProto {
                    name: Some("Inner".into()),
                    ..Default::default()
                }],
                enum_type: vec![EnumDescriptorProto {
                    name: Some("Kind".into()),
                    value: vec![EnumValueDescriptorProto {
                        name: Some("KIND_UNSPECIFIED".into()),
                        number: Some(0),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            enum_type: vec![EnumDescriptorProto {
                name: Some("Code".into()),
                value: vec![EnumValueDescriptorProto {
                    name: Some("CODE_OK".into()),
                    number: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            service: vec![ServiceDescriptorProto {
                name: Some("Search".into()),
                method: vec![MethodDescriptorProto {
                    name: Some("Query".into()),
                    input_type: Some(".acme.api.Request".into()),
                    output_type: Some(".acme.api.Request".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            extension: vec![FieldDescriptorProto {
                name: Some("tag".into()),
                number: Some(150),
                label: Some(Label::LABEL_OPTIONAL),
                r#type: Some(Type::TYPE_INT32),
                extendee: Some(".acme.base.Shared".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        FileDescriptorSet {
            file: vec![base, api],
            ..Default::default()
        }
    }

    fn test_reflector() -> Reflector {
        Reflector::from_descriptor_set_bytes(&test_set().encode_to_vec()).unwrap()
    }

    fn files(answer: Answer) -> Vec<Vec<u8>> {
        match answer {
            Answer::Files(files) => files,
            _ => panic!("expected Answer::Files"),
        }
    }

    fn assert_not_found(answer: &Answer) {
        assert!(matches!(answer, Answer::NotFound(_)));
    }

    #[test]
    fn file_by_filename_returns_raw_bytes_and_closure() {
        let set = test_set();
        let reflector = test_reflector();

        let got = files(reflector.file_by_filename("acme/api.proto"));
        // api.proto plus its import.
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], set.file[1].encode_to_vec());
        assert_eq!(got[1], set.file[0].encode_to_vec());

        // The import alone has no dependencies.
        let got = files(reflector.file_by_filename("acme/base.proto"));
        assert_eq!(got.len(), 1);

        assert_not_found(&reflector.file_by_filename("nope.proto"));
    }

    #[test]
    fn raw_bytes_survive_unknown_fields() {
        // Hand-frame a set whose file payload carries an unknown field
        // (number 12345, varint 1) that a re-encode might reorder or drop.
        // The bytes-built reflector must return it byte-for-byte.
        let mut file = test_set().file[0].encode_to_vec();
        let unknown = [0xc8, 0x83, 0x06, 0x01]; // tag 12345<<3|0, value 1
        file.extend_from_slice(&unknown);
        let mut set_bytes = vec![0x0a, u8::try_from(file.len()).unwrap()];
        set_bytes.extend_from_slice(&file);

        let reflector = Reflector::from_descriptor_set_bytes(&set_bytes).unwrap();
        let got = files(reflector.file_by_filename("acme/base.proto"));
        assert_eq!(got, vec![file]);
    }

    #[test]
    fn symbol_lookup_covers_every_kind() {
        let reflector = test_reflector();
        for symbol in [
            "acme.api.Request",
            "acme.api.Request.query",
            "acme.api.Request.variant",
            "acme.api.Request.Inner",
            "acme.api.Request.Kind",
            "acme.api.Request.KIND_UNSPECIFIED", // enum values scope to the parent
            "acme.api.Code",
            "acme.api.CODE_OK",
            "acme.api.Search",
            "acme.api.Search.Query",
            "acme.api.tag",
            ".acme.api.Request", // leading dot tolerated
        ] {
            let got = files(reflector.file_containing_symbol(symbol));
            assert_eq!(got.len(), 2, "symbol {symbol}");
        }
        // Enum values do NOT live inside the enum's own scope.
        assert_not_found(&reflector.file_containing_symbol("acme.api.Code.CODE_OK"));
        // Packages are not symbols.
        assert_not_found(&reflector.file_containing_symbol("acme.api"));
        assert_not_found(&reflector.file_containing_symbol("acme.api.Missing"));
    }

    #[test]
    fn extension_queries() {
        let reflector = test_reflector();

        let got = files(reflector.file_containing_extension("acme.base.Shared", 150));
        assert_eq!(got.len(), 2); // api.proto declares it, base.proto imported

        assert_not_found(&reflector.file_containing_extension("acme.base.Shared", 151));
        assert_not_found(&reflector.file_containing_extension("acme.base.Shared", -1));
        assert_not_found(&reflector.file_containing_extension("acme.api.Request", 150));

        match reflector.all_extension_numbers_of_type("acme.base.Shared") {
            Answer::ExtensionNumbers { base_type, numbers } => {
                assert_eq!(base_type, "acme.base.Shared");
                assert_eq!(numbers, vec![150]);
            }
            _ => panic!("expected extension numbers"),
        }
        // A known message with no extensions answers with an empty list,
        // not an error.
        match reflector.all_extension_numbers_of_type("acme.api.Request") {
            Answer::ExtensionNumbers { numbers, .. } => assert!(numbers.is_empty()),
            _ => panic!("expected extension numbers"),
        }
        // Unknown types — and non-message symbols like services — are
        // not extendable.
        assert_not_found(&reflector.all_extension_numbers_of_type("acme.Missing"));
        assert_not_found(&reflector.all_extension_numbers_of_type("acme.api.Search"));
    }

    #[test]
    fn list_services() {
        match test_reflector().list_services() {
            Answer::Services(names) => {
                assert_eq!(names, vec!["acme.api.Search", SELF_V1, SELF_V1ALPHA]);
            }
            _ => panic!("expected services"),
        }
    }

    #[test]
    fn with_services_overrides_advertised_list_only() {
        let reflector = test_reflector().with_services(["acme.api.Curated"]);
        assert_eq!(reflector.service_names(), ["acme.api.Curated"]);
        match reflector.list_services() {
            Answer::Services(names) => assert_eq!(names, vec!["acme.api.Curated"]),
            _ => panic!("expected services"),
        }
        // Symbols stay resolvable, including the de-listed service.
        let got = files(reflector.file_containing_symbol("acme.api.Search"));
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn merging_sets_skips_duplicate_files() {
        let mut reflector = test_reflector();
        // A second set re-shipping base.proto (different content — would
        // clobber the symbol index if not skipped) plus a new file.
        let second = FileDescriptorSet {
            file: vec![
                FileDescriptorProto {
                    name: Some("acme/base.proto".into()),
                    package: Some("acme.other".into()),
                    ..Default::default()
                },
                FileDescriptorProto {
                    name: Some("acme/extra.proto".into()),
                    package: Some("acme.extra".into()),
                    service: vec![ServiceDescriptorProto {
                        name: Some("Extra".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        reflector
            .add_descriptor_set_bytes(&second.encode_to_vec())
            .unwrap();

        // First registration of base.proto won: its message survives and
        // the replacement package was never indexed.
        assert!(matches!(
            reflector.file_containing_symbol("acme.base.Shared"),
            Answer::Files(_)
        ));
        match reflector.list_services() {
            Answer::Services(names) => {
                assert_eq!(
                    names,
                    vec!["acme.api.Search", "acme.extra.Extra", SELF_V1, SELF_V1ALPHA]
                );
            }
            _ => panic!("expected services"),
        }
    }

    #[test]
    fn from_descriptor_pool_serves_reencoded_files() {
        let set = test_set();
        let pool = Arc::new(DescriptorPool::new(set.clone()).unwrap());
        let reflector = Reflector::from_descriptor_pool(Arc::clone(&pool)).unwrap();

        // Same queries work; payloads decode to the same descriptors
        // (byte-exactness is only guaranteed for the bytes-built path).
        let got = files(reflector.file_containing_symbol("acme.api.Search"));
        assert_eq!(got.len(), 2);
        let decoded = FileDescriptorProto::decode_from_slice(&got[0]).unwrap();
        assert_eq!(decoded, set.file[1]);

        match reflector.list_services() {
            Answer::Services(names) => {
                assert_eq!(names, vec!["acme.api.Search", SELF_V1, SELF_V1ALPHA]);
            }
            _ => panic!("expected services"),
        }

        // The pool is shared (we still hold `pool`), so merging more
        // bytes into it must refuse rather than mutate shared state.
        let mut reflector = reflector;
        let err = reflector
            .add_descriptor_set_bytes(&FileDescriptorSet::default().encode_to_vec())
            .unwrap_err();
        assert!(matches!(err, ReflectionError::SharedPool));
    }

    /// The runtime path deliberately stays bounded, so an oversized set must
    /// reach the caller as `ElementBudget` — the variant that says the bytes
    /// are fine — and not as a decode failure that reads like corruption.
    #[test]
    fn an_oversized_set_reports_the_element_budget_not_a_decode_failure() {
        // Charged per element on struct size, so many small files exceed the
        // budget while staying small on the wire. The count is derived rather
        // than written as a literal: the charge tracks
        // `size_of::<FileDescriptorProto>()`, so a literal silently stops
        // exceeding the budget whenever that struct shrinks, and the test
        // would then assert a rejection that no longer happens. Same
        // derivation as `connectrpc::test_budget` and connectrpc-build's
        // `over_default_budget_set`, repeated because a `#[cfg(test)]` helper
        // cannot cross a crate boundary; that module carries the full
        // rationale.
        let per_element = std::mem::size_of::<FileDescriptorProto>();
        let n = (buffa::DEFAULT_ELEMENT_MEMORY_LIMIT / per_element) * 5 / 4;
        let set = FileDescriptorSet {
            file: (0..n)
                .map(|i| FileDescriptorProto {
                    name: Some(format!("f{i}.proto")),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let bytes = buffa::Message::encode_to_vec(&set);

        let err = Reflector::from_descriptor_set_bytes(&bytes).unwrap_err();
        assert!(
            matches!(err, ReflectionError::ElementBudget),
            "expected ElementBudget, got {err:?}"
        );
        // The message has to say the schema is large, not that the bytes are
        // bad — that distinction is the whole reason the variant exists.
        let rendered = err.to_string();
        assert!(
            rendered.contains("well-formed"),
            "message should absolve the bytes, got {rendered:?}"
        );
    }

    #[test]
    fn construction_errors() {
        // Truncated length prefix.
        let err = Reflector::from_descriptor_set_bytes(&[0x0a, 0xff]).unwrap_err();
        assert!(matches!(err, ReflectionError::MalformedFraming { .. }));

        // A file without a name.
        let set = FileDescriptorSet {
            file: vec![FileDescriptorProto::default()],
            ..Default::default()
        };
        let err = Reflector::from_descriptor_set_bytes(&set.encode_to_vec()).unwrap_err();
        assert!(matches!(err, ReflectionError::UnnamedFile { index: 0 }));

        // An empty set is valid and answers everything with not-found.
        let reflector = Reflector::from_descriptor_set_bytes(&[]).unwrap();
        assert_not_found(&reflector.file_by_filename("x.proto"));
        match reflector.list_services() {
            // Even an empty set self-lists the reflection services.
            Answer::Services(names) => assert_eq!(names, vec![SELF_V1, SELF_V1ALPHA]),
            _ => panic!("expected services"),
        }
    }
}
