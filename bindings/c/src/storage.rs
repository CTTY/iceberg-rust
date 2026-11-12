// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! File IO supplied by the engine through a C vtable.
//!
//! iceberg-rust reads every file (metadata, manifests, data) through its
//! [`Storage`] trait. This module implements that trait on top of a small
//! table of C function pointers, [`IcebergFileSystemVTable`], so that the
//! engine's own file system does all IO: path schemes, credentials, caches and
//! remote protocols stay on the engine side, and this crate links no IO
//! library of its own.
//!
//! The vtable entries are synchronous. They are invoked from tokio's blocking
//! pool via `spawn_blocking`, never on an async worker, with the exception of
//! `close`, which is cheap by contract and runs inline when a reader is
//! dropped.

use std::borrow::Cow;
use std::ffi::c_void;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};

use crate::strings::{AllocateStringFn, StringSlice};

/// Outcome of a file system callback.
///
/// Only the three failure classes that change behaviour are distinguished;
/// the human-readable detail travels separately through
/// [`IcebergFileSystemVTable::error_message`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergIoStatus {
    /// The call succeeded and its output parameters are set.
    Ok = 0,
    /// The path does not exist.
    NotFound = 1,
    /// The engine was denied access to the path.
    PermissionDenied = 2,
    /// Any other failure.
    Other = 3,
}

/// Opens `path` for reading.
///
/// Arguments: `ctx`, the UTF-8 path, and an out parameter receiving the
/// engine's file handle, which must be non-null on `Ok`.
pub type IcebergFsOpenFn =
    extern "C" fn(*mut c_void, StringSlice, *mut *mut c_void) -> IcebergIoStatus;

/// Returns the size in bytes of an open file.
///
/// Arguments: `ctx`, the file handle from `open`, and an out parameter for the
/// size.
pub type IcebergFsFileSizeFn = extern "C" fn(*mut c_void, *mut c_void, *mut u64) -> IcebergIoStatus;

/// Reads from an open file at an absolute offset.
///
/// Arguments: `ctx`, the file handle, the offset, the destination buffer, the
/// buffer length, and an out parameter receiving the number of bytes actually
/// read. A short read is allowed and is retried by the caller; zero bytes
/// read means end of file.
pub type IcebergFsReadAtFn =
    extern "C" fn(*mut c_void, *mut c_void, u64, *mut u8, u64, *mut u64) -> IcebergIoStatus;

/// Closes an open file. Arguments: `ctx` and the file handle. Infallible.
pub type IcebergFsCloseFn = extern "C" fn(*mut c_void, *mut c_void);

/// Tests whether `path` exists.
///
/// Arguments: `ctx`, the UTF-8 path, and an out parameter for the answer.
pub type IcebergFsExistsFn = extern "C" fn(*mut c_void, StringSlice, *mut bool) -> IcebergIoStatus;

/// Returns the message of the most recent failed callback on the calling
/// thread through the given string allocator, or null when there is none.
pub type IcebergFsErrorMessageFn = extern "C" fn(*mut c_void, AllocateStringFn) -> *mut c_void;

/// Releases `ctx`. Called exactly once, after the last other call.
pub type IcebergFsReleaseFn = extern "C" fn(*mut c_void);

/// The engine's file system, as a context pointer plus callbacks.
///
/// # Contract
///
/// **Threads.** Any entry may be called from any thread, including threads
/// the engine has never seen (tokio's blocking pool). Calls that take a file
/// handle are never made concurrently for the *same* handle; different
/// handles, as well as `open`, `exists` and `error_message`, may be called
/// concurrently.
///
/// **Errors.** When an entry returns a status other than
/// [`IcebergIoStatus::Ok`], the caller immediately invokes `error_message`
/// on the *same thread*, before any other vtable call on that thread, to
/// fetch the detail text. An implementation can therefore keep the last
/// message in thread-local storage. `error_message` returns null when no
/// message is available; the pointer it returns comes from the allocator it
/// was given and is owned by the caller.
///
/// **Lifetime.** `ctx` stays valid until `release(ctx)` is called, which
/// happens exactly once, after every file handle has been closed. File
/// handles returned by `open` are closed exactly once with `close`.
///
/// **`close` is cheap.** It runs inline when a reader is dropped, possibly on
/// an async worker thread, so it must not block on network IO or do more than
/// local bookkeeping and releasing a descriptor.
///
/// **No unwinding.** Callbacks must not throw or unwind across the boundary;
/// failures are reported through the status code.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IcebergFileSystemVTable {
    /// Opaque engine state handed back as the first argument of every entry.
    pub ctx: *mut c_void,
    /// Opens a file for reading.
    pub open: IcebergFsOpenFn,
    /// Size of an open file.
    pub file_size: IcebergFsFileSizeFn,
    /// Positional read.
    pub read_at: IcebergFsReadAtFn,
    /// Closes a file handle.
    pub close: IcebergFsCloseFn,
    /// Existence check.
    pub exists: IcebergFsExistsFn,
    /// Last error text for the calling thread.
    pub error_message: IcebergFsErrorMessageFn,
    /// Releases `ctx`.
    pub release: IcebergFsReleaseFn,
}

/// A file handle returned by the engine's `open`.
#[derive(Debug)]
struct RawFile(*mut c_void);

// SAFETY: the vtable contract allows a file handle to be used from any thread
// as long as no two calls for the same handle overlap; every use goes through
// `OpenFile`, which serialises them behind a mutex. Rust never dereferences
// the pointer.
unsafe impl Send for RawFile {}

/// Copies a [`StringSlice`] into a boxed `String`; the Rust-side allocator
/// handed to `error_message`.
extern "C" fn alloc_rust_string(slice: StringSlice) -> *mut c_void {
    ffi_guard!(|_msg| std::ptr::null_mut(), {
        // SAFETY: the slice is valid for the duration of the callback.
        let text = match unsafe { slice.as_str() } {
            Ok(s) => s.to_owned(),
            Err(_) => String::from("<invalid utf-8>"),
        };
        Box::into_raw(Box::new(text)).cast::<c_void>()
    })
}

/// Owning wrapper around a vtable; calls `release` when dropped.
pub(crate) struct FsHandle {
    vtable: IcebergFileSystemVTable,
}

// SAFETY: the vtable contract (see `IcebergFileSystemVTable`) promises that
// `ctx` and every entry may be used from any thread, with the documented
// per-handle exclusivity that `OpenFile` enforces. Rust never dereferences
// `ctx`.
unsafe impl Send for FsHandle {}
unsafe impl Sync for FsHandle {}

impl std::fmt::Debug for FsHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FsHandle({:p})", self.vtable.ctx)
    }
}

impl Drop for FsHandle {
    fn drop(&mut self) {
        (self.vtable.release)(self.vtable.ctx);
    }
}

impl FsHandle {
    /// Takes ownership of `vtable.ctx`; `release` runs when the handle drops.
    pub(crate) fn new(vtable: IcebergFileSystemVTable) -> Self {
        Self { vtable }
    }

    fn open(&self, path: &str) -> Result<RawFile> {
        let mut raw: *mut c_void = std::ptr::null_mut();
        let status = (self.vtable.open)(self.vtable.ctx, StringSlice::borrowed(path), &mut raw);
        self.check(status, "open", path)?;
        if raw.is_null() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("open returned no file handle for {path}"),
            ));
        }
        Ok(RawFile(raw))
    }

    fn file_size(&self, file: &RawFile, path: &str) -> Result<u64> {
        let mut size: u64 = 0;
        let status = (self.vtable.file_size)(self.vtable.ctx, file.0, &mut size);
        self.check(status, "file_size", path)?;
        Ok(size)
    }

    /// Fills `buf` from `offset`, retrying short reads.
    fn read_exact_at(&self, file: &RawFile, offset: u64, buf: &mut [u8], path: &str) -> Result<()> {
        let mut done: usize = 0;
        while done < buf.len() {
            let remaining = &mut buf[done..];
            let mut out_read: u64 = 0;
            let status = (self.vtable.read_at)(
                self.vtable.ctx,
                file.0,
                offset + done as u64,
                remaining.as_mut_ptr(),
                remaining.len() as u64,
                &mut out_read,
            );
            self.check(status, "read_at", path)?;
            if out_read == 0 {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "unexpected end of file: {path}: wanted {} bytes at offset {}, got {done}",
                        buf.len(),
                        offset
                    ),
                ));
            }
            let read = usize::try_from(out_read).unwrap_or(usize::MAX);
            if read > remaining.len() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "read_at reported {read} bytes for a {} byte buffer: {path}",
                        remaining.len()
                    ),
                ));
            }
            done += read;
        }
        Ok(())
    }

    fn exists(&self, path: &str) -> Result<bool> {
        let mut exists = false;
        let status =
            (self.vtable.exists)(self.vtable.ctx, StringSlice::borrowed(path), &mut exists);
        self.check(status, "exists", path)?;
        Ok(exists)
    }

    fn close(&self, file: RawFile) {
        (self.vtable.close)(self.vtable.ctx, file.0);
    }

    /// Fetches and clears the calling thread's last error text.
    fn last_error(&self) -> Option<String> {
        let ptr = (self.vtable.error_message)(self.vtable.ctx, alloc_rust_string);
        if ptr.is_null() {
            return None;
        }
        // SAFETY: the engine returns exactly the pointer `alloc_rust_string`
        // produced, which came from `Box::into_raw` of a `String`.
        Some(*unsafe { Box::from_raw(ptr.cast::<String>()) })
    }

    /// Turns a non-`Ok` status into an error, pulling in the engine's detail.
    fn check(&self, status: IcebergIoStatus, op: &str, path: &str) -> Result<()> {
        if status == IcebergIoStatus::Ok {
            return Ok(());
        }
        let detail = self.last_error().unwrap_or_default();
        Err(match status {
            IcebergIoStatus::NotFound => Error::new(
                ErrorKind::DataInvalid,
                format!("file does not exist: {path}: {detail}"),
            ),
            IcebergIoStatus::PermissionDenied => Error::new(
                ErrorKind::Unexpected,
                format!("permission denied: {op} {path}: {detail}"),
            ),
            IcebergIoStatus::Other | IcebergIoStatus::Ok => Error::new(
                ErrorKind::Unexpected,
                format!("{op} failed: {path}: {detail}"),
            ),
        })
    }
}

/// Rewrites paths under one prefix to another.
///
/// Used when a table has been moved and its metadata still records the old
/// location. Both prefixes have any trailing `/` stripped; a path is
/// rewritten only when the prefix is followed by `/` or ends the path.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Relocation {
    from: String,
    to: String,
}

impl Relocation {
    fn new(from: &str, to: &str) -> Self {
        Self {
            from: from.trim_end_matches('/').to_owned(),
            to: to.trim_end_matches('/').to_owned(),
        }
    }

    fn rewrite<'a>(&self, path: &'a str) -> Cow<'a, str> {
        match path.strip_prefix(self.from.as_str()) {
            Some(rest) if rest.is_empty() || rest.starts_with('/') => {
                Cow::Owned(format!("{}{rest}", self.to))
            }
            _ => Cow::Borrowed(path),
        }
    }
}

/// Runs a synchronous vtable call on tokio's blocking pool.
async fn run_blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let handle = tokio::runtime::Handle::try_current().map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            format!("no tokio runtime for file system call: {e}"),
        )
    })?;
    handle.spawn_blocking(f).await.map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            format!("file system task failed: {e}"),
        )
    })?
}

/// An open engine file; closed inline when dropped.
#[derive(Debug)]
struct OpenFile {
    fs: Arc<FsHandle>,
    path: String,
    raw: Mutex<Option<RawFile>>,
}

impl OpenFile {
    fn open(fs: Arc<FsHandle>, path: String) -> Result<Self> {
        let raw = fs.open(&path)?;
        Ok(Self {
            fs,
            path,
            raw: Mutex::new(Some(raw)),
        })
    }

    fn with_raw<T>(&self, f: impl FnOnce(&RawFile) -> Result<T>) -> Result<T> {
        let guard = self.raw.lock().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                format!("file handle lock poisoned: {}", self.path),
            )
        })?;
        match guard.as_ref() {
            Some(raw) => f(raw),
            None => Err(Error::new(
                ErrorKind::Unexpected,
                format!("file handle already closed: {}", self.path),
            )),
        }
    }

    fn size(&self) -> Result<u64> {
        self.with_raw(|raw| self.fs.file_size(raw, &self.path))
    }

    fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }
        let mut buf = vec![0u8; len];
        self.with_raw(|raw| self.fs.read_exact_at(raw, offset, &mut buf, &self.path))?;
        Ok(Bytes::from(buf))
    }

    fn read_all(&self) -> Result<Bytes> {
        let size = self.size()?;
        let len = usize::try_from(size).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "file too large to read into memory: {}: {size} bytes",
                    self.path
                ),
            )
        })?;
        self.read_at(0, len)
    }
}

impl Drop for OpenFile {
    fn drop(&mut self) {
        let raw = match self.raw.get_mut() {
            Ok(slot) => slot.take(),
            Err(poisoned) => poisoned.into_inner().take(),
        };
        if let Some(raw) = raw {
            self.fs.close(raw);
        }
    }
}

/// Read-only [`Storage`] over an [`IcebergFileSystemVTable`].
///
/// Cheap to clone: clones share the vtable context, which is released when
/// the last clone (and the last reader opened through it) is dropped.
#[derive(Clone, Debug)]
pub struct CallbackStorage {
    fs: Arc<FsHandle>,
    relocate: Option<Relocation>,
}

impl CallbackStorage {
    /// Wraps a vtable. `relocate` is an optional `(from, to)` prefix pair
    /// applied to every path before it reaches the engine.
    ///
    /// Takes ownership of `vtable.ctx`: `release` is called exactly once when
    /// the storage and everything derived from it have been dropped.
    pub fn new(vtable: IcebergFileSystemVTable, relocate: Option<(String, String)>) -> Self {
        Self::from_fs(FsHandle::new(vtable), relocate)
    }

    /// Like [`CallbackStorage::new`] for a vtable that is already owned by an
    /// [`FsHandle`], so a caller can take ownership of `ctx` before doing
    /// anything that may fail.
    pub(crate) fn from_fs(fs: FsHandle, relocate: Option<(String, String)>) -> Self {
        Self {
            fs: Arc::new(fs),
            relocate: relocate.map(|(from, to)| Relocation::new(&from, &to)),
        }
    }

    fn resolve(&self, path: &str) -> String {
        match &self.relocate {
            Some(relocation) => relocation.rewrite(path).into_owned(),
            None => path.to_owned(),
        }
    }

    async fn open(&self, path: &str) -> Result<OpenFile> {
        let fs = Arc::clone(&self.fs);
        let path = self.resolve(path);
        run_blocking(move || OpenFile::open(fs, path)).await
    }

    fn unsupported(op: &str) -> Error {
        Error::new(
            ErrorKind::FeatureUnsupported,
            format!("engine file system is read-only: {op} is not supported"),
        )
    }
}

impl serde::Serialize for CallbackStorage {
    fn serialize<S: serde::Serializer>(
        &self,
        _serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "CallbackStorage wraps engine callbacks and cannot be serialised",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for CallbackStorage {
    fn deserialize<D: serde::Deserializer<'de>>(
        _deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "CallbackStorage wraps engine callbacks and cannot be deserialised",
        ))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for CallbackStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let fs = Arc::clone(&self.fs);
        let path = self.resolve(path);
        run_blocking(move || fs.exists(&path)).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let fs = Arc::clone(&self.fs);
        let path = self.resolve(path);
        run_blocking(move || {
            let file = OpenFile::open(fs, path)?;
            Ok(FileMetadata { size: file.size()? })
        })
        .await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let fs = Arc::clone(&self.fs);
        let path = self.resolve(path);
        run_blocking(move || OpenFile::open(fs, path)?.read_all()).await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let file = self.open(path).await?;
        Ok(Box::new(CallbackFileRead {
            file: Arc::new(file),
        }))
    }

    async fn write(&self, _path: &str, _bs: Bytes) -> Result<()> {
        Err(Self::unsupported("write"))
    }

    async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
        Err(Self::unsupported("writer"))
    }

    async fn delete(&self, _path: &str) -> Result<()> {
        Err(Self::unsupported("delete"))
    }

    async fn delete_prefix(&self, _path: &str) -> Result<()> {
        Err(Self::unsupported("delete_prefix"))
    }

    async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> Result<()> {
        Err(Self::unsupported("delete_stream"))
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        // The logical path is kept: relocation is applied per call, so
        // `InputFile::location()` still reports what the metadata said.
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, _path: &str) -> Result<OutputFile> {
        Err(Self::unsupported("new_output"))
    }
}

/// Positional reader over an open engine file.
///
/// Reads run on the blocking pool and are serialised per file. The file is
/// closed inline when the last reference (the reader itself or an in-flight
/// read task) is dropped.
#[derive(Debug)]
pub struct CallbackFileRead {
    file: Arc<OpenFile>,
}

#[async_trait]
impl FileRead for CallbackFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.end < range.start {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "invalid read range {}..{} for {}",
                    range.start, range.end, self.file.path
                ),
            ));
        }
        let len = usize::try_from(range.end - range.start).map_err(|_| {
            Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "read range {}..{} too large for {}",
                    range.start, range.end, self.file.path
                ),
            )
        })?;
        if len == 0 {
            return Ok(Bytes::new());
        }
        let file = Arc::clone(&self.file);
        run_blocking(move || file.read_at(range.start, len)).await
    }
}

/// [`StorageFactory`] that always hands out the same [`CallbackStorage`].
#[derive(Debug)]
pub struct CallbackStorageFactory(Arc<CallbackStorage>);

impl serde::Serialize for CallbackStorageFactory {
    fn serialize<S: serde::Serializer>(
        &self,
        _serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "CallbackStorageFactory wraps engine callbacks and cannot be serialised",
        ))
    }
}

impl<'de> serde::Deserialize<'de> for CallbackStorageFactory {
    fn deserialize<D: serde::Deserializer<'de>>(
        _deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Err(serde::de::Error::custom(
            "CallbackStorageFactory wraps engine callbacks and cannot be deserialised",
        ))
    }
}

#[typetag::serde]
impl StorageFactory for CallbackStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(self.0.clone())
    }
}

/// Builds a [`FileIO`] whose every operation goes through `storage`.
pub fn file_io(storage: CallbackStorage) -> FileIO {
    FileIOBuilder::new(Arc::new(CallbackStorageFactory(Arc::new(storage)))).build()
}

#[cfg(test)]
pub(crate) mod test_support {
    //! A vtable over `std::fs`, so the storage can be exercised without an
    //! engine. Mirrors the contract an engine must honour: thread-local last
    //! error, boxed file handles, `release` frees the context.

    use std::cell::RefCell;
    use std::fs::File;
    use std::io;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Observations shared between a test and its vtable.
    #[derive(Debug, Default)]
    pub(crate) struct Counters {
        /// Successful `open` calls.
        pub(crate) opens: AtomicUsize,
        /// `close` calls.
        pub(crate) closes: AtomicUsize,
        /// `read_at` calls.
        pub(crate) reads: AtomicUsize,
        /// Whether `release` has run.
        pub(crate) released: AtomicBool,
        /// Paths passed to `open`, in order.
        pub(crate) opened_paths: Mutex<Vec<String>>,
    }

    struct StdFsVTable {
        counters: Arc<Counters>,
    }

    thread_local! {
        static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
    }

    fn fail(e: &io::Error) -> IcebergIoStatus {
        LAST_ERROR.with(|slot| *slot.borrow_mut() = Some(e.to_string()));
        match e.kind() {
            io::ErrorKind::NotFound => IcebergIoStatus::NotFound,
            io::ErrorKind::PermissionDenied => IcebergIoStatus::PermissionDenied,
            _ => IcebergIoStatus::Other,
        }
    }

    fn path_of(slice: StringSlice) -> std::result::Result<&'static str, IcebergIoStatus> {
        // SAFETY: the caller keeps the slice alive for the duration of the
        // callback; the 'static is a test-only shortcut and the reference is
        // not retained past the call.
        unsafe { slice.as_str() }
            .map_err(|e| fail(&io::Error::new(io::ErrorKind::InvalidData, e.to_string())))
    }

    /// # Safety
    /// `ctx` must be the pointer `new_std_fs_vtable` produced.
    unsafe fn vtable_of<'a>(ctx: *mut c_void) -> &'a StdFsVTable {
        // SAFETY: forwarded to the caller.
        unsafe { &*ctx.cast::<StdFsVTable>() }
    }

    /// # Safety
    /// `file` must be a pointer produced by `std_open` and not yet closed.
    unsafe fn file_of<'a>(file: *mut c_void) -> &'a File {
        // SAFETY: forwarded to the caller.
        unsafe { &*file.cast::<File>() }
    }

    extern "C" fn std_open(
        ctx: *mut c_void,
        path: StringSlice,
        out: *mut *mut c_void,
    ) -> IcebergIoStatus {
        // SAFETY: `ctx` comes from `new_std_fs_vtable`.
        let vt = unsafe { vtable_of(ctx) };
        let path = match path_of(path) {
            Ok(p) => p,
            Err(status) => return status,
        };
        match File::open(path) {
            Ok(file) => {
                vt.counters.opens.fetch_add(1, Ordering::SeqCst);
                if let Ok(mut paths) = vt.counters.opened_paths.lock() {
                    paths.push(path.to_owned());
                }
                // SAFETY: `out` is a valid out parameter supplied by the caller.
                unsafe { *out = Box::into_raw(Box::new(file)).cast::<c_void>() };
                IcebergIoStatus::Ok
            }
            Err(e) => fail(&e),
        }
    }

    extern "C" fn std_file_size(
        _ctx: *mut c_void,
        file: *mut c_void,
        out: *mut u64,
    ) -> IcebergIoStatus {
        // SAFETY: `file` comes from `std_open`.
        let file = unsafe { file_of(file) };
        match file.metadata() {
            Ok(m) => {
                // SAFETY: `out` is a valid out parameter supplied by the caller.
                unsafe { *out = m.len() };
                IcebergIoStatus::Ok
            }
            Err(e) => fail(&e),
        }
    }

    extern "C" fn std_read_at(
        ctx: *mut c_void,
        file: *mut c_void,
        offset: u64,
        buf: *mut u8,
        len: u64,
        out_read: *mut u64,
    ) -> IcebergIoStatus {
        // SAFETY: `ctx` comes from `new_std_fs_vtable`; `file` from `std_open`.
        let vt = unsafe { vtable_of(ctx) };
        let file = unsafe { file_of(file) };
        vt.counters.reads.fetch_add(1, Ordering::SeqCst);
        // SAFETY: the caller provides `len` writable bytes at `buf`.
        let buf = unsafe { std::slice::from_raw_parts_mut(buf, len as usize) };
        let result = {
            #[cfg(unix)]
            {
                std::os::unix::fs::FileExt::read_at(file, buf, offset)
            }
            #[cfg(windows)]
            {
                std::os::windows::fs::FileExt::seek_read(file, buf, offset)
            }
        };
        match result {
            Ok(n) => {
                // SAFETY: `out_read` is a valid out parameter supplied by the caller.
                unsafe { *out_read = n as u64 };
                IcebergIoStatus::Ok
            }
            Err(e) => fail(&e),
        }
    }

    extern "C" fn std_close(ctx: *mut c_void, file: *mut c_void) {
        // SAFETY: `ctx` comes from `new_std_fs_vtable`; `file` from `std_open`
        // and is closed exactly once by contract.
        let vt = unsafe { vtable_of(ctx) };
        drop(unsafe { Box::from_raw(file.cast::<File>()) });
        vt.counters.closes.fetch_add(1, Ordering::SeqCst);
    }

    extern "C" fn std_exists(
        _ctx: *mut c_void,
        path: StringSlice,
        out: *mut bool,
    ) -> IcebergIoStatus {
        let path = match path_of(path) {
            Ok(p) => p,
            Err(status) => return status,
        };
        // SAFETY: `out` is a valid out parameter supplied by the caller.
        unsafe { *out = Path::new(path).exists() };
        IcebergIoStatus::Ok
    }

    extern "C" fn std_error_message(_ctx: *mut c_void, alloc: AllocateStringFn) -> *mut c_void {
        match LAST_ERROR.with(|slot| slot.borrow_mut().take()) {
            Some(text) => alloc(StringSlice::borrowed(&text)),
            None => std::ptr::null_mut(),
        }
    }

    extern "C" fn std_release(ctx: *mut c_void) {
        // SAFETY: `ctx` comes from `new_std_fs_vtable` and is released once.
        let vt = unsafe { Box::from_raw(ctx.cast::<StdFsVTable>()) };
        vt.counters.released.store(true, Ordering::SeqCst);
    }

    /// Builds a vtable over `std::fs` together with its observation counters.
    pub(crate) fn new_std_fs_vtable() -> (IcebergFileSystemVTable, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
        let ctx = Box::into_raw(Box::new(StdFsVTable {
            counters: Arc::clone(&counters),
        }))
        .cast::<c_void>();
        let vtable = IcebergFileSystemVTable {
            ctx,
            open: std_open,
            file_size: std_file_size,
            read_at: std_read_at,
            close: std_close,
            exists: std_exists,
            error_message: std_error_message,
            release: std_release,
        };
        (vtable, counters)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::test_support::{Counters, new_std_fs_vtable};
    use super::*;

    const CONTENT: &[u8] = b"hello, iceberg world";

    /// A fresh directory under the system temp dir, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "iceberg-ffi-storage-{}-{}-{name}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::SeqCst)
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn file(&self, name: &str, content: &[u8]) -> String {
            let path = self.0.join(name);
            std::fs::write(&path, content).expect("write temp file");
            path.to_str().expect("utf-8 temp path").to_owned()
        }

        fn path(&self, name: &str) -> String {
            self.0
                .join(name)
                .to_str()
                .expect("utf-8 temp path")
                .to_owned()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("tokio runtime")
    }

    fn storage(relocate: Option<(String, String)>) -> (CallbackStorage, Arc<Counters>) {
        let (vtable, counters) = new_std_fs_vtable();
        (CallbackStorage::new(vtable, relocate), counters)
    }

    #[test]
    fn io_status_discriminants_are_stable() {
        assert_eq!(IcebergIoStatus::Ok as i32, 0);
        assert_eq!(IcebergIoStatus::NotFound as i32, 1);
        assert_eq!(IcebergIoStatus::PermissionDenied as i32, 2);
        assert_eq!(IcebergIoStatus::Other as i32, 3);
    }

    #[test]
    fn reader_reads_ranges() {
        let dir = TempDir::new("ranges");
        let path = dir.file("a.bin", CONTENT);
        let (storage, counters) = storage(None);
        let rt = runtime();
        rt.block_on(async {
            let reader = storage.reader(&path).await.unwrap();
            assert_eq!(reader.read(0..1).await.unwrap().as_ref(), b"h");
            assert_eq!(reader.read(7..14).await.unwrap().as_ref(), b"iceberg");
            let end = CONTENT.len() as u64;
            assert_eq!(reader.read(end - 1..end).await.unwrap().as_ref(), b"d");
            assert_eq!(reader.read(0..end).await.unwrap().as_ref(), CONTENT);
        });
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn zero_length_and_invalid_ranges_skip_the_engine() {
        let dir = TempDir::new("zero");
        let path = dir.file("a.bin", CONTENT);
        let (storage, counters) = storage(None);
        let rt = runtime();
        rt.block_on(async {
            let reader = storage.reader(&path).await.unwrap();
            assert!(reader.read(5..5).await.unwrap().is_empty());
            let err = reader
                .read(Range { start: 5, end: 3 })
                .await
                .expect_err("reversed range fails");
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
        });
        assert_eq!(counters.reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn read_past_end_is_data_invalid() {
        let dir = TempDir::new("eof");
        let path = dir.file("a.bin", CONTENT);
        let (storage, _counters) = storage(None);
        let rt = runtime();
        let err = rt
            .block_on(async {
                let reader = storage.reader(&path).await?;
                reader.read(0..(CONTENT.len() as u64 + 8)).await
            })
            .expect_err("read past end fails");
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(err.message().contains("unexpected end of file"), "{err}");
        assert!(err.message().contains(&path), "{err}");
    }

    #[test]
    fn whole_file_read_and_metadata() {
        let dir = TempDir::new("whole");
        let path = dir.file("a.bin", CONTENT);
        let empty = dir.file("empty.bin", b"");
        let (storage, counters) = storage(None);
        let rt = runtime();
        rt.block_on(async {
            assert_eq!(
                storage.read(&path).await.unwrap(),
                std::fs::read(&path).unwrap()
            );
            assert_eq!(
                storage.metadata(&path).await.unwrap().size,
                CONTENT.len() as u64
            );
            assert!(storage.read(&empty).await.unwrap().is_empty());
            assert_eq!(storage.metadata(&empty).await.unwrap().size, 0);
        });
        assert_eq!(counters.opens.load(Ordering::SeqCst), 4);
        assert_eq!(counters.closes.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn exists_reports_presence() {
        let dir = TempDir::new("exists");
        let path = dir.file("a.bin", CONTENT);
        let (storage, _counters) = storage(None);
        let rt = runtime();
        rt.block_on(async {
            assert!(storage.exists(&path).await.unwrap());
            assert!(!storage.exists(&dir.path("missing.bin")).await.unwrap());
        });
    }

    #[test]
    fn missing_file_is_data_invalid_with_path_and_detail() {
        let dir = TempDir::new("missing");
        let path = dir.path("nope.bin");
        let (storage, counters) = storage(None);
        let rt = runtime();
        for err in rt.block_on(async {
            [
                storage.read(&path).await.err(),
                storage.metadata(&path).await.err(),
                storage.reader(&path).await.err(),
            ]
        }) {
            let err = err.expect("missing file fails");
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(err.message().starts_with("file does not exist: "), "{err}");
            assert!(err.message().contains(&path), "{err}");
            assert!(err.message().contains("No such file"), "{err}");
        }
        assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
    }

    #[cfg(unix)]
    #[test]
    fn permission_denied_is_unexpected_with_path_and_detail() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new("denied");
        let path = dir.file("locked.bin", CONTENT);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).expect("chmod 000");
        if std::fs::File::open(&path).is_ok() {
            // Running with elevated privileges: the mode is not enforced.
            return;
        }
        let (storage, counters) = storage(None);
        let rt = runtime();
        for err in rt.block_on(async {
            [
                storage.read(&path).await.err(),
                storage.metadata(&path).await.err(),
                storage.reader(&path).await.err(),
            ]
        }) {
            let err = err.expect("unreadable file fails");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(
                err.message().starts_with("permission denied: open "),
                "{err}"
            );
            assert!(err.message().contains(&path), "{err}");
            assert!(err.message().contains("Permission denied"), "{err}");
        }
        assert_eq!(counters.opens.load(Ordering::SeqCst), 0);
    }

    #[cfg(unix)]
    #[test]
    fn other_failures_carry_the_engine_message() {
        let dir = TempDir::new("other");
        // Opening a directory succeeds on unix; reading from it fails with
        // EISDIR, which the test vtable maps to `Other`.
        let path = dir.0.to_str().unwrap().to_owned();
        let (storage, _counters) = storage(None);
        let rt = runtime();
        let err = rt
            .block_on(async {
                let reader = storage.reader(&path).await?;
                reader.read(0..4).await
            })
            .expect_err("reading a directory fails");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.message().contains("read_at failed"), "{err}");
        assert!(err.message().contains("Is a directory"), "{err}");
    }

    #[test]
    fn relocation_rewrites_only_matching_prefixes() {
        let r = Relocation::new("s3://bucket/warehouse/tbl/", "/local/tbl/");
        assert_eq!(r.from, "s3://bucket/warehouse/tbl");
        assert_eq!(r.to, "/local/tbl");
        assert_eq!(
            r.rewrite("s3://bucket/warehouse/tbl/metadata/v1.metadata.json"),
            "/local/tbl/metadata/v1.metadata.json"
        );
        assert_eq!(r.rewrite("s3://bucket/warehouse/tbl"), "/local/tbl");
        assert_eq!(
            r.rewrite("s3://bucket/warehouse/tbl2/x"),
            "s3://bucket/warehouse/tbl2/x"
        );
        assert_eq!(r.rewrite("/elsewhere/x"), "/elsewhere/x");
    }

    #[test]
    fn storage_applies_relocation_and_passes_other_paths_through() {
        let dir = TempDir::new("relocate");
        let real = dir.file("a.bin", CONTENT);
        let (storage, counters) = storage(Some((
            "/logical/root/".to_owned(),
            format!("{}/", dir.0.to_str().unwrap()),
        )));
        let rt = runtime();
        rt.block_on(async {
            assert_eq!(
                storage.read("/logical/root/a.bin").await.unwrap().as_ref(),
                CONTENT
            );
            assert!(storage.exists("/logical/root/a.bin").await.unwrap());
            assert!(!storage.exists("/logical/rootx/a.bin").await.unwrap());
            // A path outside the prefix reaches the engine untouched.
            assert_eq!(storage.read(&real).await.unwrap().as_ref(), CONTENT);
            // The InputFile keeps the logical location.
            let input = storage.new_input("/logical/root/a.bin").unwrap();
            assert_eq!(input.location(), "/logical/root/a.bin");
            assert_eq!(input.read().await.unwrap().as_ref(), CONTENT);
        });
        let opened = counters.opened_paths.lock().unwrap().clone();
        assert_eq!(opened, vec![real.clone(), real.clone(), real]);
    }

    #[test]
    fn dropping_readers_closes_and_last_owner_releases() {
        let dir = TempDir::new("lifetime");
        let path = dir.file("a.bin", CONTENT);
        let (storage, counters) = storage(None);
        let rt = runtime();
        let reader = rt.block_on(async {
            let reader = storage.reader(&path).await.unwrap();
            assert_eq!(reader.read(0..5).await.unwrap().as_ref(), b"hello");
            reader
        });
        assert_eq!(counters.opens.load(Ordering::SeqCst), 1);
        assert_eq!(counters.closes.load(Ordering::SeqCst), 0);
        drop(reader);
        assert_eq!(counters.closes.load(Ordering::SeqCst), 1);

        // Clones and InputFiles keep the context alive.
        let clone = storage.clone();
        let input = storage.new_input(&path).unwrap();
        drop(storage);
        assert!(!counters.released.load(Ordering::SeqCst));
        drop(clone);
        assert!(!counters.released.load(Ordering::SeqCst));
        drop(input);
        assert!(counters.released.load(Ordering::SeqCst));
    }

    #[test]
    fn file_io_routes_through_the_vtable() {
        let dir = TempDir::new("fileio");
        let path = dir.file("a.bin", CONTENT);
        let (storage, counters) = storage(None);
        let io = file_io(storage);
        let rt = runtime();
        rt.block_on(async {
            let input = io.new_input(&path).unwrap();
            assert!(input.exists().await.unwrap());
            assert_eq!(input.metadata().await.unwrap().size, CONTENT.len() as u64);
            assert_eq!(input.read().await.unwrap().as_ref(), CONTENT);
            let reader = input.reader().await.unwrap();
            assert_eq!(reader.read(0..5).await.unwrap().as_ref(), b"hello");
            assert!(io.exists(&path).await.unwrap());
        });
        assert!(io.new_output(&path).is_err());
        drop(io);
        assert_eq!(
            counters.opens.load(Ordering::SeqCst),
            counters.closes.load(Ordering::SeqCst)
        );
        assert!(counters.released.load(Ordering::SeqCst));
    }

    #[test]
    fn writes_are_unsupported() {
        let dir = TempDir::new("readonly");
        let path = dir.path("new.bin");
        let (storage, _counters) = storage(None);
        let rt = runtime();
        let kinds = rt.block_on(async {
            [
                storage.write(&path, Bytes::from_static(b"x")).await.err(),
                storage.writer(&path).await.err(),
                storage.delete(&path).await.err(),
                storage.delete_prefix(&path).await.err(),
                storage
                    .delete_stream(Box::pin(futures::stream::empty::<String>()))
                    .await
                    .err(),
            ]
        });
        for err in kinds {
            assert_eq!(
                err.expect("write path fails").kind(),
                ErrorKind::FeatureUnsupported
            );
        }
        assert_eq!(
            storage.new_output(&path).err().map(|e| e.kind()),
            Some(ErrorKind::FeatureUnsupported)
        );
        assert!(!std::path::Path::new(&path).exists());
    }

    #[test]
    fn file_io_over_the_vtable_is_not_serialisable() {
        let (storage, _counters) = storage(None);
        let io = file_io(storage);
        // serialize_all goes through typetag to the hand-written impl, which
        // refuses: a table of engine callbacks cannot leave the process.
        let err = io.serialize_all().expect_err("serialisation fails");
        assert!(err.to_string().contains("cannot be serialised"), "{err}");
    }
}
