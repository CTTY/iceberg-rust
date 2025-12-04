# Design Document: Runtime Trait Abstraction


## Overview


This design refactors iceberg-rust's runtime module from an enum-based conditional compilation approach to a trait-based abstraction. This enables third-party runtime implementations without modifying the core library.


**Current Problem:**
```rust
pub enum JoinHandle<T> {
   #[cfg(feature = "tokio")]
   Tokio(tokio::task::JoinHandle<T>),
   #[cfg(all(feature = "smol", not(feature = "tokio")))]
   Smol(smol::Task<T>),
   // ...
}
```


Adding new runtimes requires modifying the core crate and using conditional compilation.


**Solution:**
- Define `Runtime` and `JoinHandle` traits in core crate
- Move runtime implementations to separate crates
- Use global runtime with automatic initialization

## Architecture

```
┌─────────────────────────────────────────┐
│         crates/iceberg                  │
│  - trait Runtime                        │
│  - trait JoinHandle                     │
│  - pub fn spawn()                       │
│  - pub fn spawn_blocking()              │
└─────────────────────────────────────────┘
             ▲           ▲
             │           │
    ┌────────┘           └──────-───┐
    │                               │
┌───┴──────────────┐    ┌───────────┴──────┐
│ crates/runtime/  │    │ crates/runtime/  │
│     tokio        │    │      smol        │
│                  │    │                  │
│ impl Runtime     │    │ impl Runtime     │
│ auto-init via    │    │ auto-init via    │
│ ctor             │    │ ctor             │
└──────────────────┘    └──────────────────┘
```


## Core Traits


### JoinHandle Trait


```rust
pub trait JoinHandle<T>: Future<Output = T> + Send
where
   T: Send + 'static,
{
   fn abort(&self);
   fn is_finished(&self) -> bool;
}
```

### Runtime Trait


```rust
pub trait Runtime: Send + Sync + 'static {
   fn spawn<F>(&self, future: F) -> Box<dyn JoinHandle<F::Output>>
   where
       F: Future + Send + 'static,
       F::Output: Send + 'static;


   fn spawn_blocking<F, T>(&self, f: F) -> Box<dyn JoinHandle<T>>
   where
       F: FnOnce() -> T + Send + 'static,
       T: Send + 'static;
}
```


**Design Choice**: `Runtime` returns `Box<dyn JoinHandle<T>>` instead of using associated types. This avoids complex type erasure while keeping the API simple.


### Global Runtime


```rust
static RUNTIME: OnceLock<&'static dyn Runtime> = OnceLock::new();


pub fn set_runtime(runtime: &'static dyn Runtime) -> bool {
   RUNTIME.set(runtime).is_ok()
}


pub fn spawn<F>(future: F) -> Box<dyn JoinHandle<F::Output>>
where
   F: Future + Send + 'static,
   F::Output: Send + 'static,
{
   runtime().spawn(future)
}


pub fn spawn_blocking<F, T>(f: F) -> Box<dyn JoinHandle<T>>
where
   F: FnOnce() -> T + Send + 'static,
   T: Send + 'static,
{
   runtime().spawn_blocking(f)
}
```


## Runtime Implementations


### Tokio Runtime


```rust
pub struct TokioRuntime;
pub struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);


impl<T: Send + 'static> Future for TokioJoinHandle<T> {
   type Output = T;
   fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
       Pin::new(&mut self.0)
           .poll(cx)
           .map(|result| result.expect("tokio task panicked"))
   }
}


impl<T: Send + 'static> JoinHandle<T> for TokioJoinHandle<T> {
   fn abort(&self) {
       self.0.abort();
   }
   fn is_finished(&self) -> bool {
       self.0.is_finished()
   }
}


impl Runtime for TokioRuntime {
   fn spawn<F>(&self, future: F) -> Box<dyn JoinHandle<F::Output>>
   where
       F: Future + Send + 'static,
       F::Output: Send + 'static,
   {
       Box::new(TokioJoinHandle(tokio::task::spawn(future)))
   }


   fn spawn_blocking<F, T>(&self, f: F) -> Box<dyn JoinHandle<T>>
   where
       F: FnOnce() -> T + Send + 'static,
       T: Send + 'static,
   {
       Box::new(TokioJoinHandle(tokio::task::spawn_blocking(f)))
   }
}


static TOKIO_RUNTIME: TokioRuntime = TokioRuntime;


#[ctor]
pub fn init() {
   set_runtime(&TOKIO_RUNTIME);
}
```


### Smol Runtime


```rust
pub struct SmolRuntime;
pub struct SmolJoinHandle<T>(smol::Task<T>);


impl<T: Send + 'static> Future for SmolJoinHandle<T> {
   type Output = T;
   fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
       Pin::new(&mut self.0).poll(cx)
   }
}


impl<T: Send + 'static> JoinHandle<T> for SmolJoinHandle<T> {
   fn abort(&self) {
       self.0.cancel();
   }
   fn is_finished(&self) -> bool {
       self.0.is_finished()
   }
}


impl Runtime for SmolRuntime {
   fn spawn<F>(&self, future: F) -> Box<dyn JoinHandle<F::Output>>
   where
       F: Future + Send + 'static,
       F::Output: Send + 'static,
   {
       Box::new(SmolJoinHandle(smol::spawn(future)))
   }


   fn spawn_blocking<F, T>(&self, f: F) -> Box<dyn JoinHandle<T>>
   where
       F: FnOnce() -> T + Send + 'static,
       T: Send + 'static,
   {
       Box::new(SmolJoinHandle(smol::unblock(f)))
   }
}


static SMOL_RUNTIME: SmolRuntime = SmolRuntime;


#[ctor]
pub fn init() {
   set_runtime(&SMOL_RUNTIME);
}
```


## Usage


### Automatic Initialization (Default)


```rust
use iceberg::runtime::spawn;


#[tokio::main]
async fn main() {
   // Runtime automatically initialized
   let handle = spawn(async { 1 + 1 });
   assert_eq!(handle.await, 2);
}
```


### Custom Runtime


```rust
use iceberg::runtime::{Runtime, set_runtime};


struct MyRuntime;


impl Runtime for MyRuntime {
   // ... implement trait ...
}


static MY_RUNTIME: MyRuntime = MyRuntime;


fn main() {
   set_runtime(&MY_RUNTIME);
   // Now can use spawn() and spawn_blocking()
}
```

### Open Questions

#### Runtime Storage: Global vs Catalog-Scoped

The RFC proposes a global runtime approach using `OnceLock<&'static dyn Runtime>`, but an alternative design would store the runtime in the catalog itself. Each approach has distinct trade-offs:

**Global Runtime (Current Proposal)**

Advantages:
- Simpler API: Users call `iceberg::runtime::spawn()` without needing catalog references
- Single initialization point: Runtime is set once at application startup via `ctor` or explicit `set_runtime()`
- Lower memory overhead: One runtime instance shared across all catalogs
- Matches common patterns: Most applications use a single async runtime (tokio or smol)
- Easier migration: Existing code using conditional compilation can switch to global runtime with minimal changes

Disadvantages:
- Less flexible for multi-runtime scenarios: Applications that need different runtimes for different catalogs (e.g., tokio for one, smol for another) cannot do so
- Hidden dependency: The runtime must be initialized before catalog operations, which may not be obvious
- Testing complexity: Tests need to ensure runtime is initialized, potentially requiring test-specific setup

**Catalog-Scoped Runtime**

Advantages:
- Maximum flexibility: Each catalog can have its own runtime, enabling mixed-runtime applications
- Explicit dependency: Runtime is clearly associated with the catalog that uses it
- Better isolation: Catalog operations are self-contained with their runtime dependencies
- Easier testing: Each catalog test can use its own runtime without global state

Disadvantages:
- More complex API: Every spawn operation needs catalog context (e.g., `catalog.runtime().spawn()`)
- API surface changes: Existing functions that spawn tasks would need catalog parameters
- Higher memory overhead: Multiple catalog instances mean multiple runtime references
- Awkward for standalone operations: Operations outside catalog context (e.g., FileIO) still need runtime access

**Recommendation**

The global runtime approach is recommended for the initial implementation because:

1. **Pragmatic**: 99% of applications use a single async runtime
2. **Ergonomic**: Simpler API reduces friction for common use cases
3. **Incremental**: Can be extended later with catalog-scoped overrides if needed

A future enhancement could support both patterns:
```rust
// Global runtime (default)
iceberg::runtime::spawn(async { ... });

// Catalog-specific override (opt-in)
catalog.with_runtime(custom_runtime).spawn(async { ... });
```

This hybrid approach would provide the simplicity of global runtime for common cases while enabling catalog-scoped runtime for advanced scenarios.
