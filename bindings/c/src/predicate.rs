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

//! Building an iceberg predicate from engine callbacks.
//!
//! The engine never sees iceberg-rust's expression types. Instead
//! `iceberg_scan_builder_set_predicate` hands it an opaque
//! [`IcebergPredicateState`], an arena of nodes, and the engine calls the
//! `iceberg_pred_*` constructors below to add columns, literals and
//! predicates to it. Every constructor returns the id of the node it created;
//! the engine passes those ids to later constructors and finally returns the
//! id of the root predicate from its visitor.
//!
//! # Failure semantics
//!
//! A constructor that cannot do its job (invalid or wrong-kind node id, null
//! pointer, unrepresentable literal) records the first such failure in the
//! state and returns [`ICEBERG_PRED_NONE`]. Once failed, every later
//! constructor on that state is a no-op that also returns the sentinel, and
//! `iceberg_scan_builder_set_predicate` reports
//! [`IcebergErrorCode::InvalidArgument`]
//! with the recorded message instead of planning with a partial predicate.
//! The engine therefore does not need to check every return value.
//!
//! A visitor that returns [`ICEBERG_PRED_NONE`] on a state that has *not*
//! failed means "no predicate": the builder plans every file. The failure
//! outranks the sentinel, so a visitor that blindly returns the id of a
//! failed node cannot silently drop a filter.

use std::ffi::c_void;

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

use crate::error::{FfiError, FfiResult, IcebergErrorCode};
use crate::literal::{IcebergLiteral, to_datum};
use crate::strings::StringSlice;

/// Node id returned by a failing constructor, and returned by the engine's
/// visitor to mean "no predicate".
pub const ICEBERG_PRED_NONE: usize = usize::MAX;

/// Engine callback invoked by `iceberg_scan_builder_set_predicate`.
///
/// Arguments: the engine's `ctx` and the predicate state to build into.
/// Returns the id of the root predicate node, or [`ICEBERG_PRED_NONE`] for no
/// predicate. Runs on the calling thread and must not unwind.
pub type IcebergPredicateVisitorFn =
    extern "C" fn(*mut c_void, *mut IcebergPredicateState) -> usize;

/// One arena entry.
#[derive(Debug, Clone)]
enum Node {
    Column(String),
    Literal(Datum),
    Predicate(Predicate),
}

impl Node {
    fn kind(&self) -> &'static str {
        match self {
            Node::Column(_) => "column",
            Node::Literal(_) => "literal",
            Node::Predicate(_) => "predicate",
        }
    }
}

/// Arena of predicate nodes under construction; opaque to the engine.
#[derive(Debug, Default)]
pub struct IcebergPredicateState {
    nodes: Vec<Node>,
    failed: Option<String>,
}

impl IcebergPredicateState {
    /// An empty state.
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, node: Node) -> usize {
        self.nodes.push(node);
        self.nodes.len() - 1
    }

    fn fail(&mut self, message: String) -> usize {
        if self.failed.is_none() {
            self.failed = Some(message);
        }
        ICEBERG_PRED_NONE
    }

    fn node(&self, id: usize, expected: &str) -> Result<&Node, String> {
        match self.nodes.get(id) {
            Some(node) if node.kind() == expected => Ok(node),
            Some(node) => Err(format!(
                "predicate node {id} is a {} but a {expected} was expected",
                node.kind()
            )),
            None => Err(format!("predicate node id {id} does not exist")),
        }
    }

    fn column(&self, id: usize) -> Result<Reference, String> {
        match self.node(id, "column")? {
            Node::Column(name) => Ok(Reference::new(name.clone())),
            _ => Err(format!("predicate node {id} is not a column")),
        }
    }

    fn literal(&self, id: usize) -> Result<Datum, String> {
        match self.node(id, "literal")? {
            Node::Literal(datum) => Ok(datum.clone()),
            _ => Err(format!("predicate node {id} is not a literal")),
        }
    }

    fn predicate(&self, id: usize) -> Result<Predicate, String> {
        match self.node(id, "predicate")? {
            Node::Predicate(p) => Ok(p.clone()),
            _ => Err(format!("predicate node {id} is not a predicate")),
        }
    }

    /// Resolves the visitor's return value.
    ///
    /// A recorded failure wins over everything; otherwise `root ==
    /// ICEBERG_PRED_NONE` is "no predicate" and any other id must name a
    /// predicate node.
    pub fn finish(self, root: usize) -> FfiResult<Option<Predicate>> {
        if let Some(message) = self.failed {
            return Err(FfiError::new(
                IcebergErrorCode::InvalidArgument,
                format!("predicate could not be built: {message}"),
            ));
        }
        if root == ICEBERG_PRED_NONE {
            return Ok(None);
        }
        self.predicate(root)
            .map(Some)
            .map_err(|m| FfiError::new(IcebergErrorCode::InvalidArgument, m))
    }
}

/// Runs `f` against the state behind `state` and records its node.
///
/// Null and already-failed states yield the sentinel without calling `f`. A
/// panic inside `f` marks the state failed (the sentinel alone would read as
/// "no predicate" if the visitor returned it).
fn apply(
    state: *mut IcebergPredicateState,
    f: impl FnOnce(&mut IcebergPredicateState) -> Result<Node, String>,
) -> usize {
    ffi_guard!(
        |msg| {
            if !state.is_null() {
                // SAFETY: `state` is the non-null pointer handed to the
                // visitor by `iceberg_scan_builder_set_predicate`; the
                // unwound body no longer holds a borrow of it.
                unsafe { &mut *state }.fail(msg);
            }
            ICEBERG_PRED_NONE
        },
        {
            if state.is_null() {
                return ICEBERG_PRED_NONE;
            }
            // SAFETY: as above; the visitor runs on one thread and holds no
            // other reference to the state while it calls into this crate.
            let state = unsafe { &mut *state };
            if state.failed.is_some() {
                return ICEBERG_PRED_NONE;
            }
            match f(state) {
                Ok(node) => state.push(node),
                Err(message) => state.fail(message),
            }
        }
    )
}

/// Views `n` items at `ptr`, failing on a null pointer with `n > 0`.
///
/// # Safety
///
/// `ptr` must be valid for `n` reads for the duration of the call, or `n`
/// must be zero.
unsafe fn items<'a, T>(ptr: *const T, n: usize, what: &str) -> Result<&'a [T], String> {
    if n == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(format!("{what} pointer is null but {n} entries were given"));
    }
    // SAFETY: forwarded to the caller.
    Ok(unsafe { std::slice::from_raw_parts(ptr, n) })
}

fn binary(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
    op: fn(Reference, Datum) -> Predicate,
) -> usize {
    apply(state, |s| {
        let reference = s.column(column)?;
        let datum = s.literal(literal)?;
        Ok(Node::Predicate(op(reference, datum)))
    })
}

fn unary(
    state: *mut IcebergPredicateState,
    column: usize,
    op: fn(Reference) -> Predicate,
) -> usize {
    apply(state, |s| Ok(Node::Predicate(op(s.column(column)?))))
}

fn set(
    state: *mut IcebergPredicateState,
    column: usize,
    literals: *const usize,
    n: usize,
    op: fn(Reference, Vec<Datum>) -> Predicate,
) -> usize {
    apply(state, |s| {
        let reference = s.column(column)?;
        // SAFETY: the engine keeps the id array alive for the duration of
        // the call.
        let ids = unsafe { items(literals, n, "literal id") }?;
        let datums = ids
            .iter()
            .map(|&id| s.literal(id))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Node::Predicate(op(reference, datums)))
    })
}

fn logical(
    state: *mut IcebergPredicateState,
    children: *const usize,
    n: usize,
    identity: Predicate,
    op: fn(Predicate, Predicate) -> Predicate,
) -> usize {
    apply(state, |s| {
        // SAFETY: the engine keeps the id array alive for the duration of
        // the call.
        let ids = unsafe { items(children, n, "child id") }?;
        let mut acc = identity;
        for &id in ids {
            acc = op(acc, s.predicate(id)?);
        }
        Ok(Node::Predicate(acc))
    })
}

// The pointer arguments of every `iceberg_pred_*` function are engine
// arguments with the same contract as every handle in this crate (valid for
// the call, checked for null); `unsafe` on an `extern "C"` signature would not
// change anything for the C caller.

/// Adds a column reference named by `n` path parts joined with `.`.
///
/// `n` must be at least 1 and every part valid UTF-8.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_column(
    state: *mut IcebergPredicateState,
    path_parts: *const StringSlice,
    n: usize,
) -> usize {
    apply(state, |_| {
        if n == 0 {
            return Err("column path has no parts".to_owned());
        }
        // SAFETY: the engine keeps the parts alive for the duration of the
        // call.
        let parts = unsafe { items(path_parts, n, "column path") }?;
        let mut name = String::new();
        for (i, part) in parts.iter().enumerate() {
            // SAFETY: as above.
            let part = unsafe { part.as_str() }
                .map_err(|e| format!("column path part {i} is not valid UTF-8: {e}"))?;
            if i > 0 {
                name.push('.');
            }
            name.push_str(part);
        }
        Ok(Node::Column(name))
    })
}

/// Adds a literal; see [`IcebergLiteral`] for the encoding. A null literal
/// (`is_null`) or one that cannot be represented fails the state.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_literal(
    state: *mut IcebergPredicateState,
    literal: *const IcebergLiteral,
) -> usize {
    apply(state, |_| {
        if literal.is_null() {
            return Err("literal pointer is null".to_owned());
        }
        // SAFETY: non-null, and the engine keeps the struct and its byte
        // payload alive for the duration of the call.
        let datum = unsafe { to_datum(&*literal) }.map_err(|e| e.message)?;
        Ok(Node::Literal(datum))
    })
}

/// `column < literal`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_lt(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::less_than)
}

/// `column <= literal`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_le(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::less_than_or_equal_to)
}

/// `column > literal`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_gt(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::greater_than)
}

/// `column >= literal`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_ge(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::greater_than_or_equal_to)
}

/// `column = literal`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_eq(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::equal_to)
}

/// `column <> literal`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_ne(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::not_equal_to)
}

/// `column STARTS WITH literal` (string columns).
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_starts_with(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::starts_with)
}

/// `column NOT STARTS WITH literal` (string columns).
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_not_starts_with(
    state: *mut IcebergPredicateState,
    column: usize,
    literal: usize,
) -> usize {
    binary(state, column, literal, Reference::not_starts_with)
}

/// `column IS NULL`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_is_null(state: *mut IcebergPredicateState, column: usize) -> usize {
    unary(state, column, Reference::is_null)
}

/// `column IS NOT NULL`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_not_null(state: *mut IcebergPredicateState, column: usize) -> usize {
    unary(state, column, Reference::is_not_null)
}

/// `column IS NAN` (float and double columns).
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_is_nan(state: *mut IcebergPredicateState, column: usize) -> usize {
    unary(state, column, Reference::is_nan)
}

/// `column IS NOT NAN` (float and double columns).
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_not_nan(state: *mut IcebergPredicateState, column: usize) -> usize {
    unary(state, column, Reference::is_not_nan)
}

/// `column IN (literals...)`; `literals` holds `n` literal node ids.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_in(
    state: *mut IcebergPredicateState,
    column: usize,
    literals: *const usize,
    n: usize,
) -> usize {
    set(state, column, literals, n, Reference::is_in)
}

/// `column NOT IN (literals...)`; `literals` holds `n` literal node ids.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_not_in(
    state: *mut IcebergPredicateState,
    column: usize,
    literals: *const usize,
    n: usize,
) -> usize {
    set(state, column, literals, n, Reference::is_not_in)
}

/// Conjunction of `n` predicate node ids; zero children is `TRUE`.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_and(
    state: *mut IcebergPredicateState,
    children: *const usize,
    n: usize,
) -> usize {
    logical(state, children, n, Predicate::AlwaysTrue, Predicate::and)
}

/// Disjunction of `n` predicate node ids; zero children is `FALSE`.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_or(
    state: *mut IcebergPredicateState,
    children: *const usize,
    n: usize,
) -> usize {
    logical(state, children, n, Predicate::AlwaysFalse, Predicate::or)
}

/// Negation of a predicate node, expressed with inverse operators (`NOT (a <
/// 5)` becomes `a >= 5`).
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_not(state: *mut IcebergPredicateState, child: usize) -> usize {
    apply(state, |s| Ok(Node::Predicate(s.predicate(child)?.negate())))
}

/// The predicate `TRUE`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_always_true(state: *mut IcebergPredicateState) -> usize {
    apply(state, |_| Ok(Node::Predicate(Predicate::AlwaysTrue)))
}

/// The predicate `FALSE`.
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_pred_always_false(state: *mut IcebergPredicateState) -> usize {
    apply(state, |_| Ok(Node::Predicate(Predicate::AlwaysFalse)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod test_support {
    use super::*;
    use crate::literal::IcebergLiteralKind;

    /// Adds a dotted column path.
    pub(crate) fn column(state: &mut IcebergPredicateState, path: &str) -> usize {
        let parts: Vec<StringSlice> = path.split('.').map(StringSlice::borrowed).collect();
        iceberg_pred_column(state, parts.as_ptr(), parts.len())
    }

    pub(crate) fn long(state: &mut IcebergPredicateState, v: i64) -> usize {
        let mut lit = IcebergLiteral::zero(IcebergLiteralKind::Long);
        lit.i = v;
        iceberg_pred_literal(state, &lit)
    }

    pub(crate) fn int(state: &mut IcebergPredicateState, v: i32) -> usize {
        let mut lit = IcebergLiteral::zero(IcebergLiteralKind::Int);
        lit.i = i64::from(v);
        iceberg_pred_literal(state, &lit)
    }

    pub(crate) fn string(state: &mut IcebergPredicateState, s: &str) -> usize {
        let mut lit = IcebergLiteral::zero(IcebergLiteralKind::String);
        lit.bytes = StringSlice::borrowed(s);
        iceberg_pred_literal(state, &lit)
    }

    pub(crate) fn timestamp(state: &mut IcebergPredicateState, micros: i64) -> usize {
        let mut lit = IcebergLiteral::zero(IcebergLiteralKind::Timestamp);
        lit.i = micros;
        iceberg_pred_literal(state, &lit)
    }

    pub(crate) fn decimal(
        state: &mut IcebergPredicateState,
        mantissa: i128,
        precision: u32,
        scale: u32,
    ) -> usize {
        let mut lit = IcebergLiteral::zero(IcebergLiteralKind::Decimal);
        (lit.u_hi, lit.u_lo) = crate::literal::split_mantissa(mantissa);
        lit.precision = precision;
        lit.scale = scale;
        iceberg_pred_literal(state, &lit)
    }

    pub(crate) fn and(state: &mut IcebergPredicateState, children: &[usize]) -> usize {
        iceberg_pred_and(state, children.as_ptr(), children.len())
    }

    pub(crate) fn or(state: &mut IcebergPredicateState, children: &[usize]) -> usize {
        iceberg_pred_or(state, children.as_ptr(), children.len())
    }

    pub(crate) fn is_in(
        state: &mut IcebergPredicateState,
        column: usize,
        literals: &[usize],
    ) -> usize {
        iceberg_pred_in(state, column, literals.as_ptr(), literals.len())
    }

    pub(crate) fn not_in(
        state: &mut IcebergPredicateState,
        column: usize,
        literals: &[usize],
    ) -> usize {
        iceberg_pred_not_in(state, column, literals.as_ptr(), literals.len())
    }

    /// Finishes the state and renders the predicate.
    pub(crate) fn display(state: IcebergPredicateState, root: usize) -> String {
        state
            .finish(root)
            .expect("finish")
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<none>".to_owned())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn builds_comparison_and_conjunction() {
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let ten = long(&mut s, 10);
        let lt = iceberg_pred_lt(&mut s, a, ten);
        let bc = column(&mut s, "b.c");
        let x = string(&mut s, "x");
        let eq = iceberg_pred_eq(&mut s, bc, x);
        let root = and(&mut s, &[lt, eq]);
        assert_eq!(display(s, root), r#"(a < 10) AND (b.c = "x")"#);
    }

    #[test]
    fn comparison_operators_render() {
        let cases: [(
            extern "C" fn(*mut IcebergPredicateState, usize, usize) -> usize,
            &str,
        ); 6] = [
            (iceberg_pred_lt, "a < 1"),
            (iceberg_pred_le, "a <= 1"),
            (iceberg_pred_gt, "a > 1"),
            (iceberg_pred_ge, "a >= 1"),
            (iceberg_pred_eq, "a = 1"),
            (iceberg_pred_ne, "a != 1"),
        ];
        for (op, expected) in cases {
            let mut s = IcebergPredicateState::new();
            let a = column(&mut s, "a");
            let one = int(&mut s, 1);
            let root = op(&mut s, a, one);
            assert_eq!(display(s, root), expected);
        }
    }

    #[test]
    fn in_and_not_in() {
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let one = int(&mut s, 1);
        let three = int(&mut s, 3);
        let root = is_in(&mut s, a, &[one, three]);
        let text = display(s, root);
        assert!(text.starts_with("a IN ("), "{text}");
        assert!(text.contains('1') && text.contains('3'), "{text}");

        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let one = int(&mut s, 1);
        let root = not_in(&mut s, a, &[one]);
        let text = display(s, root);
        assert!(text.contains("NOT IN"), "{text}");
    }

    #[test]
    fn or_renders_both_sides() {
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let one = int(&mut s, 1);
        let two = int(&mut s, 2);
        let p = iceberg_pred_eq(&mut s, a, one);
        let q = iceberg_pred_eq(&mut s, a, two);
        let root = or(&mut s, &[p, q]);
        assert_eq!(display(s, root), "(a = 1) OR (a = 2)");
    }

    #[test]
    fn not_negates_inner() {
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let five = int(&mut s, 5);
        let lt = iceberg_pred_lt(&mut s, a, five);
        let root = iceberg_pred_not(&mut s, lt);
        assert_eq!(display(s, root), "a >= 5");
    }

    #[test]
    fn and_of_nothing_is_true_or_of_nothing_is_false() {
        let mut s = IcebergPredicateState::new();
        let root = and(&mut s, &[]);
        assert_eq!(display(s, root), "TRUE");
        let mut s = IcebergPredicateState::new();
        let root = or(&mut s, &[]);
        assert_eq!(display(s, root), "FALSE");
        let mut s = IcebergPredicateState::new();
        let root = iceberg_pred_always_true(&mut s);
        assert_eq!(display(s, root), "TRUE");
        let mut s = IcebergPredicateState::new();
        let root = iceberg_pred_always_false(&mut s);
        assert_eq!(display(s, root), "FALSE");
    }

    #[test]
    fn unary_operators() {
        let cases: [(
            extern "C" fn(*mut IcebergPredicateState, usize) -> usize,
            &str,
        ); 4] = [
            (iceberg_pred_is_null, "a IS NULL"),
            (iceberg_pred_not_null, "a IS NOT NULL"),
            (iceberg_pred_is_nan, "a IS NAN"),
            (iceberg_pred_not_nan, "a IS NOT NAN"),
        ];
        for (op, expected) in cases {
            let mut s = IcebergPredicateState::new();
            let a = column(&mut s, "a");
            let root = op(&mut s, a);
            assert_eq!(display(s, root), expected);
        }
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let p = string(&mut s, "p");
        let root = iceberg_pred_starts_with(&mut s, a, p);
        assert_eq!(display(s, root), r#"a STARTS WITH "p""#);
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let p = string(&mut s, "p");
        let root = iceberg_pred_not_starts_with(&mut s, a, p);
        assert_eq!(display(s, root), r#"a NOT STARTS WITH "p""#);
    }

    #[test]
    fn kind_mismatch_fails_state() {
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let one = int(&mut s, 1);
        // Literal where a column is expected.
        assert_eq!(iceberg_pred_lt(&mut s, one, a), ICEBERG_PRED_NONE);
        // Later calls are no-ops.
        assert_eq!(iceberg_pred_always_true(&mut s), ICEBERG_PRED_NONE);
        let e = s.finish(ICEBERG_PRED_NONE).expect_err("failed state");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("node 1"), "{}", e.message);
        assert!(e.message.contains("literal"), "{}", e.message);

        // A column where a predicate is expected.
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        assert_eq!(iceberg_pred_not(&mut s, a), ICEBERG_PRED_NONE);
        assert!(s.finish(ICEBERG_PRED_NONE).is_err());

        // A root that is not a predicate.
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        let e = s.finish(a).expect_err("root must be a predicate");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
    }

    #[test]
    fn invalid_id_fails_state() {
        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        assert_eq!(
            iceberg_pred_lt(&mut s, a, ICEBERG_PRED_NONE),
            ICEBERG_PRED_NONE
        );
        assert!(s.finish(ICEBERG_PRED_NONE).is_err());

        let mut s = IcebergPredicateState::new();
        assert_eq!(iceberg_pred_is_null(&mut s, 7), ICEBERG_PRED_NONE);
        let e = s.finish(ICEBERG_PRED_NONE).expect_err("out of range");
        assert!(e.message.contains("does not exist"), "{}", e.message);

        let mut s = IcebergPredicateState::new();
        let ids = [0usize, 9];
        let t = iceberg_pred_always_true(&mut s);
        assert_eq!(t, 0);
        assert_eq!(and(&mut s, &ids), ICEBERG_PRED_NONE);
        assert!(s.finish(t).is_err());
    }

    #[test]
    fn null_pointers_fail_state() {
        let mut s = IcebergPredicateState::new();
        assert_eq!(
            iceberg_pred_literal(&mut s, std::ptr::null()),
            ICEBERG_PRED_NONE
        );
        assert!(s.finish(ICEBERG_PRED_NONE).is_err());

        let mut s = IcebergPredicateState::new();
        assert_eq!(
            iceberg_pred_and(&mut s, std::ptr::null(), 2),
            ICEBERG_PRED_NONE
        );
        assert!(s.finish(ICEBERG_PRED_NONE).is_err());

        let mut s = IcebergPredicateState::new();
        let a = column(&mut s, "a");
        assert_eq!(
            iceberg_pred_in(&mut s, a, std::ptr::null(), 1),
            ICEBERG_PRED_NONE
        );
        assert!(s.finish(ICEBERG_PRED_NONE).is_err());
    }

    #[test]
    fn null_literal_fails_state() {
        let mut s = IcebergPredicateState::new();
        let mut lit = IcebergLiteral::zero(crate::literal::IcebergLiteralKind::Int);
        lit.is_null = true;
        assert_eq!(iceberg_pred_literal(&mut s, &lit), ICEBERG_PRED_NONE);
        let e = s.finish(ICEBERG_PRED_NONE).expect_err("null literal");
        assert!(e.message.contains("null"), "{}", e.message);
    }

    #[test]
    fn null_state_pointer_returns_sentinel() {
        let null = std::ptr::null_mut();
        assert_eq!(iceberg_pred_always_true(null), ICEBERG_PRED_NONE);
        assert_eq!(
            iceberg_pred_column(null, std::ptr::null(), 0),
            ICEBERG_PRED_NONE
        );
        assert_eq!(
            iceberg_pred_literal(null, std::ptr::null()),
            ICEBERG_PRED_NONE
        );
        assert_eq!(iceberg_pred_lt(null, 0, 1), ICEBERG_PRED_NONE);
        assert_eq!(iceberg_pred_is_null(null, 0), ICEBERG_PRED_NONE);
        assert_eq!(
            iceberg_pred_in(null, 0, std::ptr::null(), 0),
            ICEBERG_PRED_NONE
        );
        assert_eq!(
            iceberg_pred_and(null, std::ptr::null(), 0),
            ICEBERG_PRED_NONE
        );
        assert_eq!(iceberg_pred_not(null, 0), ICEBERG_PRED_NONE);
    }

    #[test]
    fn empty_column_path_fails() {
        let mut s = IcebergPredicateState::new();
        assert_eq!(
            iceberg_pred_column(&mut s, std::ptr::null(), 0),
            ICEBERG_PRED_NONE
        );
        let e = s.finish(ICEBERG_PRED_NONE).expect_err("empty path");
        assert!(e.message.contains("no parts"), "{}", e.message);

        let mut s = IcebergPredicateState::new();
        assert_eq!(
            iceberg_pred_column(&mut s, std::ptr::null(), 2),
            ICEBERG_PRED_NONE
        );
        assert!(s.finish(ICEBERG_PRED_NONE).is_err());
    }

    #[test]
    fn sentinel_is_size_max() {
        // cbindgen.toml defines ICEBERG_PRED_NONE as SIZE_MAX by hand.
        assert_eq!(ICEBERG_PRED_NONE, usize::MAX);
    }

    #[test]
    fn none_root_on_healthy_state_is_no_predicate() {
        let mut s = IcebergPredicateState::new();
        let _ = iceberg_pred_always_true(&mut s);
        assert!(s.finish(ICEBERG_PRED_NONE).expect("ok").is_none());
    }
}
