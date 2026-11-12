//! Walking a table schema through engine callbacks.
//!
//! The engine fills in an [`IcebergSchemaVisitor`] and Rust drives it from
//! iceberg-rust's post-order `SchemaVisitor`, so the engine builds its own
//! type tree without ever seeing Rust types.
//!
//! # List protocol
//!
//! Fields are delivered bottom-up into *field lists* the engine owns and
//! identifies by an integer of its choosing:
//!
//! 1. For every struct (including the schema root), the list or map that
//!    needs children, Rust first calls `make_field_list(ctx, reserve)` and
//!    remembers the returned id.
//! 2. It then calls one `visit_*` per child in declaration order, passing
//!    that id as `sibling_list`. Nested children are always delivered before
//!    the parent, so a `visit_struct`/`visit_list`/`visit_map` call carries a
//!    `child_list` whose contents are already complete.
//! 3. `iceberg_table_visit_schema` returns the id of the root list, whose
//!    entries are the schema's top-level columns.
//!
//! A list type is delivered as a one-entry list holding its element field,
//! named `element`; a map type as a two-entry list holding `key` then
//! `value`. Those names follow the Iceberg specification; the ids are the
//! field ids recorded in the schema.

use std::ffi::c_void;

use iceberg::spec::{
    ListType, MapType, NestedFieldRef, PrimitiveType, Schema, SchemaVisitor, StructType,
    VariantType, visit_schema,
};
use iceberg::{Error, ErrorKind};

use crate::error::{FfiError, IcebergErrorCode, IcebergResultUsize};
use crate::handle::Shared;
use crate::optional::IcebergOptionalI32;
use crate::strings::StringSlice;
use crate::table::Table;

/// Primitive type classification delivered by `visit_primitive`.
///
/// `Decimal` carries `precision` and `scale`, `Fixed` carries `width`; the
/// other kinds leave those arguments zero. `Variant` is Iceberg's
/// semi-structured type and is reported here because, like a primitive, it
/// has no child fields.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergPrimitiveKind {
    /// `boolean`.
    Boolean = 0,
    /// `int`, 32-bit signed.
    Int = 1,
    /// `long`, 64-bit signed.
    Long = 2,
    /// `float`, 32-bit IEEE 754.
    Float = 3,
    /// `double`, 64-bit IEEE 754.
    Double = 4,
    /// `decimal(precision, scale)`.
    Decimal = 5,
    /// `date`, days since the Unix epoch.
    Date = 6,
    /// `time`, microseconds since midnight.
    Time = 7,
    /// `timestamp`, microseconds, no time zone.
    Timestamp = 8,
    /// `timestamptz`, microseconds, UTC-adjusted.
    Timestamptz = 9,
    /// `timestamp_ns`, nanoseconds, no time zone.
    TimestampNs = 10,
    /// `timestamptz_ns`, nanoseconds, UTC-adjusted.
    TimestamptzNs = 11,
    /// `string`, UTF-8.
    String = 12,
    /// `uuid`, 16 bytes.
    Uuid = 13,
    /// `fixed[width]`.
    Fixed = 14,
    /// `binary`, variable length.
    Binary = 15,
    /// `unknown`, the all-null placeholder type.
    Unknown = 16,
    /// `variant`, semi-structured data.
    Variant = 17,
}

/// Creates a field list able to hold `reserve` entries and returns its id.
///
/// Arguments: `ctx`, `reserve`.
pub type IcebergMakeFieldListFn = extern "C" fn(*mut c_void, usize) -> usize;

/// Appends a struct field whose children are already in `child_list`.
///
/// Arguments: `ctx`, `sibling_list`, `name`, `field_id`, `required`,
/// `child_list`.
pub type IcebergVisitStructFn = extern "C" fn(*mut c_void, usize, StringSlice, i32, bool, usize);

/// Appends a list field; `child_list` holds the single element field.
///
/// Arguments: `ctx`, `sibling_list`, `name`, `field_id`, `required`,
/// `element_id`, `element_required`, `child_list`.
pub type IcebergVisitListFn =
    extern "C" fn(*mut c_void, usize, StringSlice, i32, bool, i32, bool, usize);

/// Appends a map field; `child_list` holds the key field then the value
/// field. Map keys are always required.
///
/// Arguments: `ctx`, `sibling_list`, `name`, `field_id`, `required`,
/// `key_id`, `value_id`, `value_required`, `child_list`.
pub type IcebergVisitMapFn =
    extern "C" fn(*mut c_void, usize, StringSlice, i32, bool, i32, i32, bool, usize);

/// Appends a primitive field.
///
/// Arguments: `ctx`, `sibling_list`, `name`, `field_id`, `required`, `kind`,
/// `precision`, `scale`, `width`.
pub type IcebergVisitPrimitiveFn =
    extern "C" fn(*mut c_void, usize, StringSlice, i32, bool, IcebergPrimitiveKind, u32, u32, u64);

/// Engine callbacks that receive a schema, see the module documentation for
/// the protocol.
///
/// Every callback receives `ctx` first. Name slices are valid only during the
/// callback. Callbacks are invoked on the calling thread, in dependency order
/// (children before parents, siblings in declaration order), and must not
/// unwind.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IcebergSchemaVisitor {
    /// Opaque engine state handed back as the first argument of every entry.
    pub ctx: *mut c_void,
    /// Creates a field list.
    pub make_field_list: IcebergMakeFieldListFn,
    /// Delivers a struct field.
    pub visit_struct: IcebergVisitStructFn,
    /// Delivers a list field.
    pub visit_list: IcebergVisitListFn,
    /// Delivers a map field.
    pub visit_map: IcebergVisitMapFn,
    /// Delivers a primitive field.
    pub visit_primitive: IcebergVisitPrimitiveFn,
}

/// A visited type, with its children (if any) already delivered.
#[derive(Debug)]
enum TypeNode {
    Primitive {
        kind: IcebergPrimitiveKind,
        precision: u32,
        scale: u32,
        width: u64,
    },
    Struct {
        child_list: usize,
    },
    List {
        element_id: i32,
        element_required: bool,
        child_list: usize,
    },
    Map {
        key_id: i32,
        value_id: i32,
        value_required: bool,
        child_list: usize,
    },
}

impl TypeNode {
    fn primitive(kind: IcebergPrimitiveKind) -> Self {
        Self::Primitive {
            kind,
            precision: 0,
            scale: 0,
            width: 0,
        }
    }
}

/// The post-order visitor's intermediate value: either a bare type or a
/// type that has been attached to a named field.
#[derive(Debug)]
enum Node {
    Type(TypeNode),
    Field {
        name: String,
        id: i32,
        required: bool,
        ty: TypeNode,
    },
}

fn internal(message: &str) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        format!("schema visitor protocol violated: {message}"),
    )
}

/// Drives an [`IcebergSchemaVisitor`].
struct Driver {
    visitor: IcebergSchemaVisitor,
}

impl Driver {
    fn make_list(&self, reserve: usize) -> usize {
        (self.visitor.make_field_list)(self.visitor.ctx, reserve)
    }

    /// Delivers one field into `list`.
    fn emit(&self, list: usize, name: &str, id: i32, required: bool, ty: &TypeNode) {
        let ctx = self.visitor.ctx;
        let name = StringSlice::borrowed(name);
        match *ty {
            TypeNode::Primitive {
                kind,
                precision,
                scale,
                width,
            } => (self.visitor.visit_primitive)(
                ctx, list, name, id, required, kind, precision, scale, width,
            ),
            TypeNode::Struct { child_list } => {
                (self.visitor.visit_struct)(ctx, list, name, id, required, child_list)
            }
            TypeNode::List {
                element_id,
                element_required,
                child_list,
            } => (self.visitor.visit_list)(
                ctx,
                list,
                name,
                id,
                required,
                element_id,
                element_required,
                child_list,
            ),
            TypeNode::Map {
                key_id,
                value_id,
                value_required,
                child_list,
            } => (self.visitor.visit_map)(
                ctx,
                list,
                name,
                id,
                required,
                key_id,
                value_id,
                value_required,
                child_list,
            ),
        }
    }

    /// Creates a list for `field`'s type and delivers it there under `name`.
    fn emit_nested(&self, name: &str, field: &NestedFieldRef, ty: TypeNode) -> usize {
        let list = self.make_list(1);
        self.emit(list, name, field.id, field.required, &ty);
        list
    }
}

fn expect_type(node: Node) -> iceberg::Result<TypeNode> {
    match node {
        Node::Type(ty) => Ok(ty),
        Node::Field { name, .. } => Err(internal(&format!(
            "expected a type but found field `{name}`"
        ))),
    }
}

impl SchemaVisitor for Driver {
    type T = Node;

    fn schema(&mut self, _schema: &Schema, value: Node) -> iceberg::Result<Node> {
        Ok(value)
    }

    fn field(&mut self, field: &NestedFieldRef, value: Node) -> iceberg::Result<Node> {
        Ok(Node::Field {
            name: field.name.clone(),
            id: field.id,
            required: field.required,
            ty: expect_type(value)?,
        })
    }

    fn r#struct(&mut self, _struct: &StructType, results: Vec<Node>) -> iceberg::Result<Node> {
        let child_list = self.make_list(results.len());
        for result in results {
            match result {
                Node::Field {
                    name,
                    id,
                    required,
                    ty,
                } => self.emit(child_list, &name, id, required, &ty),
                Node::Type(_) => return Err(internal("struct child without a field")),
            }
        }
        Ok(Node::Type(TypeNode::Struct { child_list }))
    }

    fn list(&mut self, list: &ListType, value: Node) -> iceberg::Result<Node> {
        let element = &list.element_field;
        let child_list = self.emit_nested("element", element, expect_type(value)?);
        Ok(Node::Type(TypeNode::List {
            element_id: element.id,
            element_required: element.required,
            child_list,
        }))
    }

    fn map(&mut self, map: &MapType, key: Node, value: Node) -> iceberg::Result<Node> {
        let key_field = &map.key_field;
        let value_field = &map.value_field;
        let child_list = self.make_list(2);
        self.emit(
            child_list,
            "key",
            key_field.id,
            key_field.required,
            &expect_type(key)?,
        );
        self.emit(
            child_list,
            "value",
            value_field.id,
            value_field.required,
            &expect_type(value)?,
        );
        Ok(Node::Type(TypeNode::Map {
            key_id: key_field.id,
            value_id: value_field.id,
            value_required: value_field.required,
            child_list,
        }))
    }

    fn primitive(&mut self, p: &PrimitiveType) -> iceberg::Result<Node> {
        use IcebergPrimitiveKind as K;
        let ty = match p {
            PrimitiveType::Boolean => TypeNode::primitive(K::Boolean),
            PrimitiveType::Int => TypeNode::primitive(K::Int),
            PrimitiveType::Long => TypeNode::primitive(K::Long),
            PrimitiveType::Float => TypeNode::primitive(K::Float),
            PrimitiveType::Double => TypeNode::primitive(K::Double),
            PrimitiveType::Decimal { precision, scale } => TypeNode::Primitive {
                kind: K::Decimal,
                precision: *precision,
                scale: *scale,
                width: 0,
            },
            PrimitiveType::Date => TypeNode::primitive(K::Date),
            PrimitiveType::Time => TypeNode::primitive(K::Time),
            PrimitiveType::Timestamp => TypeNode::primitive(K::Timestamp),
            PrimitiveType::Timestamptz => TypeNode::primitive(K::Timestamptz),
            PrimitiveType::TimestampNs => TypeNode::primitive(K::TimestampNs),
            PrimitiveType::TimestamptzNs => TypeNode::primitive(K::TimestamptzNs),
            PrimitiveType::String => TypeNode::primitive(K::String),
            PrimitiveType::Uuid => TypeNode::primitive(K::Uuid),
            PrimitiveType::Fixed(width) => TypeNode::Primitive {
                kind: K::Fixed,
                precision: 0,
                scale: 0,
                width: *width,
            },
            PrimitiveType::Binary => TypeNode::primitive(K::Binary),
            PrimitiveType::Unknown => TypeNode::primitive(K::Unknown),
            PrimitiveType::Geometry(_) | PrimitiveType::Geography(_) => {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    format!("schema type {p} is not supported"),
                ));
            }
        };
        Ok(Node::Type(ty))
    }

    fn variant(&mut self, _v: &VariantType) -> iceberg::Result<Node> {
        Ok(Node::Type(TypeNode::primitive(
            IcebergPrimitiveKind::Variant,
        )))
    }
}

/// Delivers `schema` through `visitor` and returns the root list id.
pub(crate) fn deliver(schema: &Schema, visitor: IcebergSchemaVisitor) -> iceberg::Result<usize> {
    let mut driver = Driver { visitor };
    match visit_schema(schema, &mut driver)? {
        Node::Type(TypeNode::Struct { child_list }) => Ok(child_list),
        other => Err(internal(&format!("schema root is {other:?}"))),
    }
}

/// Delivers one of the table's schemas through `visitor` and returns the id
/// of the root field list.
///
/// `schema_id` absent selects the current schema; an unknown id is
/// `IcebergErrorCodeInvalidArgument`, as is a null `visitor`. Geometry and
/// geography columns are `IcebergErrorCodeFeatureUnsupported`. Callbacks
/// already made before an error are not undone; the engine should discard
/// the lists it created.
// The visitor pointer is an engine argument with the same contract as every
// handle in this crate (valid for the call, checked for null); `unsafe` on an
// `extern "C"` signature would not change anything for the C caller.
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[unsafe(no_mangle)]
pub extern "C" fn iceberg_table_visit_schema(
    table: Shared<Table>,
    schema_id: IcebergOptionalI32,
    visitor: *const IcebergSchemaVisitor,
) -> IcebergResultUsize {
    with_table!(table, IcebergResultUsize, |t| {
        if visitor.is_null() {
            return Err(FfiError::new(
                IcebergErrorCode::InvalidArgument,
                "schema visitor is null",
            ));
        }
        // SAFETY: non-null, and the engine keeps the struct alive for the
        // duration of the call; it is copied out immediately.
        let visitor = unsafe { *visitor };
        let metadata = t.metadata();
        let schema = match schema_id.get() {
            None => metadata.current_schema(),
            Some(id) => metadata.schema_by_id(id).ok_or_else(|| {
                FfiError::new(
                    IcebergErrorCode::InvalidArgument,
                    format!("unknown schema id {id}"),
                )
            })?,
        };
        Ok(deliver(schema, visitor)?)
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::error::IcebergResultTag;
    use crate::error::test_support::take;
    use crate::runtime::iceberg_runtime_free;
    use crate::runtime::test_support::new_test_runtime;
    use crate::table::iceberg_table_free;
    use crate::table::test_support::load_fixture;

    /// A field as the engine would record it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestField {
        name: String,
        id: i32,
        required: bool,
        ty: TestType,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum TestType {
        Primitive {
            kind: IcebergPrimitiveKind,
            precision: u32,
            scale: u32,
            width: u64,
        },
        Struct {
            child_list: usize,
        },
        List {
            element_id: i32,
            element_required: bool,
            child_list: usize,
        },
        Map {
            key_id: i32,
            value_id: i32,
            value_required: bool,
            child_list: usize,
        },
    }

    /// Engine-side list store: list ids index `lists`.
    #[derive(Debug, Default)]
    struct TestTree {
        lists: Vec<Vec<TestField>>,
    }

    impl TestTree {
        fn list(&self, id: usize) -> &[TestField] {
            &self.lists[id]
        }

        fn field<'a>(&'a self, list: usize, name: &str) -> &'a TestField {
            self.list(list)
                .iter()
                .find(|f| f.name == name)
                .unwrap_or_else(|| panic!("no field {name} in list {list}"))
        }
    }

    fn tree(ctx: *mut c_void) -> std::sync::MutexGuard<'static, TestTree> {
        // SAFETY: `ctx` is the `Mutex<TestTree>` the test passed, alive for
        // the whole visit.
        let tree = unsafe { &*ctx.cast::<Mutex<TestTree>>() };
        tree.lock().expect("tree lock")
    }

    fn name_of(slice: StringSlice) -> String {
        // SAFETY: valid for the duration of the callback.
        unsafe { slice.as_str() }.expect("utf-8 name").to_owned()
    }

    fn push(
        ctx: *mut c_void,
        list: usize,
        name: StringSlice,
        id: i32,
        required: bool,
        ty: TestType,
    ) {
        tree(ctx).lists[list].push(TestField {
            name: name_of(name),
            id,
            required,
            ty,
        });
    }

    extern "C" fn make_field_list(ctx: *mut c_void, reserve: usize) -> usize {
        let mut t = tree(ctx);
        t.lists.push(Vec::with_capacity(reserve));
        t.lists.len() - 1
    }

    extern "C" fn visit_struct(
        ctx: *mut c_void,
        list: usize,
        name: StringSlice,
        id: i32,
        required: bool,
        child_list: usize,
    ) {
        push(
            ctx,
            list,
            name,
            id,
            required,
            TestType::Struct { child_list },
        );
    }

    extern "C" fn visit_list(
        ctx: *mut c_void,
        list: usize,
        name: StringSlice,
        id: i32,
        required: bool,
        element_id: i32,
        element_required: bool,
        child_list: usize,
    ) {
        push(
            ctx,
            list,
            name,
            id,
            required,
            TestType::List {
                element_id,
                element_required,
                child_list,
            },
        );
    }

    extern "C" fn visit_map(
        ctx: *mut c_void,
        list: usize,
        name: StringSlice,
        id: i32,
        required: bool,
        key_id: i32,
        value_id: i32,
        value_required: bool,
        child_list: usize,
    ) {
        push(
            ctx,
            list,
            name,
            id,
            required,
            TestType::Map {
                key_id,
                value_id,
                value_required,
                child_list,
            },
        );
    }

    extern "C" fn visit_primitive(
        ctx: *mut c_void,
        list: usize,
        name: StringSlice,
        id: i32,
        required: bool,
        kind: IcebergPrimitiveKind,
        precision: u32,
        scale: u32,
        width: u64,
    ) {
        push(
            ctx,
            list,
            name,
            id,
            required,
            TestType::Primitive {
                kind,
                precision,
                scale,
                width,
            },
        );
    }

    fn visitor_for(tree: &Mutex<TestTree>) -> IcebergSchemaVisitor {
        IcebergSchemaVisitor {
            ctx: (tree as *const Mutex<TestTree>).cast_mut().cast(),
            make_field_list,
            visit_struct,
            visit_list,
            visit_map,
            visit_primitive,
        }
    }

    fn primitive(kind: IcebergPrimitiveKind) -> TestType {
        TestType::Primitive {
            kind,
            precision: 0,
            scale: 0,
            width: 0,
        }
    }

    fn optional(name: &str, id: i32, ty: TestType) -> TestField {
        TestField {
            name: name.to_owned(),
            id,
            required: false,
            ty,
        }
    }

    #[test]
    fn primitive_kind_discriminants_are_stable() {
        use IcebergPrimitiveKind as K;
        let all = [
            K::Boolean,
            K::Int,
            K::Long,
            K::Float,
            K::Double,
            K::Decimal,
            K::Date,
            K::Time,
            K::Timestamp,
            K::Timestamptz,
            K::TimestampNs,
            K::TimestamptzNs,
            K::String,
            K::Uuid,
            K::Fixed,
            K::Binary,
            K::Unknown,
            K::Variant,
        ];
        for (i, kind) in all.iter().enumerate() {
            assert_eq!(*kind as usize, i);
        }
        assert_eq!(K::Variant as i32, 17);
    }

    #[test]
    fn visit_schema_builds_the_expected_tree() {
        use IcebergPrimitiveKind as K;
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, "simple_v2/metadata/v3.metadata.json");

        let store = Mutex::new(TestTree::default());
        let visitor = visitor_for(&store);
        let r = iceberg_table_visit_schema(table, IcebergOptionalI32::NONE, &visitor);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        let root = r.ok;
        let t = store.lock().expect("tree lock");

        let root_fields = t.list(root);
        assert_eq!(root_fields.len(), 15);
        let names: Vec<&str> = root_fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "id", "flag", "i", "f", "d", "dec", "day", "ts", "tstz", "s", "u", "b", "point",
                "tags", "attrs"
            ]
        );
        let ids: Vec<i32> = root_fields.iter().map(|f| f.id).collect();
        assert_eq!(ids, (1..=15).collect::<Vec<_>>());

        assert_eq!(
            root_fields[0],
            TestField {
                name: "id".to_owned(),
                id: 1,
                required: true,
                ty: primitive(K::Long),
            }
        );
        assert_eq!(root_fields[1], optional("flag", 2, primitive(K::Boolean)));
        assert_eq!(root_fields[2], optional("i", 3, primitive(K::Int)));
        assert_eq!(root_fields[3], optional("f", 4, primitive(K::Float)));
        assert_eq!(root_fields[4], optional("d", 5, primitive(K::Double)));
        assert_eq!(
            root_fields[5],
            optional(
                "dec",
                6,
                TestType::Primitive {
                    kind: K::Decimal,
                    precision: 10,
                    scale: 2,
                    width: 0,
                }
            )
        );
        assert_eq!(root_fields[6], optional("day", 7, primitive(K::Date)));
        assert_eq!(root_fields[7], optional("ts", 8, primitive(K::Timestamp)));
        assert_eq!(
            root_fields[8],
            optional("tstz", 9, primitive(K::Timestamptz))
        );
        assert_eq!(root_fields[9], optional("s", 10, primitive(K::String)));
        assert_eq!(root_fields[10], optional("u", 11, primitive(K::Uuid)));
        assert_eq!(root_fields[11], optional("b", 12, primitive(K::Binary)));

        // point struct<x double, y double>
        let point = t.field(root, "point");
        assert_eq!(point.id, 13);
        assert!(!point.required);
        let TestType::Struct { child_list } = point.ty else {
            panic!("point is {:?}", point.ty);
        };
        assert_ne!(child_list, root);
        assert_eq!(
            t.list(child_list),
            [
                optional("x", 16, primitive(K::Double)),
                optional("y", 17, primitive(K::Double)),
            ]
        );

        // tags list<string>
        let tags = t.field(root, "tags");
        assert_eq!(tags.id, 14);
        let TestType::List {
            element_id,
            element_required,
            child_list,
        } = tags.ty
        else {
            panic!("tags is {:?}", tags.ty);
        };
        assert_eq!(element_id, 18);
        assert!(!element_required);
        assert_eq!(
            t.list(child_list),
            [optional("element", 18, primitive(K::String))]
        );

        // attrs map<string, int>
        let attrs = t.field(root, "attrs");
        assert_eq!(attrs.id, 15);
        let TestType::Map {
            key_id,
            value_id,
            value_required,
            child_list,
        } = attrs.ty
        else {
            panic!("attrs is {:?}", attrs.ty);
        };
        assert_eq!((key_id, value_id, value_required), (19, 20, false));
        assert_eq!(
            t.list(child_list),
            [
                TestField {
                    name: "key".to_owned(),
                    id: 19,
                    required: true,
                    ty: primitive(K::String),
                },
                optional("value", 20, primitive(K::Int)),
            ]
        );

        // Four lists in total: root, point, tags, attrs.
        assert_eq!(t.lists.len(), 4);
        drop(t);

        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn explicit_schema_id_selects_a_schema() {
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, "simple_v2/metadata/v3.metadata.json");
        let store = Mutex::new(TestTree::default());
        let visitor = visitor_for(&store);

        let r = iceberg_table_visit_schema(table, IcebergOptionalI32::from(Some(0)), &visitor);
        assert_eq!(r.tag, IcebergResultTag::Ok, "{:?}", take(r.err));
        assert_eq!(store.lock().expect("tree lock").list(r.ok).len(), 15);

        let r = iceberg_table_visit_schema(table, IcebergOptionalI32::from(Some(999)), &visitor);
        assert_eq!(r.tag, IcebergResultTag::Err);
        let e = take(r.err).expect("error object");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("999"), "{}", e.message);

        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn null_visitor_is_invalid_argument() {
        let rt = new_test_runtime();
        let (table, _counters) = load_fixture(rt, "simple_v2/metadata/v3.metadata.json");
        let r = iceberg_table_visit_schema(table, IcebergOptionalI32::NONE, std::ptr::null());
        assert_eq!(r.tag, IcebergResultTag::Err);
        assert_eq!(
            take(r.err).map(|e| e.code),
            Some(IcebergErrorCode::InvalidArgument)
        );
        iceberg_table_free(table);
        iceberg_runtime_free(rt);
    }

    #[test]
    fn unsupported_primitive_is_reported() {
        use iceberg::spec::{GeometryType, NestedField, Type};
        let schema = Schema::builder()
            .with_fields([NestedField::optional(
                1,
                "geo",
                Type::Primitive(PrimitiveType::Geometry(GeometryType::default())),
            )
            .into()])
            .build()
            .expect("schema");
        let store = Mutex::new(TestTree::default());
        let err = deliver(&schema, visitor_for(&store)).expect_err("geometry must fail");
        assert_eq!(err.kind(), ErrorKind::FeatureUnsupported);
    }

    #[test]
    fn fixed_and_variant_carry_their_shape() {
        use iceberg::spec::{NestedField, Type};
        let schema = Schema::builder()
            .with_fields([
                NestedField::required(1, "fx", Type::Primitive(PrimitiveType::Fixed(16))).into(),
                NestedField::optional(2, "v", Type::Variant(VariantType)).into(),
            ])
            .build()
            .expect("schema");
        let store = Mutex::new(TestTree::default());
        let root = deliver(&schema, visitor_for(&store)).expect("deliver");
        let t = store.lock().expect("tree lock");
        assert_eq!(
            t.list(root),
            [
                TestField {
                    name: "fx".to_owned(),
                    id: 1,
                    required: true,
                    ty: TestType::Primitive {
                        kind: IcebergPrimitiveKind::Fixed,
                        precision: 0,
                        scale: 0,
                        width: 16,
                    },
                },
                optional("v", 2, primitive(IcebergPrimitiveKind::Variant)),
            ]
        );
    }
}
