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

//! Primitive values crossing the C boundary.
//!
//! An [`IcebergLiteral`] is a tagged, `#[repr(C)]` view of one Iceberg
//! primitive value. The engine builds one to hand a predicate constant to the
//! predicate builder; the crate builds one to report an identity partition
//! constant. Only the fields the `kind` documents are meaningful; the rest are
//! zero or empty.

use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};

use crate::error::{FfiError, FfiResult, IcebergErrorCode};
use crate::strings::StringSlice;

/// Which Iceberg primitive type an [`IcebergLiteral`] holds.
///
/// Discriminants equal the matching
/// [`IcebergPrimitiveKind`](crate::schema::IcebergPrimitiveKind) values, so
/// the engine can convert a column's kind into a literal kind by value.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergLiteralKind {
    /// `boolean`; `i` is 0 or 1.
    Boolean = 0,
    /// `int`; `i` holds the 32-bit value.
    Int = 1,
    /// `long`; `i`.
    Long = 2,
    /// `float`; `f` holds the value widened to 64 bits.
    Float = 3,
    /// `double`; `f`.
    Double = 4,
    /// `decimal(precision, scale)`; `u_hi`/`u_lo` hold the unscaled 128-bit
    /// mantissa, `precision` and `scale` the type parameters.
    Decimal = 5,
    /// `date`; `i` is days since the Unix epoch.
    Date = 6,
    /// `time`; `i` is microseconds since midnight.
    Time = 7,
    /// `timestamp`; `i` is microseconds since the Unix epoch.
    Timestamp = 8,
    /// `timestamptz`; `i` is microseconds since the Unix epoch, UTC.
    Timestamptz = 9,
    /// `timestamp_ns`; `i` is nanoseconds since the Unix epoch.
    TimestampNs = 10,
    /// `timestamptz_ns`; `i` is nanoseconds since the Unix epoch, UTC.
    TimestamptzNs = 11,
    /// `string`; `bytes` is UTF-8.
    String = 12,
    /// `uuid`; `bytes` is exactly 16 raw bytes, big-endian.
    Uuid = 13,
    /// `fixed[n]`; `bytes` holds the `n` raw bytes.
    Fixed = 14,
    /// `binary`; `bytes` holds the raw bytes.
    Binary = 15,
}

/// One primitive value, tagged by [`IcebergLiteralKind`].
///
/// `bytes` borrows memory owned by whoever built the literal and is valid only
/// for the call it is passed to. A literal with `is_null` set carries no
/// value; the predicate builder rejects it, and partition constants never
/// produce one (null constants are skipped).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IcebergLiteral {
    /// Which primitive type this is.
    pub kind: IcebergLiteralKind,
    /// Whether the value is SQL NULL (no payload field is meaningful).
    pub is_null: bool,
    /// Boolean, Int, Long, Date, Time, Timestamp*, TimestampNs*.
    pub i: i64,
    /// Decimal: high 64 bits of the two's-complement 128-bit mantissa.
    pub u_hi: u64,
    /// Decimal: low 64 bits of the mantissa.
    pub u_lo: u64,
    /// Float (widened) and Double.
    pub f: f64,
    /// String (UTF-8), Binary, Fixed and Uuid (16 raw bytes).
    pub bytes: StringSlice,
    /// Decimal precision.
    pub precision: u32,
    /// Decimal scale.
    pub scale: u32,
}

impl IcebergLiteral {
    /// A literal of `kind` with every payload field zero or empty.
    pub fn zero(kind: IcebergLiteralKind) -> Self {
        Self {
            kind,
            is_null: false,
            i: 0,
            u_hi: 0,
            u_lo: 0,
            f: 0.0,
            bytes: StringSlice::empty(),
            precision: 0,
            scale: 0,
        }
    }
}

/// Reassembles a 128-bit mantissa from its two halves.
pub fn mantissa(hi: u64, lo: u64) -> i128 {
    (((hi as u128) << 64) | lo as u128) as i128
}

/// Splits a 128-bit mantissa into `(hi, lo)` halves.
pub fn split_mantissa(m: i128) -> (u64, u64) {
    let bits = m as u128;
    ((bits >> 64) as u64, bits as u64)
}

fn invalid(message: String) -> FfiError {
    FfiError::new(IcebergErrorCode::InvalidArgument, message)
}

fn narrow_i32(value: i64, what: &str) -> FfiResult<i32> {
    i32::try_from(value).map_err(|_| invalid(format!("{what} literal {value} is out of range")))
}

/// Converts an engine literal into an iceberg [`Datum`].
///
/// # Safety
///
/// `lit.bytes` must be valid for the duration of the call (see
/// [`StringSlice::as_bytes`]).
pub unsafe fn to_datum(lit: &IcebergLiteral) -> FfiResult<Datum> {
    use IcebergLiteralKind as K;
    if lit.is_null {
        return Err(invalid(
            "null literals cannot be used in a predicate".to_owned(),
        ));
    }
    // SAFETY: forwarded to the caller.
    let bytes = unsafe { lit.bytes.as_bytes() };
    Ok(match lit.kind {
        K::Boolean => Datum::bool(lit.i != 0),
        K::Int => Datum::int(narrow_i32(lit.i, "int")?),
        K::Long => Datum::long(lit.i),
        K::Float => Datum::float(lit.f as f32),
        K::Double => Datum::double(lit.f),
        K::Decimal => {
            Datum::decimal_from_mantissa(mantissa(lit.u_hi, lit.u_lo), lit.precision, lit.scale)?
        }
        K::Date => Datum::date(narrow_i32(lit.i, "date")?),
        K::Time => Datum::time_micros(lit.i)?,
        K::Timestamp => Datum::timestamp_micros(lit.i),
        K::Timestamptz => Datum::timestamptz_micros(lit.i),
        K::TimestampNs => Datum::timestamp_nanos(lit.i),
        K::TimestamptzNs => Datum::timestamptz_nanos(lit.i),
        K::String => Datum::string(
            std::str::from_utf8(bytes)
                .map_err(|e| invalid(format!("string literal is not valid UTF-8: {e}")))?,
        ),
        K::Uuid => {
            let raw: [u8; 16] = bytes.try_into().map_err(|_| {
                invalid(format!(
                    "uuid literal must be 16 bytes, got {}",
                    bytes.len()
                ))
            })?;
            Datum::uuid(uuid::Uuid::from_bytes(raw))
        }
        K::Fixed => Datum::fixed(bytes.iter().copied()),
        K::Binary => Datum::binary(bytes.iter().copied()),
    })
}

/// Views an iceberg [`Datum`] as an engine literal.
///
/// The returned literal borrows `datum` for string and binary payloads and
/// `uuid_buf` for uuid payloads; both must outlive every use of the literal.
/// Datums that have no exact FFI representation (`AboveMax`/`BelowMin`, or an
/// unexpected type/literal pairing) are [`IcebergErrorCode::Unexpected`].
pub fn from_datum<'a>(datum: &'a Datum, uuid_buf: &'a mut [u8; 16]) -> FfiResult<IcebergLiteral> {
    use IcebergLiteralKind as K;
    use PrimitiveLiteral as L;
    use PrimitiveType as T;
    let mut lit = IcebergLiteral::zero(K::Boolean);
    match (datum.data_type(), datum.literal()) {
        (T::Boolean, L::Boolean(b)) => {
            lit.kind = K::Boolean;
            lit.i = i64::from(*b);
        }
        (T::Int, L::Int(v)) => {
            lit.kind = K::Int;
            lit.i = i64::from(*v);
        }
        (T::Date, L::Int(v)) => {
            lit.kind = K::Date;
            lit.i = i64::from(*v);
        }
        (T::Long, L::Long(v)) => {
            lit.kind = K::Long;
            lit.i = *v;
        }
        (T::Time, L::Long(v)) => {
            lit.kind = K::Time;
            lit.i = *v;
        }
        (T::Timestamp, L::Long(v)) => {
            lit.kind = K::Timestamp;
            lit.i = *v;
        }
        (T::Timestamptz, L::Long(v)) => {
            lit.kind = K::Timestamptz;
            lit.i = *v;
        }
        (T::TimestampNs, L::Long(v)) => {
            lit.kind = K::TimestampNs;
            lit.i = *v;
        }
        (T::TimestamptzNs, L::Long(v)) => {
            lit.kind = K::TimestamptzNs;
            lit.i = *v;
        }
        (T::Float, L::Float(v)) => {
            lit.kind = K::Float;
            lit.f = f64::from(v.0);
        }
        (T::Double, L::Double(v)) => {
            lit.kind = K::Double;
            lit.f = v.0;
        }
        (T::Decimal { precision, scale }, L::Int128(m)) => {
            lit.kind = K::Decimal;
            (lit.u_hi, lit.u_lo) = split_mantissa(*m);
            lit.precision = *precision;
            lit.scale = *scale;
        }
        (T::String, L::String(s)) => {
            lit.kind = K::String;
            lit.bytes = StringSlice::borrowed(s);
        }
        (T::Binary, L::Binary(v)) => {
            lit.kind = K::Binary;
            lit.bytes = StringSlice::from_bytes(v);
        }
        (T::Fixed(_), L::Binary(v)) => {
            lit.kind = K::Fixed;
            lit.bytes = StringSlice::from_bytes(v);
        }
        (T::Uuid, L::UInt128(u)) => {
            lit.kind = K::Uuid;
            *uuid_buf = u.to_be_bytes();
            lit.bytes = StringSlice::from_bytes(uuid_buf);
        }
        (ty, other) => {
            return Err(FfiError::new(
                IcebergErrorCode::Unexpected,
                format!("datum {other:?} of type {ty} cannot be represented as an FFI literal"),
            ));
        }
    }
    Ok(lit)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use iceberg::spec::Type;

    use super::*;

    fn with_i(kind: IcebergLiteralKind, i: i64) -> IcebergLiteral {
        let mut lit = IcebergLiteral::zero(kind);
        lit.i = i;
        lit
    }

    fn with_f(kind: IcebergLiteralKind, f: f64) -> IcebergLiteral {
        let mut lit = IcebergLiteral::zero(kind);
        lit.f = f;
        lit
    }

    fn with_bytes(kind: IcebergLiteralKind, bytes: &[u8]) -> IcebergLiteral {
        let mut lit = IcebergLiteral::zero(kind);
        lit.bytes = StringSlice::from_bytes(bytes);
        lit
    }

    fn decimal(m: i128, precision: u32, scale: u32) -> IcebergLiteral {
        let mut lit = IcebergLiteral::zero(IcebergLiteralKind::Decimal);
        (lit.u_hi, lit.u_lo) = split_mantissa(m);
        lit.precision = precision;
        lit.scale = scale;
        lit
    }

    /// `to_datum` then `from_datum`, returning the datum and the literal.
    fn round_trip(lit: IcebergLiteral) -> (Datum, IcebergLiteral, [u8; 16]) {
        let datum = unsafe { to_datum(&lit) }.expect("to_datum");
        let mut buf = [0u8; 16];
        let back = from_datum(&datum, &mut buf).expect("from_datum");
        assert_eq!(back.kind, lit.kind);
        assert!(!back.is_null);
        // Copy the buffer out so the caller can inspect bytes that borrow it.
        let bytes = unsafe { back.bytes.as_bytes() }.to_vec();
        let mut stable = [0u8; 16];
        stable[..bytes.len().min(16)].copy_from_slice(&bytes[..bytes.len().min(16)]);
        (datum, back, stable)
    }

    #[test]
    fn mantissa_halves_round_trip() {
        for m in [
            0i128,
            1,
            -1,
            i128::MAX,
            i128::MIN,
            -123_456_789_012_345_678_901_234_567_890,
        ] {
            let (hi, lo) = split_mantissa(m);
            assert_eq!(mantissa(hi, lo), m);
        }
    }

    #[test]
    fn literal_kind_matches_primitive_kind() {
        use IcebergLiteralKind as K;

        use crate::schema::IcebergPrimitiveKind as P;
        let pairs = [
            (K::Boolean, P::Boolean),
            (K::Int, P::Int),
            (K::Long, P::Long),
            (K::Float, P::Float),
            (K::Double, P::Double),
            (K::Decimal, P::Decimal),
            (K::Date, P::Date),
            (K::Time, P::Time),
            (K::Timestamp, P::Timestamp),
            (K::Timestamptz, P::Timestamptz),
            (K::TimestampNs, P::TimestampNs),
            (K::TimestamptzNs, P::TimestamptzNs),
            (K::String, P::String),
            (K::Uuid, P::Uuid),
            (K::Fixed, P::Fixed),
            (K::Binary, P::Binary),
        ];
        for (i, (k, p)) in pairs.iter().enumerate() {
            assert_eq!(*k as usize, i);
            assert_eq!(*k as i32, *p as i32);
        }
    }

    #[test]
    fn round_trips_every_kind() {
        use IcebergLiteralKind as K;

        let (d, back, _) = round_trip(with_i(K::Boolean, 1));
        assert_eq!(d, Datum::bool(true));
        assert_eq!(back.i, 1);

        let (d, back, _) = round_trip(with_i(K::Int, -42));
        assert_eq!(d, Datum::int(-42));
        assert_eq!(back.i, -42);

        let (d, back, _) = round_trip(with_i(K::Long, i64::MIN));
        assert_eq!(d, Datum::long(i64::MIN));
        assert_eq!(back.i, i64::MIN);

        let (d, back, _) = round_trip(with_f(K::Float, f64::from(f32::MAX)));
        assert_eq!(d, Datum::float(f32::MAX));
        assert_eq!(back.f, f64::from(f32::MAX));

        let (d, back, _) = round_trip(with_f(K::Double, f64::NAN));
        assert!(d.is_nan());
        assert!(back.f.is_nan());

        let m = -123_456_789_012_345_678_901_234_567_890i128;
        let (d, back, _) = round_trip(decimal(m, 38, 10));
        assert_eq!(d, Datum::decimal_from_mantissa(m, 38, 10).unwrap());
        assert_eq!(mantissa(back.u_hi, back.u_lo), m);
        assert_eq!((back.precision, back.scale), (38, 10));

        let (d, back, _) = round_trip(with_i(K::Date, 19785));
        assert_eq!(d, Datum::date(19785));
        assert_eq!(back.i, 19785);

        let (d, back, _) = round_trip(with_i(K::Time, 45_296_789_012));
        assert_eq!(d, Datum::time_micros(45_296_789_012).unwrap());
        assert_eq!(back.i, 45_296_789_012);

        let (d, back, _) = round_trip(with_i(K::Timestamp, 1_709_424_000_000_000));
        assert_eq!(d, Datum::timestamp_micros(1_709_424_000_000_000));
        assert_eq!(back.i, 1_709_424_000_000_000);

        let (d, back, _) = round_trip(with_i(K::Timestamptz, -1));
        assert_eq!(d, Datum::timestamptz_micros(-1));
        assert_eq!(back.i, -1);

        let (d, back, _) = round_trip(with_i(K::TimestampNs, 7));
        assert_eq!(d, Datum::timestamp_nanos(7));
        assert_eq!(back.i, 7);

        let (d, back, _) = round_trip(with_i(K::TimestamptzNs, -7));
        assert_eq!(d, Datum::timestamptz_nanos(-7));
        assert_eq!(back.i, -7);

        let (d, back, _) = round_trip(with_bytes(K::String, "".as_bytes()));
        assert_eq!(d, Datum::string(""));
        assert_eq!(back.bytes.len, 0);
        let (d, _, _) = round_trip(with_bytes(K::String, "ünïcødé ✓".as_bytes()));
        assert_eq!(d, Datum::string("ünïcødé ✓"));

        let uuid = uuid::Uuid::parse_str("12345678-9abc-def0-1234-56789abcdef0").unwrap();
        let (d, back, buf) = round_trip(with_bytes(K::Uuid, uuid.as_bytes()));
        assert_eq!(d, Datum::uuid(uuid));
        assert_eq!(back.bytes.len, 16);
        assert_eq!(&buf, uuid.as_bytes());

        let fixed: Vec<u8> = (0u8..16).collect();
        let (d, back, buf) = round_trip(with_bytes(K::Fixed, &fixed));
        assert_eq!(d, Datum::fixed(fixed.clone()));
        assert_eq!(back.bytes.len, 16);
        assert_eq!(buf.as_slice(), fixed.as_slice());

        let (d, back, buf) = round_trip(with_bytes(K::Binary, &[0, 255]));
        assert_eq!(d, Datum::binary([0u8, 255]));
        assert_eq!(back.bytes.len, 2);
        assert_eq!(&buf[..2], &[0, 255]);
    }

    #[test]
    fn null_literal_is_rejected() {
        let mut lit = with_i(IcebergLiteralKind::Int, 1);
        lit.is_null = true;
        let e = unsafe { to_datum(&lit) }.expect_err("null must fail");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("null"), "{}", e.message);
    }

    #[test]
    fn int_out_of_range_is_invalid_argument() {
        let e = unsafe { to_datum(&with_i(IcebergLiteralKind::Int, i64::from(i32::MAX) + 1)) }
            .expect_err("out of range");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        let e = unsafe { to_datum(&with_i(IcebergLiteralKind::Date, i64::from(i32::MIN) - 1)) }
            .expect_err("out of range");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        let e = unsafe { to_datum(&with_i(IcebergLiteralKind::Time, -1)) }.expect_err("negative");
        assert_eq!(e.code, IcebergErrorCode::DataInvalid);
    }

    #[test]
    fn uuid_needs_sixteen_bytes() {
        let e = unsafe { to_datum(&with_bytes(IcebergLiteralKind::Uuid, &[1, 2, 3])) }
            .expect_err("short uuid");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
        assert!(e.message.contains("16"), "{}", e.message);
    }

    #[test]
    fn invalid_utf8_string_is_invalid_argument() {
        let e = unsafe { to_datum(&with_bytes(IcebergLiteralKind::String, &[0xff, 0xfe])) }
            .expect_err("bad utf-8");
        assert_eq!(e.code, IcebergErrorCode::InvalidArgument);
    }

    #[test]
    fn decimal_mantissa_too_wide_is_data_invalid() {
        let e = unsafe { to_datum(&decimal(10_000, 4, 2)) }.expect_err("too wide");
        assert_eq!(e.code, IcebergErrorCode::DataInvalid);
    }

    #[test]
    fn above_max_datum_cannot_be_represented() {
        let above = Datum::long(i64::MAX)
            .to(&Type::Primitive(PrimitiveType::Int))
            .expect("conversion yields AboveMax");
        assert!(matches!(above.literal(), PrimitiveLiteral::AboveMax));
        let mut buf = [0u8; 16];
        let e = from_datum(&above, &mut buf).expect_err("no FFI form");
        assert_eq!(e.code, IcebergErrorCode::Unexpected);
    }
}
