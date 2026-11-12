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

//! Nullable scalars for the C boundary.
//!
//! C has no `Option<i64>`, so optional integers travel as a `{ has_value,
//! value }` pair. `value` is zero whenever `has_value` is false.

/// An optional 64-bit integer.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IcebergOptionalI64 {
    /// Whether `value` is meaningful.
    pub has_value: bool,
    /// The value; zero when `has_value` is false.
    pub value: i64,
}

impl IcebergOptionalI64 {
    /// The absent value.
    pub(crate) const NONE: Self = Self {
        has_value: false,
        value: 0,
    };

    /// Converts back to an `Option`.
    pub fn get(self) -> Option<i64> {
        self.has_value.then_some(self.value)
    }
}

impl From<Option<i64>> for IcebergOptionalI64 {
    fn from(value: Option<i64>) -> Self {
        match value {
            Some(value) => Self {
                has_value: true,
                value,
            },
            None => Self::NONE,
        }
    }
}

/// An optional 32-bit integer.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IcebergOptionalI32 {
    /// Whether `value` is meaningful.
    pub has_value: bool,
    /// The value; zero when `has_value` is false.
    pub value: i32,
}

impl IcebergOptionalI32 {
    /// The absent value.
    pub(crate) const NONE: Self = Self {
        has_value: false,
        value: 0,
    };

    /// Converts back to an `Option`.
    pub fn get(self) -> Option<i32> {
        self.has_value.then_some(self.value)
    }
}

impl From<Option<i32>> for IcebergOptionalI32 {
    fn from(value: Option<i32>) -> Self {
        match value {
            Some(value) => Self {
                has_value: true,
                value,
            },
            None => Self::NONE,
        }
    }
}

/// An optional unsigned 64-bit integer.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IcebergOptionalU64 {
    /// Whether `value` is meaningful.
    pub has_value: bool,
    /// The value; zero when `has_value` is false.
    pub value: u64,
}

impl IcebergOptionalU64 {
    /// The absent value.
    pub(crate) const NONE: Self = Self {
        has_value: false,
        value: 0,
    };

    /// Converts back to an `Option`.
    pub fn get(self) -> Option<u64> {
        self.has_value.then_some(self.value)
    }
}

impl From<Option<u64>> for IcebergOptionalU64 {
    fn from(value: Option<u64>) -> Self {
        match value {
            Some(value) => Self {
                has_value: true,
                value,
            },
            None => Self::NONE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_u64() {
        assert_eq!(IcebergOptionalU64::from(None).get(), None);
        assert_eq!(
            IcebergOptionalU64::from(Some(u64::MAX)).get(),
            Some(u64::MAX)
        );
        assert_eq!(IcebergOptionalU64::from(None), IcebergOptionalU64::NONE);
    }

    #[test]
    fn round_trip_i64() {
        assert_eq!(IcebergOptionalI64::from(None).get(), None);
        assert_eq!(IcebergOptionalI64::from(Some(-7)).get(), Some(-7));
        assert_eq!(IcebergOptionalI64::from(None), IcebergOptionalI64::NONE);
    }

    #[test]
    fn round_trip_i32() {
        assert_eq!(IcebergOptionalI32::from(None).get(), None);
        assert_eq!(IcebergOptionalI32::from(Some(3)).get(), Some(3));
        assert_eq!(IcebergOptionalI32::from(None).value, 0);
    }
}
