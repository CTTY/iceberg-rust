<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Test fixtures

Iceberg tables used by the `iceberg-ffi` unit tests. Do not edit by hand.

Every table was written by iceberg-rust itself (`MemoryCatalog` over the local
file system, the Parquet data-file writer and `Transaction::fast_append`), then
the catalog's `0000N-<uuid>.metadata.json` files were renamed to
`vN.metadata.json`, the `metadata-log` entries rewritten to those names and
`version-hint.text` written without a trailing newline.

All locations recorded inside the metadata are relative to a repository root
and start with `test/data/generated/`. The tests rewrite that prefix to the
absolute path of this directory before any file past the metadata JSON is read,
which also exercises the relocation support.

| directory | contents |
|---|---|
| `simple_v2` | unpartitioned, every primitive type plus struct/list/map, 2 appends of 4 rows (metadata `v1..v3`, hint `3`) |
| `partitioned_identity_v2` | identity(region) + bucket[4](user_id), 3 appends of 6 rows (metadata `v1..v4`, hint `4`) |
| `partitioned_transforms_v2` | bucket[4](id) + truncate[2](category) + day(ts), 2 appends (8 + 6 rows), one data file per partition tuple (metadata `v1..v3`, hint `3`) |
| `schema_evolution_v2` | 3 rows as (a int, b string, c int); then drop `c`, add `d` long, rename `b` -> `b_renamed`, widen `a` to long, reorder; 3 more rows with the new schema (metadata `v1..v5`, hint `5`) |
| `types_v2` | every primitive type (incl. decimal(38, 10), time, uuid, fixed(16)) plus struct/list/map, 5 rows with NULLs in row 4 (metadata `v1..v2`, hint `2`) |
| `simple_v1` | format-version-1 table (id, name, value), 2 appends of 3 rows (metadata `v1..v3`, hint `3`) |
| `simple_v2_gzip` | copy of `simple_v2` whose only metadata JSON file is the gzip-compressed `v3.gz.metadata.json` |
| `simple_v2_moved` | full copy of `simple_v2`; its metadata still records location `test/data/generated/simple_v2` |
| `not_iceberg` | `metadata/v1.metadata.json` is JSON that is not table metadata |
