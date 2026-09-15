<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->


# Conflict validation predicates — companion to RFC-0003

[RFC-0003 (Stateful Transactions and Snapshot Production)](0003_stateful_transaction.md)
fixes *when* validation runs (per attempt, against the current transaction-local
table, selected by the action — section 4.3 step 1) and *what survives a rebase*
(the preserve/recompute table in section 5.1). It deliberately leaves the
concrete conflict predicates to companion work (section 8.1 step 4, section
8.2). This document is that companion: it enumerates *what* the predicates are,
so nothing from the earlier design document is lost. Each predicate is expressed
as a function of **action intent** (preserved for the execution) and
**base-dependent inputs** (recomputed for every attempt), mirroring Java's
`MergingSnapshotProducer` so behavior stays cross-implementation compatible.

## Common machinery

Every predicate scans the same shape of window:

```
window(base, starting_snapshot_id, parent) =
    ancestors of `parent` back to (exclusive) `starting_snapshot_id`,
    resolved through `base`
```

filtered to snapshots whose `operation` is in a per-predicate set, collecting
that snapshot's own manifests (data or delete content). Java:
`validationHistory(...)`.

Two properties worth carrying over as explicit rules:

* **Reachability is itself a predicate.** If the walk cannot connect `parent`
  back to `starting_snapshot_id` (expired/unreachable history), validation
  fails — it never silently validates a partial window. (Java: the
  `lastSnapshot.parentId() == startingSnapshotId` check.)
* **Format-version gate.** Delete-file predicates return trivially clean on
  v1 tables (`formatVersion < 2`); DV predicates apply from v3.

Operation sets (Java constants):

| Set | Operations |
|---|---|
| ADDED_FILES | append, overwrite |
| DATA_FILES_EXIST | overwrite, replace, delete |
| DATA_FILES_EXIST_SKIP_DELETE | overwrite, replace |
| ADDED_DELETE_FILES | overwrite, delete |
| ADDED_DVS | overwrite, delete, replace |

## The matrix

Lifetime columns use the section 5.1 terms: **preserve** = action intent, held
for the whole execution across attempts; **recompute** = derived from the
current attempt's table, never carried across a rebase without a validity
check.

| # | Predicate (Java anchor) | Question it answers | Scans | Action config (preserve) | Base-dependent (recompute) | Failure |
|---|---|---|---|---|---|---|
| P1 | Conflicting appends — `validateAddedDataFiles` | Did anyone add data files that could match my filter/partitions since I started? | DATA manifests of ADDED_FILES snapshots in window | `starting_snapshot_id`, conflict filter (expression or partition set) | window contents, matched manifest entries | "Found conflicting files matching %s" |
| P2 | Conflicting new deletes — `validateNoNewDeleteFiles` | Did anyone add delete files that could apply to records matching my filter? | DELETE manifests of ADDED_DELETE_FILES snapshots | `starting_snapshot_id`, data filter / partition set | `DeleteFileIndex` over window, starting sequence number (re-resolved from base; fails if starting snapshot expired) | "Found new conflicting delete files…" |
| P3 | New deletes for my files — `validateNoNewDeletesForDataFiles` | Did anyone add deletes that apply to *these specific* data files (at or before my starting sequence number)? | same index as P2, then per-file `forDataFile` | starting snapshot, optional data filter, the file list, `ignore_equality_deletes` flag | index + per-file resolution | "found new delete for replaced data file: %s" |
| P4 | Conflicting removals — `validateDeletedDataFiles` | Did anyone delete data files matching my filter/partitions? | DATA manifests, entries with deleted status in window | starting snapshot, filter/partition set | window contents | "Found conflicting deleted files…" |
| P5 | Referenced files exist — `validateDataFilesExist` | Are the files I reference (rewrite / apply row deltas to) still live? | DATA manifests of DATA_FILES_EXIST[_SKIP_DELETE] snapshots; non-ADDED entries ∩ required set | starting snapshot, required file set, `skip_deletes`, optional filter | window contents, matched removals | "Cannot commit, missing data files: %s" |
| P6 | Concurrent DVs — `validateAddedDVs` (v3) | Did anyone add deletion vectors for data files I'm also writing DVs for? | DELETE manifests of ADDED_DVS snapshots | starting snapshot, conflict filter, my DV'd file set | window contents | conflict on the shared data file |
| P7 | Replaced manifests exist — `validateDeletedManifests` (RewriteManifests) | Are the manifests I'm replacing still part of the current base? | current base manifest list only | replaced-manifest set | base manifest list | "Manifest is missing: %s" |
| P8 | Required deletes present — apply-time `ManifestFilterManager` missing-paths check | Do the files I must delete actually exist to be deleted? | apply-time filtering of base manifests | files-to-delete set, `fail_missing_delete_paths` | filtered manifests | "Missing required files to delete: %s" |

P8 is apply-time rather than validate-time in Java, but under RFC-0003 both
land in the same place: attempt-local work derived from the current base,
inside the merging producer's manifest filtering (section 4.3 step 2). A
`fail_missing_delete_paths` toggle keeps Java parity; operations that replace
files should default it on, since an add-half without its remove-half is a
silent duplication.

## Which operations run which predicates

| Operation | Predicates (with API toggle) |
|---|---|
| FastAppend / MergeAppend | none (append is conflict-free by construction) |
| OverwriteFiles | P1 (`validate_no_conflicting_appends` / serializable), P2+P4 over the row filter and P3 over explicitly deleted files (`validate_no_conflicting_deletes`) |
| RowDelta (first merging consumer, section 4.1) | P5 (always when files are referenced; `skip_deletes = !validate_deletes`), P1 (serializable), P2+P3 (`validate_no_conflicting_delete_files`), P6 (always, v3), P8 (`validate_deletes`) |
| RewriteFiles (compaction) | P3 over replaced files — with `ignore_equality_deletes` when the rewrite preserves data sequence numbers (equality deletes at higher sequence numbers still apply to the rewritten files, so only position deletes conflict); P8 for the replaced set |
| ReplacePartitions | P1 over replaced partitions (serializable), P4+P2 over replaced partitions (unless conflicting-deletes validation is disabled) |
| DeleteFiles | P8 |
| RewriteManifests | P7 (+ local file-count consistency) |

Isolation levels are just toggle presets: **serializable** = appends conflict
too (P1 on); **snapshot** = only delete/existence conflicts (P1 off). The
toggles themselves are action configuration — lifetime **preserve**.

## How this composes with the section 5.1 table

* Every predicate's *configuration* column is the "Action intent and resolved
  logical identity — preserve for the execution" row.
* Every predicate's *result* is the "Metadata reads and processed validation
  history — reuse under the same semantic validity rule" row: a rebase from
  base N to N′ extends the window from `(start, N]` to `(start, N′]`. Full
  recomputation is always correct; per-snapshot scan results keyed by snapshot
  id MAY be reused because ancestors already scanned are immutable — but, as
  section 5.1 puts it, cached history or an earlier successful check cannot
  substitute for the attempt's obligation to establish its own coverage.
* The starting sequence number is *value-stable* but must be **re-resolved**
  from each new base (and the resolution can fail — expired starting
  snapshot ⇒ validation error, fail-closed).

## Per-predicate forced-conflict tests (section 8.1 step 5 verification)

Each test has the same skeleton: configure the action, let attempt 1 pass
validation, land a conflicting commit, force the retry, assert attempt 2
fails validation **and wrote no metadata files**.

* **P1**: overwrite with filter `part = A` (serializable); concurrent append
  into `A` ⇒ fail. Concurrent append into `B` ⇒ pass (filter precision).
* **P2/P3**: rewrite files F1..Fn; concurrent position delete against F1 ⇒
  fail. With sequence-number-preserving rewrite + concurrent *equality*
  delete ⇒ pass (`ignore_equality_deletes`). Cross-check: same equality
  delete without sequence preservation ⇒ fail.
* **P4**: overwrite by filter; concurrent delete of a file matching the
  filter ⇒ fail.
* **P5**: row delta referencing F1; concurrent rewrite that removes F1 ⇒
  fail with the missing-file error, not a corrupt commit.
* **P6** (v3): two concurrent DV writers on the same data file ⇒ second
  fails.
* **P7**: rewrite manifests; concurrent append (changes manifest list) ⇒
  fail/retry recomputes from the new manifest list.
* **P8**: delete of a path not in the table with fail-missing on ⇒ fail;
  off ⇒ no-op.
* **Reachability**: expire the starting snapshot between attempts ⇒
  validation error naming both snapshots, no partial-window pass.

