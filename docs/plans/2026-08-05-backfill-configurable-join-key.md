# Configurable Join Keys for Backfill Implementation Plan

**Goal:** Generalize the backfill job so callers can explicitly select a persisted physical field as the row join key, while preserving the current primary-key join as the default and leaving a clean extension point for future logical keys such as `(file path, row number)`.

**Architecture:** Introduce a public join-key specification and a separately resolved runtime model. Phase 1 exposes only the existing primary-key strategy and a single persisted physical-field strategy. The resolved model is component-based and uses internal canonical join-column aliases, so the read, validation, and join pipeline can later accept multi-component logical keys without another rewrite. The per-segment writer path remains unchanged because it already depends only on `$segment_id`, `$row_offset`, and the fields being written.

**Technology stack:** Scala, Spark SQL/DataFrame API, Spark DataSource V2, ScalaTest, sbt, scalafmt

---

## Current behavior and constraints

The current implementation assumes the Milvus primary key throughout the pre-write pipeline:

- `MilvusBackfill.run` resolves `(pkName, pkFieldId)` from the snapshot or client.
- `applyColumnMapping` requires the mapped backfill data to contain the primary-key field. Without an explicit mapping, it requires a literal `pk` input column and renames it to the collection PK name.
- Every mapped column except the PK is treated as a target field.
- Backfill-side duplicate detection uses `countDistinct(pkName)`.
- `readCollectionWithMetadata` always reads the PK field ID first.
- Schema compatibility and all three merge modes join by `Seq(pkName)`.
- Metrics and documentation describe matches as PK matches.

The downstream writer does not need the PK. After the join, `processSegments` projects only `$segment_id`, `$row_offset`, target fields, and internal statistics flags. It repartitions by segment and restores physical row order using `$row_offset`. Therefore, the writer, V2/V3 dispatch, manifest handling, and result artifact generation are outside the main change surface.

## Scope

Phase 1 includes:

- Preserve primary-key join behavior when no join-key option is supplied.
- Add an explicit physical-field join strategy for persisted scalar fields present in the snapshot schema.
- Permit an explicit physical join key even when the snapshot schema has no field marked as a primary key.
- Support input-column renaming through the existing `columnMapping` mechanism.
- Require exact type compatibility and a non-null, unique row key.
- Make internal join execution component-based even though the phase-1 CLI accepts one physical field.
- Keep the result JSON wire shape unchanged.

Phase 1 does not include:

- Logical keys synthesized from file metadata, row positions, expressions, or UDFs.
- Joining directly on `$segment_id` / `$row_offset` as a public row identity.
- Many-to-one or one-to-many join cardinality.
- Updating the selected join-key field in the same backfill operation.
- Expanding the currently unsupported client-only ADDFIELD path.
- Changes to native readers, JNI, V2/V3 writers, or Milvus commit behavior.

## Pull request split

Implementation is split into two stacked pull requests. The split is based on
review boundaries rather than the numbered tasks below.

### PR1: Generalize the internal join pipeline without changing behavior

Suggested title:

```text
refactor: decouple backfill join pipeline from primary keys
```

PR1 keeps the public configuration, CLI, result JSON, and default PK behavior
unchanged. It introduces only the internal execution seam needed by PR2:

- Private resolved join-key/component models using canonical internal aliases.
- A prepared-input model that separates join columns from writer target fields.
- Component-based source projection and join-key type compatibility checks.
- `performJoin` accepting multiple internal join columns.
- Generic backfill-side null/uniqueness validation while still validating the
  existing PK input.
- Regression tests proving all existing PK and merge-mode behavior is unchanged.
- An internal multi-component join test proving the future logical-key seam.

PR1 consists primarily of the internal portions of Tasks 1, 4, 5, 6, and 7.
It must not add `BackfillConfig.joinKey` or the `--join-key` CLI flag.

### PR2: Expose persisted physical join keys end to end

Suggested title:

```text
feat: support physical join keys for backfill
```

PR2 is based on PR1 and delivers the complete user-visible feature:

- Public `BackfillJoinKey.PrimaryKey` / `PhysicalField` configuration.
- `BackfillConfig.joinKey`, defaulting to `PrimaryKey`.
- The `--join-key` CLI flag.
- Snapshot-schema resolution for persisted physical fields, including schemas
  without a primary-key marker.
- Physical-key column-mapping behavior.
- Explicit source-side non-null/uniqueness validation and type restrictions.
- End-to-end orchestration, logs, documentation, and the complete regression
  suite.

PR2 consists of the public portions of Tasks 1-3, the physical-key cases from
Tasks 4-6, and Tasks 8-10. Configuration, CLI, resolver, validation, and user
documentation must ship together so no merged state accepts a join-key option
that is ignored or performs an unsafe non-unique join.

Logical file/row keys remain a future, separate feature PR after their reader
metadata and snapshot-stability contract is defined.

Implementation status as of 2026-08-05:

- PR1 implementation is present in the working tree and its unit/reader
  regression suites pass.
- PR2 has not started; no public join-key configuration or CLI flag is exposed.

## Public behavior

Programmatic configuration:

```scala
sealed trait BackfillJoinKey extends Serializable

object BackfillJoinKey {
  case object PrimaryKey extends BackfillJoinKey
  final case class PhysicalField(name: String) extends BackfillJoinKey
}

case class BackfillConfig(
    // existing fields ...
    mode: String = MilvusOption.BackfillModeCoalesce,
    joinKey: BackfillJoinKey = BackfillJoinKey.PrimaryKey
)
```

CLI configuration:

```text
--join-key <snapshot-field-name>
```

Omitting `--join-key` selects `BackfillJoinKey.PrimaryKey` and preserves the current behavior.

Examples:

```bash
# Existing behavior: input `pk` is mapped implicitly to the collection PK.
spark-submit ... --parquet data.parquet --snapshot snapshot.json

# The parquet already uses the physical field name.
spark-submit ... \
  --join-key external_row_id \
  --parquet data.parquet \
  --snapshot snapshot.json

# The parquet key column has a different name.
spark-submit ... \
  --join-key external_row_id \
  --column-mapping source_row_id:external_row_id,new_vec:embedding \
  --parquet data.parquet \
  --snapshot snapshot.json
```

Compatibility matrix:

| Join configuration | Column mapping | Required input key | Result |
|---|---|---|---|
| Omitted/default PK | None | Literal `pk` | Existing implicit `pk -> <collection PK>` behavior |
| Omitted/default PK | Present | One mapping target equals the collection PK | Existing explicit mapping behavior |
| Physical field `k` | None | Literal `k` | All other columns are target fields |
| Physical field `k` | Present | One mapping target equals `k` | The mapped key is consumed for joining, not written |

---

### Task 1: Add the join-key specification and runtime model

**Files:**

- Create: `src/main/scala/operations/backfill/BackfillJoinKey.scala`
- Modify: `src/main/scala/operations/backfill/BackfillConfig.scala`
- Test: `src/test/scala/operations/backfill/BackfillConfigTest.scala`

**Step 1: Write failing model/default tests**

Add tests covering:

```scala
test("backfill defaults to collection primary-key join") {
  val config = BackfillConfig.forTest("c")
  config.joinKey shouldBe BackfillJoinKey.PrimaryKey
}

test("backfill accepts an explicit physical-field join key") {
  val config = BackfillConfig
    .forTest("c")
    .copy(joinKey = BackfillJoinKey.PhysicalField("external_row_id"))
  config.validate() shouldBe Right(())
}

test("backfill rejects a blank physical-field join key") {
  val config = BackfillConfig
    .forTest("c")
    .copy(joinKey = BackfillJoinKey.PhysicalField("  "))
  config.validate().isLeft shouldBe true
}
```

**Step 2: Run the tests and verify failure**

Run:

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillConfigTest"
```

Expected: FAIL because the join-key model and config field do not exist.

**Step 3: Implement the minimal public and resolved models**

Create:

```scala
sealed trait BackfillJoinKey extends Product with Serializable

object BackfillJoinKey {
  case object PrimaryKey extends BackfillJoinKey
  final case class PhysicalField(name: String) extends BackfillJoinKey
}

private[backfill] final case class ResolvedJoinComponent(
    sourceColumn: String,
    fieldId: Long,
    sourceField: StructField,
    internalColumn: String
)

private[backfill] final case class ResolvedJoinKey(
    kind: String,
    components: Seq[ResolvedJoinComponent]
) {
  require(components.nonEmpty, "resolved join key must have at least one component")
  def sourceColumns: Seq[String] = components.map(_.sourceColumn)
  def internalColumns: Seq[String] = components.map(_.internalColumn)
}
```

Use deterministic reserved aliases such as `__bf_join_0__`. Keep the resolved representation as a sequence even though phase 1 creates exactly one component.

Append `joinKey` to `BackfillConfig` with a default so existing named construction sites continue to compile. Extend `validate()` to reject blank physical-field names.

**Step 4: Run the tests and verify success**

Run the same targeted suite.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/BackfillJoinKey.scala src/main/scala/operations/backfill/BackfillConfig.scala src/test/scala/operations/backfill/BackfillConfigTest.scala
git commit -m "enhance: add backfill join key model"
```

### Task 2: Add CLI parsing for an explicit physical join key

**Files:**

- Modify: `src/main/scala/operations/backfill/BackfillApp.scala`
- Modify: `src/test/scala/operations/backfill/BackfillAppTest.scala`

**Step 1: Write failing parser tests**

Cover:

- `--join-key external_row_id` is accepted.
- The parsed value becomes `BackfillJoinKey.PhysicalField("external_row_id")` when building the config.
- `--join-key` without a value produces the existing clear missing-value error.
- A value containing only whitespace is rejected by config validation.
- Omitting the flag keeps `PrimaryKey`.

If config construction remains embedded in `main`, extract a package-visible helper such as:

```scala
private[backfill] def buildConfig(parsed: Map[String, String]): BackfillConfig
```

This avoids testing `main` through `System.exit` and gives future join-key CLI strategies one parser boundary.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillAppTest"
```

Expected: FAIL because `join-key` is not a known key/value flag.

**Step 3: Implement CLI parsing**

- Add `join-key` to `KvFlags`.
- Map a supplied value to `BackfillJoinKey.PhysicalField(value.trim)`.
- Map an omitted value to `BackfillJoinKey.PrimaryKey`.
- Add `--join-key` to the usage documentation at the top of `BackfillApp`.

Do not introduce a public `join-key-type` flag in phase 1. Future logical-key CLI syntax should be added separately after its metadata contract is defined.

**Step 4: Run the tests and verify success**

Run the same targeted suite.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/BackfillApp.scala src/test/scala/operations/backfill/BackfillAppTest.scala
git commit -m "enhance: accept physical join key for backfill"
```

### Task 3: Resolve the configured join key from the snapshot schema

**Files:**

- Modify: `src/main/scala/operations/backfill/MilvusBackfill.scala`
- Test: `src/test/scala/operations/backfill/MilvusBackfillTest.scala`

**Step 1: Write failing resolver tests**

Extract a package-visible pure helper:

```scala
private[backfill] def resolveJoinKey(
    schema: CollectionSchema,
    spec: BackfillJoinKey
): Either[BackfillError, ResolvedJoinKey]
```

Cover:

- `PrimaryKey` resolves the field marked `isPrimaryKey=true`.
- `PrimaryKey` fails with the current clear error if no PK exists.
- `PhysicalField("external_row_id")` resolves the exact snapshot field name and ID.
- A physical field works when no snapshot field is marked as a PK.
- A missing physical field fails and lists available field names.
- Case mismatches fail instead of relying on Spark's case-insensitive resolution.
- Unsupported physical key types fail before data is read.

Phase-1 supported key types should be deliberately narrow and stable for equality joins:

```text
ByteType, ShortType, IntegerType, LongType, StringType, BinaryType
```

Reject floating-point, vector, array, map, struct, and JSON-like fields. Additional scalar types can be added later with explicit equality-semantics tests.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.MilvusBackfillTest"
```

Expected: FAIL because join-key resolution is still hard-coded to PK lookup.

**Step 3: Implement the resolver**

For `PrimaryKey`, retain the current snapshot lookup behavior. For `PhysicalField`, resolve directly from `metadata.collection.schema.fields`. Convert the selected field through `MilvusSnapshotReader.fieldToStructField` and assign its internal alias.

In `run`, replace the early `(pkName, pkFieldId)` block with a resolved key. Because ADDFIELD already requires snapshot schema later in the method, do not expand client-only behavior in this task. Preserve the existing client-path error semantics outside the new resolver.

**Step 4: Run the tests and verify success**

Run the same targeted suite.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/MilvusBackfill.scala src/test/scala/operations/backfill/MilvusBackfillTest.scala
git commit -m "enhance: resolve configurable backfill join keys"
```

### Task 4: Separate input join columns from target fields

**Files:**

- Modify: `src/main/scala/operations/backfill/MilvusBackfill.scala`
- Modify: `src/test/scala/operations/backfill/ColumnMappingTest.scala`

**Step 1: Write failing input-preparation tests**

Replace the PK-specific helper contract with a prepared-input contract:

```scala
private[backfill] final case class PreparedBackfillData(
    dataFrame: DataFrame,
    joinColumns: Seq[String],
    targetFieldNames: Seq[String]
)
```

Add tests covering:

- Default PK plus no mapping still renames literal `pk` to the internal join alias.
- Default PK plus explicit mapping finds the mapping entry targeting the real PK field.
- Explicit physical key plus no mapping consumes the same-named parquet column as the join key.
- Explicit physical key plus mapping consumes the source column whose mapping target equals the physical field name.
- The consumed join column is excluded from `targetFieldNames`.
- A missing join-key mapping fails with a join-key-specific message rather than a primary-key message.
- At least one non-key target field remains required.
- Duplicate mapping targets and rename chains/swaps retain their current behavior.
- Internal names such as `__bf_join_0__` and `__bf_matched__` cannot collide with user target fields.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.ColumnMappingTest"
```

Expected: FAIL because `applyColumnMapping` still requires the PK and derives targets by excluding `pkName`.

**Step 3: Implement prepared input projection**

Refactor `applyColumnMapping` into a helper such as:

```scala
private[backfill] def prepareBackfillData(
    df: DataFrame,
    joinKey: ResolvedJoinKey,
    joinSpec: BackfillJoinKey,
    userMapping: Option[Map[String, String]]
): Either[BackfillError, PreparedBackfillData]
```

Rules:

1. Determine which raw parquet column supplies each join component.
2. Project each join component to its internal alias.
3. Project non-key mapping entries to Milvus target field names.
4. Drop unlisted columns when a mapping is provided.
5. Return target field names explicitly; do not infer them later by subtracting the PK.

For the default PK/no-mapping path only, preserve the legacy literal `pk` alias. Do not apply that alias to an explicitly configured physical key.

The selected join field is identity-only for the operation. Reject or consume it as the key; never include it in the writer target schema.

**Step 4: Run the tests and verify success**

Run the same targeted suite.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/MilvusBackfill.scala src/test/scala/operations/backfill/ColumnMappingTest.scala
git commit -m "refactor: separate backfill join keys from target fields"
```

### Task 5: Generalize source projection and schema compatibility

**Files:**

- Modify: `src/main/scala/operations/backfill/MilvusBackfill.scala`
- Modify: `src/test/scala/operations/backfill/BackfillModeTest.scala`
- Modify: `src/test/scala/operations/backfill/MilvusBackfillTest.scala`

**Step 1: Write failing read-projection tests**

Cover:

- The source reader's `fieldIDs` option begins with the selected physical field ID rather than the PK field ID.
- Snapshot read schema contains the selected physical join field, requested source-side target fields, `$segment_id`, and `$row_offset` in the expected order.
- Repeated field IDs are removed defensively while preserving schema/field-ID order.
- The physical source join column is normalized to the same internal alias used by the prepared backfill input.
- Join compatibility validates every component and reports the physical field name and both Spark types.

Expose a narrow test helper for building read options/schema rather than requiring a native reader integration test for every assertion.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillModeTest com.zilliz.spark.connector.operations.backfill.MilvusBackfillTest"
```

Expected: FAIL because the reader and validator accept only `pkFieldId` / `pkName`.

**Step 3: Generalize the source read**

Change `readCollectionWithMetadata` to accept `ResolvedJoinKey` rather than `pkFieldId`:

```scala
private def readCollectionWithMetadata(
    spark: SparkSession,
    config: BackfillConfig,
    joinKey: ResolvedJoinKey,
    snapshotMetadata: Option[SnapshotMetadata],
    v2Segments: Seq[V2SegmentInfo],
    extraReadFields: Seq[(String, Long, StructField)]
): Either[BackfillError, DataFrame]
```

Build `ReaderFieldIDs` and the supplied Spark schema from:

```text
resolved join components ++ source-side target fields
```

After loading, rename the physical source key columns to their internal aliases. Continue validating the presence of `$segment_id` and `$row_offset` exactly as today.

Rename `validateSchemaCompatibility` to `validateJoinKeyCompatibility` and compare all resolved components. Keep target-field compatibility in `validateMergeableFieldTypes` unchanged.

**Step 4: Run the tests and verify success**

Run the same targeted suites.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/MilvusBackfill.scala src/test/scala/operations/backfill/BackfillModeTest.scala src/test/scala/operations/backfill/MilvusBackfillTest.scala
git commit -m "enhance: read configured join fields for backfill"
```

### Task 6: Enforce row-key cardinality and null invariants

**Files:**

- Modify: `src/main/scala/operations/backfill/MilvusBackfill.scala`
- Modify: `src/test/scala/operations/backfill/BackfillModeTest.scala`

**Step 1: Write failing validation tests**

Add a reusable helper that validates a DataFrame key using the internal join columns. Cover:

- A unique, non-null key succeeds.
- A duplicate key fails.
- A null key fails with a distinct null-key message rather than being reported only as a duplicate.
- Error messages identify whether the invalid side is `backfill data` or `source snapshot`.
- Multi-component keys are handled correctly by the internal helper even though phase 1 does not expose them publicly.
- A duplicate tuple fails while repeated individual component values in otherwise unique tuples succeed.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillModeTest"
```

Expected: FAIL because current validation is PK-specific and does not distinguish nulls from duplicates.

**Step 3: Implement exact key validation**

Use one exact aggregation per side:

```scala
val keyStruct = struct(joinColumns.map(col): _*)
val hasNull = joinColumns.map(c => col(c).isNull).reduce(_ || _)

df.agg(
  count(lit(1)).as("rows"),
  sum(when(hasNull, 1L).otherwise(0L)).as("null_key_rows"),
  countDistinct(when(!hasNull, keyStruct)).as("distinct_valid_keys")
)
```

The invariant is:

```text
null_key_rows == 0 && distinct_valid_keys == rows
```

The backfill input is already cached; retain that behavior. For an explicit physical source key, persist the source DataFrame with `MEMORY_AND_DISK`, validate it once, reuse it for the join, and unpersist it in the existing outer cleanup path. For the default PK strategy, the Milvus schema already defines the key invariant, so source-side uniqueness validation may be skipped to preserve current performance.

Log row count and distinct join-key count using generic terminology. Preserve `totalBackfillDataRows` semantics.

**Step 4: Run the tests and verify success**

Run the same targeted suite.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/MilvusBackfill.scala src/test/scala/operations/backfill/BackfillModeTest.scala
git commit -m "enhance: validate backfill join key cardinality"
```

### Task 7: Generalize the join while preserving all merge modes

**Files:**

- Modify: `src/main/scala/operations/backfill/MilvusBackfill.scala`
- Modify: `src/test/scala/operations/backfill/BackfillModeTest.scala`

**Step 1: Write failing non-PK join tests**

For each mode, build source data containing both a PK and a different physical row key, then join on the physical key:

- `replace`: matched rows take file values and unmatched source rows receive null target values.
- `coalesce`: non-null source values win, and file values fill source nulls.
- `overwrite`: matched rows take file values including null, and unmatched rows retain source values.
- Match and provenance flags retain their current meanings.
- The output has exactly one row per source row.
- The physical key and internal aliases are absent from the writer projection.

Also add an internal two-component join test to prove the execution layer is ready for a future logical key.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillModeTest"
```

Expected: FAIL because `performJoin` accepts a single `pkName`.

**Step 3: Generalize `performJoin`**

Change its signature to:

```scala
private[backfill] def performJoin(
    originalDF: DataFrame,
    backfillDF: DataFrame,
    joinColumns: Seq[String],
    newFieldNames: Seq[String],
    mode: String
): DataFrame
```

Use the existing Spark using-column left join with the internal aliases:

```scala
originalDF.join(backfillWithFlag, joinColumns, "left")
```

Keep all coalesce, overwrite, replace, match-flag, and provenance logic otherwise unchanged. Update comments from `PK matched` to `join key matched`.

Do not alter `processSegments`, writer row layout, V2/V3 logic, or output ordering.

**Step 4: Run the tests and verify success**

Run the same targeted suite.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/MilvusBackfill.scala src/test/scala/operations/backfill/BackfillModeTest.scala
git commit -m "enhance: join backfill data on configurable keys"
```

### Task 8: Integrate the resolved key through `run`

**Files:**

- Modify: `src/main/scala/operations/backfill/MilvusBackfill.scala`
- Modify: `src/test/scala/operations/backfill/MilvusBackfillTest.scala`
- Modify: `src/test/scala/operations/backfill/ColumnMappingTest.scala`

**Step 1: Add integration-level regression tests**

Cover the orchestration sequence without requiring native writes where possible:

- Default configuration follows the same PK resolution, implicit `pk` mapping, target-field selection, and join behavior as before.
- Explicit physical key resolves from a schema with no primary-key marker.
- The selected field ID reaches reader options.
- A differently named parquet key reaches the internal join alias through `columnMapping`.
- The selected key is not present in `newFieldNameToId` or the writer target schema.
- Missing key fields, unsupported key types, nulls, duplicates, and type mismatches fail before segment writes begin.
- Existing vector normalization still applies only to target fields, not join fields.

**Step 2: Run the tests and verify failure**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.MilvusBackfillTest com.zilliz.spark.connector.operations.backfill.ColumnMappingTest"
```

Expected: FAIL until `run` uses the new helpers end to end.

**Step 3: Rewire `run`**

The new orchestration order should be:

1. Load snapshot metadata and V2 segment metadata.
2. Resolve `BackfillJoinKey` into `ResolvedJoinKey`.
3. Read raw backfill parquet.
4. Prepare join columns and target fields through `prepareBackfillData`.
5. Normalize vector target columns.
6. Cache and validate the backfill input key.
7. Resolve target field IDs and source-side merge fields.
8. Read source rows using the resolved key fields.
9. Validate the source key when required and validate key type compatibility.
10. Join using internal component aliases.
11. Pass the unchanged post-join contract to `processSegments`.

Add a driver log entry such as:

```text
Backfill join key: kind=physical, fields=external_row_id, fieldIds=123
```

Do not add join-key fields to `BackfillResult.toJson` in this phase. The JSON is consumed as part of Milvus commit handling, and avoiding a wire-shape change keeps this feature isolated from downstream parser compatibility.

**Step 4: Run the tests and verify success**

Run the same targeted suites.

Expected: PASS.

**Step 5: Commit**

```bash
git add src/main/scala/operations/backfill/MilvusBackfill.scala src/test/scala/operations/backfill/MilvusBackfillTest.scala src/test/scala/operations/backfill/ColumnMappingTest.scala
git commit -m "enhance: integrate configurable backfill join keys"
```

### Task 9: Update documentation and metric terminology

**Files:**

- Modify: `src/main/scala/operations/backfill/README.md`
- Modify: `docs/design-snapshot-backfill.md`
- Modify: `docs/user-guide-snapshot-backfill.md`
- Modify: `docs/backfill-result-json-format.md`
- Modify: `src/main/scala/operations/backfill/BackfillResult.scala`
- Modify: relevant tests under `src/test/scala/operations/backfill/`

**Step 1: Update user-facing documentation**

Document:

- PK remains the default join strategy.
- `--join-key` selects a persisted physical snapshot field.
- The field must be non-null, unique, and type-compatible with the parquet key.
- Mapping examples for same-name and renamed input keys.
- The join key cannot be backfilled in the same operation.
- Logical file/row keys are not yet supported.
- `$row_offset` is used to restore segment write order and is not automatically a stable logical row identity.

**Step 2: Generalize terminology without changing JSON names**

Update comments and prose from `PK match` to `join-key match` around:

- `MatchFlagCol`
- `matchedRowCount`
- `totalMatchedRows`
- mode descriptions
- result JSON documentation

Keep the existing JSON field names because they are already generic enough. Do not add or rename serialized fields.

**Step 3: Run documentation-adjacent tests**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillResultTest com.zilliz.spark.connector.operations.backfill.BackfillAppTest"
```

Expected: PASS.

**Step 4: Commit**

```bash
git add src/main/scala/operations/backfill/README.md docs/design-snapshot-backfill.md docs/user-guide-snapshot-backfill.md docs/backfill-result-json-format.md src/main/scala/operations/backfill/BackfillResult.scala src/test/scala/operations/backfill
git commit -m "docs: describe configurable backfill join keys"
```

### Task 10: Format and run the complete backfill regression suite

**Files:**

- All files changed above

**Step 1: Format**

```bash
sbt scalafmtAll
```

Expected: SUCCESS.

**Step 2: Run focused tests**

```bash
sbt "testOnly com.zilliz.spark.connector.operations.backfill.BackfillConfigTest com.zilliz.spark.connector.operations.backfill.BackfillAppTest com.zilliz.spark.connector.operations.backfill.ColumnMappingTest com.zilliz.spark.connector.operations.backfill.BackfillModeTest com.zilliz.spark.connector.operations.backfill.BackfillResultTest com.zilliz.spark.connector.operations.backfill.MilvusBackfillTest com.zilliz.spark.connector.operations.backfill.VectorBackfillSupportTest"
```

Expected: PASS.

**Step 3: Run the broader reader regression suites**

The feature changes reader field projection, so also run:

```bash
sbt "testOnly com.zilliz.spark.connector.read.MilvusLoonPartitionReaderTest com.zilliz.spark.connector.read.MilvusPackedV2PartitionReaderTest com.zilliz.spark.connector.sources.MilvusScanClientSnapshotTest"
```

Expected: PASS.

**Step 4: Check formatting and diff hygiene**

```bash
git diff --check
git status --short
```

Expected: no whitespace errors and only intended files changed.

---

## Logical-key extension seam

The phase-1 implementation must not model a join key as only a single schema field name inside the execution pipeline. A future logical key may need multiple source and input components and may not correspond to a Milvus field ID at all.

A later extension can add a new public specification, for example:

```scala
final case class LogicalFileRow(
    inputFileColumn: String,
    inputRowNumberColumn: String
) extends BackfillJoinKey
```

Its resolver would be responsible for:

1. Requesting stable source metadata columns from the reader.
2. Defining the snapshot/version scope in which file identity and row number are stable.
3. Normalizing source and input components to `__bf_join_0__`, `__bf_join_1__`, and so on.
4. Returning a multi-component `ResolvedJoinKey`.

The following phase-1 components should then remain unchanged:

- Key null/uniqueness validation.
- Type compatibility validation.
- `performJoin`.
- Merge-mode semantics.
- Per-segment repartitioning and `$row_offset` sorting.
- V2/V3 writing and commit artifacts.

Do not treat `$row_offset` alone as this future logical key. It currently identifies physical position within a segment read and is also used to restore write order; compaction, file replacement, or a different snapshot may change that position. File/row identity requires an explicit reader and snapshot-stability contract before it becomes a supported public join strategy.

## Acceptance criteria

- Existing jobs that omit `joinKey` or `--join-key` behave exactly as before.
- Existing `pk` implicit mapping and explicit PK column mappings remain valid.
- A snapshot-backed collection or external table can join on a configured persisted scalar field.
- An explicit physical key works even when the schema has no primary-key marker.
- Invalid, null, duplicate, missing, unsupported, or type-incompatible keys fail before any segment is written.
- All merge modes preserve their existing matched/unmatched and null semantics.
- The selected join field is not written as a target field.
- Per-segment row counts and physical row order remain unchanged.
- Result JSON remains backward compatible.
- The internal join implementation accepts multiple resolved components, enabling a later logical file/row key without another join-layer redesign.
