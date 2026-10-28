# Primary-key statistics read samples

Generated once on 2026-10-21 with `PrimaryKeyStatsBuilder` and
`PrimaryKeyStatsEncoder` at spark-milvus commit
`5dd6f42dadc70ed20b7b719ed479ba83581eaaa8`. These are synthetic public keys,
not application data. Ordinary tests read these bytes without the builders.

- `long-N.json`: Int64 field 100 with just key N (1, 7, 9, 101, 202, 303, 1000).
- `long-1-2.json`: Int64 field 100 with keys 1 and 2.
- `wrong-field.json`: key 1 under field 101, for wrong-field rejection.
- `compound-first.json` and `compound-second.json`: keys 7 and 9;
  `compound.json` contains both objects in that order.
- `varchar.json`: VarChar field 101 with `row-7`, `row-0`, `row-99`, `中文`;
  bounds are `row-0` and `中文`.
- `varchar-unicode.json`: VarChar field 101 with `row-0`, `中文`, `Ａ`,
  U+E000, U+1F600, and the empty string. UTF-8 bounds are the empty string and
  U+1F600; every inserted key must be found.

The pruner's candidate 1000 is absent from the key-1 and key-1/2 filters.
Tests validate this before asserting partition/global-L0 retention,
compound-file precedence, corrupt-file conservatism and reuse without IO.
The Milvus-produced 3000-key fixture remains separately in `src/test/data`.
