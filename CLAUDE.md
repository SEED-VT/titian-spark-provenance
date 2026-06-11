# BigDebug 4.0

Migration of Titian (record-level data provenance, VLDB '16) — and later BigDebug
(interactive Spark debugger, ICSE '16) — from a Spark 2.1 fork to an attach-as-a-library
for Spark 4.x, covering Scala, PySpark, and Spark SQL.

**Current scope: Titian only.** BigDebug primitives are deferred until the Titian port
is complete and validated.

- Read `MIGRATION_PLAN.md` for the full strategy and task list before starting work —
  especially its "Caveats and known limitations" section (cluster deployment, Spark
  version coupling, unvalidated surfaces) before claiming support for anything.
- SQL/DataFrame provenance work follows `SQL_LINEAGE_PLAN.md` (phases 0–4 and twelve
  caveats); current active workstream.
- `TOUCHPOINT_INVENTORY.md` maps every file the old fork modified to a Spark 4
  extension point, and states the fidelity requirements.
- The original Spark 2.1 fork (branch `titian-2.1`) is cloned at `reference/titian-2.1/`
  (read-only reference — do not modify it). Everything for this project — reference
  clones, code, docs — stays under this directory.
- The new implementation must match the semantics of Titian (VLDB '16) and BigDebug
  (ICSE '16) as defined in the papers and the original implementation; the original
  `LineageSuite` test programs are the acceptance bar.
- Target: Spark 4.1.x, Scala 2.13, JDK 17+. Library only — never fork Spark sources.
