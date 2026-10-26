# Bounded Canonical Lance Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Root coordinates one product implementer per task and independent review; implementers do not spawn agents.

**Goal:** Implement and qualify an inactive, owner-affine canonical Lance layout that can continue across bounded partitions while preserving existing exact evidence and migration identities.

**Architecture:** Keep new rows for each admitted owner in one home partition; globally route and reuse canonical IDs through derived metadata in the existing coordination database. Extend prepare, identical durable replay and atomic ACK with explicit layout versions; keep original registration/job domains and existing public exact operations unchanged. Prove ordinary whole-owner streaming first, then qualify rollover and full retained capacity.

**Tech Stack:** Python 3.12; existing supervised SQLite coordination/source workers; pinned Lance 4.0.1 and Arrow in exact workers; ordinary `hashlib`; existing guardian, `SemanticSession` and safe test runner.

**Spec:** [2026-10-25-bounded-lance-layout-design.md](../specs/2026-10-25-bounded-lance-layout-design.md). This is a Task 3c subplan of [2026-10-24-unified-lance-memory.md](2026-10-24-unified-lance-memory.md), not a replacement for reviewed Tasks 3b1/3b2a/3b2b.

## Global Constraints

- Baseline: independently approved `162a94f2e9fed205768fc3b6fa46d26bdeef900a` (Task 3b2b plus the focused full-session/UID admission fix). No source interface changed from `a610fad`. Do not redo or silently alter reviewed source contracts.
- Keep exact workers at 192 MiB task / 256 MiB aggregate, reads 2 s and writes 3 s. Source, exact, queue and verification stages consume the same 5 s session deadline.
- Source-specific profiles and transport limits remain unchanged: SQLite source 128/192 MiB and 2 s; native source 512/640 MiB and its current 2 s stage; native synthetic fixture stages at most 5 s. Preserve existing queue guards.
- Preserve 512 KiB complete frames, 256 combined entries/finalizations, 384 KiB decoded and 480 KiB ASCII body bounds; complete envelopes must fit too.
- Require at most 1,024 unindexed rows and 32 unindexed fragments after append. Count batch/live receipts; the current conservative maximum is 513 physical rows, not 256.
- Keep pressure refusal, global admission, preemption and descendant cleanup. Never increase limits to make qualification pass. Keep hosts engine/model-free and external networking disabled.
- Preserve original text, canonical IDs, provenance, exposure, metadata, receipts and source lineage. Preserve original archives, protected sources and SQLite rollback stores.
- SQLite stores coordination and derived metadata only: no text, source/metadata/scopes/caller JSON bodies, frames, durable text outbox, SHA state or partial-proof journal.
- Preserve `sqlite-migration-v1`, `native-migration-v1`, their registration/job ID derivation, existing source cursors and old exact/public request/receipt formats. Layout extensions have separate explicit versions; reject unsupported/mixed versions.
- Roll between admitted owners, never within a batch or halfway through an owner. Coalesce only within one source binding, source phase and home partition.
- One writable partition at a time; sealed partitions remain readable. No compaction, cleanup, source deletion, redistribution, new model, caller-selected executable/budget or generic provider framework.
- This plan adds private fixture/operator primitives only. No ordinary Task 4 retrieval routing, semantic coverage claim, production mover, installation, capture/service change, merge or activation.
- One product writer per task. Root grants the exclusive native slot for concrete SQLite/Lance test scope before execution. Pure contract checks need no native slot; every engine test uses supervised workers and explicit safe-runner selection.
- A resource refusal is evidence: retain intent/artifacts/accounting, stop that qualification sequence and report it. No automatic retry, tuning matrix, new cap or alternate fixture to manufacture a pass.

## Review Focus

- A shared canonical ID with a different first owner/source binding must reuse only already visible identical evidence; foreign staged records and changed provenance must conflict (Task 3).
- Unicode/NUL-rich fields, empty occurrences and final rows still in the append frame must hash the exact original ordered bytes without text sorting/materialization (Task 1).
- Lost append/ACK replies and a corrupt or incomplete directory must not yield duplicate IDs, false absence, early owner visibility or double counters (Tasks 2–3).
- A packed prefix ending before a phase/home/frame boundary must regenerate identically after interruption, including original native before/next cursors (Task 4).
- Underestimated large owners and retained orphan/index debt must refuse before further mutation; they remain counted incomplete and cannot be hidden by rollover or a completion claim (Tasks 3 and 5).

---

## File and protocol map

Keep modules focused; do not reorganize unrelated storage code.

| File | Responsibility |
| --- | --- |
| New `cognitive_castle/bulk_owner_stream.py` | Engine-free ordered original-byte owner verification; no persistent state. |
| Existing `bulk_exact_store.py`, `lance_exact_worker.py`, `semantic_runtime.py` | Pinned streaming adapter, exact maintenance/admission, fixed private layout operation dispatch and unchanged guards. |
| New `cognitive_castle/lance_layout_codec.py` | Pure strict schemas and deterministic IDs for layout plans, targets, intents, routes and source windows. |
| New `cognitive_castle/lance_layout_directory.py` | Derived metadata tables and bounded SQL operations using a supplied connection; safe read-only route access inside exact workers. Task 4 extends existing native binding, active-owner lifecycle, gates and bounded owner spans together for an ordered admitted prefix. |
| Existing `semantic_queue.py`, `semantic_store_worker.py`, `migration_codec.py` | Extend the existing job transaction for layout prepare/ACK; retain all v1 paths and job identities. Task 4 preserves each owner's original source-cursor association and atomically verifies/ACKs the complete packed prefix. |
| New `cognitive_castle/partitioned_exact_store.py` | Fixed partition paths, bounded global resolver, routed exact operations and partition maintenance. |
| Existing `migration_coordinator.py` | Private layout tick, fixed destination recovery and same-tick proof/ACK. |
| Existing SQLite/native source codec, runtime, worker and exporter modules | Versioned body-free owner planning and bounded source-view coalescing; default v1 behavior unchanged. |
| New scoped tests below | Pure contracts and separately supervised engine fixtures; no palace access. |

Use JSON dictionaries on existing worker boundaries. `lance_layout_codec.py` defines strict validators for these named contracts, not a permissive arbitrary-metadata dictionary:

- `OwnerPlan`: `format="lance-owner-plan-v1"`, source binding/phase/key, owner ID, manifest hash, original text/member/field count/hash/length summaries, and conservative numerical expansion counters for rows, encoded/retained bytes, batches, fragments, versions, artifacts, index outputs and temporary space. It contains no manifest bodies. Planning is not proof of publication or production authority.
- `LayoutTarget`: `format="lance-layout-target-v1"`, generation identity, immutable qualification-envelope hash, sequential partition ID, owner-plan hashes and reservation ID. Paths are derived internally as `<generation>/exact/partitions/<partition_id>`; no arbitrary route paths.
- `Route`: canonical ID, kind, row hash, partition ID, committed version/receipt reference and owner publication-gate reference. State distinguishes reserved, staged and published; foreign staging is not reusable.
- `LayoutIntent`: `format="lance-layout-interval-v1"`, original migration job/source/domain bindings, before/after cursors, fixed target, fixed source window/packing version, actual bulk request digest/counts, bounded route candidates and finalized-owner references. Use a new domain-separated layout interval ID; do not change v1 `interval_id` or registration hashes.
- `LayoutVerified`: `format="lance-layout-verified-v1"`, exact intent hash, actual identical-replay receipt, bounded route observations, grouped owner/live proof observations. It is current-tick evidence passed to atomic ACK, never saved as partial progress.
- `LayoutBatchResult`: `format="lance-layout-batch-result-v1"`, the unchanged compact bulk receipt nested as `receipt`, plus bounded verified `Route` observations. Only actual durable identical replay may supply those observations for ACK; a first append's success is insufficient. Preflight the complete prospective ACK against existing `migration_codec.MAX_METADATA` (192 KiB) before publication as well as the 512 KiB transport frame; deterministic packing may be smaller than 256 entries.
- `LayoutSourceWindow`: `format="lance-layout-source-v1"`, action `plan` or `export`, original descriptor/cursor and a bounded fixed allowed-owner prefix/plan hashes for export. Planning returns `OwnerPlan` values; export returns original bulk entries/finalizations plus each finalized owner's original phase/key/cursor association. Source binding and original descriptor/cursor formats do not change.

New fixed queue operations are `queue_layout_bind(job_id, plans, envelope)`, `queue_layout_prepare(job_id, expected_cursor, intent)`, `queue_layout_ack(job_id, intent_id, verified_summary)` and read-only `queue_layout_routes(ids)`. They use the same `queue.sqlite3` and migration job rows; layout tables store placement/reservations/routes, not a second migration job domain. Existing `queue_migration_state` returns an explicitly versioned layout attachment for a layout-enabled job; old v1 entry points refuse an attached layout job rather than misinterpreting its pending interval.

Exact worker additions are `layout_import_batch(intent_id, batch_id, source_binding, entries, finalizations) -> LayoutBatchResult`, `layout_read(operation, arguments)` and `layout_maintain(partition_id)`. `layout_read` has a closed read-only whitelist of the existing `read_sources/page/receipt/bulk_owner/bulk_page/bulk_receipt` actions plus Task 3's `directory_page`; existing actions preserve their inner result contracts. Root is the registered generation, never a caller-selected partition. Task 4 adds `layout_verify_owners(intent_id, version)` for grouped proof. Write/replay/maintenance stages remain 3 s; all reads/grouped checks remain 2 s.

## Qualification inputs and commands

Use `/Users/ladislavbihari/.echelon/venv/bin/python` and its adjacent `ruff`. Commands below are exact scoped selections; outer safe-runner timeouts bound the suite/fixture sequence, not individual worker limits. No tests or engines are run while writing this plan.

Before the Task 1 large diagnostic, root reviews and freezes `.superpowers/sdd/2026-10-24-unified-lance-memory/layout-52mib-qualification.json`: source fixture identity/hash, destination/run roots, complete owner numerical expansion, maximum rows/retained bytes/fragments/versions/artifacts, all retained index/temp/orphan allowances, free-space requirement, fixed native scan/maintenance profile and total case schedule. The candidate is the existing 52 MiB/6,656-member logical fixture; no synthetic cardinality ladder. Existing 16 MiB pool/64 MiB tracked spill settings may be used only as this explicitly frozen qualification profile, not inferred production defaults. Missing/invalid manifest means no diagnostic start. Exceeded envelope or resource refusal preserves evidence and blocks dependent tasks; root decides next action.

Task 5 uses an equivalent root-frozen `layout-rollover-qualification.json`, populated from permitted actual metadata inventory and prior measurements. Neither manifest changes public/runtime budgets. They are concrete run inputs reviewed before execution, not unfinished implementation choices.

### Task 1: Ordinary ordered owner verification and the known large-owner gate

**Files:** Create `bulk_owner_stream.py`, `tests/test_bulk_owner_stream.py`, `tests/test_lance_owner_stream.py`; modify `bulk_exact_store.py` narrowly. Task 1's ordered dispatch is confined to the fixed supervised fixture worker in `tests/test_lance_owner_stream.py`; do not add an unregistered ordered operation to the ordinary exact worker/runtime. Reuse existing source and guardian/session mechanisms; no source conversion redesign.

**Interfaces:** Produce `verify_owner_rows(rows, manifest, pending, *, resolve_reused) -> None` in `bulk_owner_stream.py`; `rows` is an iterator of six-string canonical rows, `pending` a bounded current-frame ID map, and `resolve_reused(ids) -> dict[id,row]` is a bounded resolver. Failure uses existing corrupt/conflict store outcomes. Add internal `OrderedBulkExactStore(directory, *, qualification, writable=False)` as a `BulkExactStore` subclass, with `_owner_rows(dataset, owner, pending)` and an ordered `_verify_owner` override. Define strict `OrderedQualification` in `bulk_owner_stream.py`: `format="ordered-owner-qualification-v1"`, frozen manifest hash, exact private destination/parent identity, owner/source binding, expected pre-operation version (or explicitly authorized before/after pair for lost-reply reconciliation), and numerical ceilings for physical rows, fragments, versions, artifact entries/depth, total logical/allocated bytes and maximum individual file bytes. It admits a **dedicated one-owner dataset only** in Task 1; it is not a caller-selected public storage profile.

The hash-frozen fixture runner/worker binds the root-approved qualification manifest and its expected digest before execution. Its private `ordered_import_batch` action accepts the existing bulk arguments and a qualification reference/hash only; it rejects a mismatch with that frozen run binding and cannot accept replacement ceilings/destination paths from the request. It constructs the qualification, invokes the subclass's inherited **real** `import_batch`, and uses the existing exact guard/session chain at 3 s/192/256 MiB. Missing qualification refuses before mkdir, artifact enumeration or native open. Original `BulkExactStore`, ordinary worker operations and arbitrary legacy batch order remain unchanged. Task 3 derives equivalent qualification from its registered home/envelope rather than this fixture manifest. Keep full `_verify_batch` and canonical validators; no saved hash progress or fake publication boundary.

- [x] Write pure RED assertions `test_stream_checks_original_bytes_and_pending_tail`, `test_stream_rejects_missing_duplicate_reordered_or_foreign_pages`, and `test_stream_uses_actual_reused_bytes`. Assert exact original text/membership/field digests and counts for Unicode, NUL, empty and multi-page fields; changed offsets/IDs/exposure or missing bytes must fail. A bounded single-pass iterator must work without materializing/sorting the owner.
- [x] Run RED, then implement the pure verifier and rerun GREEN:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 20 tests/test_bulk_owner_stream.py --noconftest -x -q`
- [x] Implement Task 1 qualification enforcement **before native open**: retain every existing parent/private-file/no-symlink/no-hardlink/entry-type check, but replace this subclass's recursive `Path.iterdir()` walk with streaming `os.scandir()` and a depth/entry-bounded stack. Count directories, all retained manifests/indexes, regular files and orphan/temp artifacts; reject on the first ceiling+1 entry or bytes/depth violation. Check maximum individual file bytes, including manifests, before parsing native metadata. Never sort/materialize all names or call the unbounded base walk first. Revalidate the qualification/path/version under the existing writer lock before append. Fixture append/maintenance preflight reserves conservative resulting geometry within the frozen envelope; postchecks cannot retroactively authorize an overshoot. Unknown acknowledgment leaves the fixture charged and unqualified pending the frozen reconciliation case.
- [x] Implement the pinned iterator over **all fragments of that admitted dedicated dataset**, in the opened manifest's insertion order: this complete dataset is Task 1's scan span, so no Task 2 registry, inferred owner span or arbitrary fragment selector is needed. After bounded native open, reject an unexpected version, schema/storage mode, fragment/physical-row count, deletion/rewrite or foreign-owner data before claiming verification. Use explicit projection, `scan_in_order=True`, strict bounded batches, zero readahead, existing bounded I/O settings and a physical-row ceiling+1 sentinel; count every scanned row, including recognized receipt rows. Validate field/ordinal/offset order and original hashes; skip only structurally validated bound receipts, and append genuinely pending final rows exactly once. No full scan of an arbitrary legacy/shared dataset is enabled. Compaction remains disabled.

  Source-order basis: both exporters emit descriptor fields followed by membership/record pairs. `import_batch` preserves the input order through `candidates` and `pending`, then appends the batch receipt last; matched rows are omitted. A split pair across batch boundaries must therefore survive interspersed receipt rows, while same-batch replay appends nothing. Task 1's native qualification contains no foreign-record reuse; pure resolver tests cover its semantics, and Task 3 separately qualifies real remote reuse. Any nonconforming physical layout fails closed rather than switching to a sort or an unbounded lookup path.
- [x] Add supervised RED/GREEN `test_pinned_stream_finalization_and_replay` and `test_stream_layout_corruption_refuses`: actual fixture `ordered_import_batch` append/replay, split membership/record pair, late IDs, final pending rows, unchanged canonical files and version/fragment mismatch refusal. Include missing/changed qualification refusal before path/native access, first-extra-artifact/depth/oversize-manifest refusal before native open, physical row ceiling enforcement and foreign-owner refusal. Verify unchanged legacy `import_batch` acceptance of valid entries in a different physical order; the ordered path refuses its invalid ordered-layout claim. Acquire the native slot, then run:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 60 tests/test_lance_owner_stream.py -k 'pinned_stream or layout_corruption' --noconftest -x -s`
- [x] After root freezes the large diagnostic manifest, stage its exact bulk representation in individually admitted calls, using existing explicit fixture maintenance before the tail becomes unreadable. This fixture support does not change the product maintenance ceiling. Add `test_52mib_finalization_and_cold_replay`: full finalization, then a fresh 5 s session performing source regeneration, actual identical replay, required owner/live checks and real metadata ACK. A retained first append may precede that tick; no saved verification offset may. Capture stage/tick RSS/time, frames, files, all physical rows and refusals. Run once:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 600 tests/test_lance_owner_stream.py::test_52mib_finalization_and_cold_replay --noconftest -x -s`
  Original gate (failed; its unconditional two-replay requirement is superseded only by the private protocol correction below): GREEN requires exact 54,525,952-byte/6,656-member parity and the original bounded final source interval, including its entries and owner/batch receipts. Retain that ordinary publication while losing its first response before metadata ACK; a fresh session must regenerate the same pending interval and complete two actual identical imports, full owner/live checks and real ACK inside unchanged limits. Both cold replay imports append zero rows. Do not introduce a staging-only batch or manufacture an empty finalization cursor to make the final publication smaller. Hosts import no native engine. Source-only or append-only success is insufficient. On failure stop dependent implementation and report the gate; do not introduce SHA-state machinery.
  Observed operator interruption: the initial frozen run stopped at stage 122 after 19 real ACKs and two maintenance pairs when foreground work preempted queue prepare for interval 20. Its 1,071-call attempt ceiling remains part of that failed, retained evidence. A separately sealed outstanding-only continuation may consume at most 952 new calls, 1,074 cumulatively including all 122 prior calls; this explicit schedule correction covers regeneration and reopening after the observed interruption. It may not repeat completed intervals or maintenance, add optional reconciliation, reset accounting, or retry automatically. Preserve the original code/configuration/seals/logs/refusal, derive progress from actual queue state, and count every new archive, log, refusal slot and basetemp within the unchanged storage/entry envelope. All worker/session memory and time limits remain unchanged.


  Second observed interruption and continuation amendment: continuation 1 stopped during the first import for interval 36 after actual ACK 35, at 225 cumulative calls. Retain both interrupted attempts and require ACK geometry 38 versions / 2,686 rows / 35 fragments plus the exact pending intent for interval 36; its permitted after-state is 39 / 2,767 / 36. A separately sealed continuation 2 may perform at most 853 new calls, 1,078 cumulatively, with outer timeout 480 s and outer RSS 700 MiB. Prior outer durations 59.5 s + 50.4 s plus 480 s remain below the original 600 s total. The unknown first import must reconcile through both ordinary identical imports, never a fabricated cursor or optional maintenance reconciliation.

  Explicit diagnostic-metadata amendment for continuation 2: reuse the continuation-1 packet directory and retain every original artifact. Nine new metadata/artifact slots (two raw-code archives, one fixed configuration, two distinct seal/pin files, three event/peak/refusal files, and one fresh basetemp) raise normal H entries from 255 to 264; set H hard ceiling to 272 and whole entry ceiling to 2,448. This allowance only preserves the two preempted attempts after the independently fixed capture-priority bug. H remains 24 MiB and the whole byte allowance remains 512 MiB. Source 128 entries, D 1,024 entries, T 1,024 entries, 380 exact workspaces, all data/append/index limits, worker RSS/stage limits and the shared 5 s deadline remain unchanged. No deletion, overwrite, hidden retained debt, generic retry framework or automatic retry is authorized.


  Actual cold gate outcome: continuation 2 reached the full final publication. The first cold import verified all 13,696 physical rows, 171 batch receipts and the original 52 MiB / 6,656 members, but the second import exhausted the shared 5 s deadline. Retain the 1,075 starts, including the last start without an end record; no final ACK or qualification pass is established. Disk/RSS stayed below their ceilings. Dependent tasks remain blocked.

  Approved repair scope: remove repeated canonical decoding within each ordered proof using a private, strict, current-row decoded result. Its constructor accepts raw six-string rows and performs all existing validation; the public raw-row verifier always validates. Preserve public codec return values and legacy exact behavior. The ordered scanner and verifier may reuse that validated row's record/header/receipt and original UTF-8 bytes only within the same proof, with one-row lookahead and the bounded pending frame. Keep every canonical/pointer/exposure/owner/order/count/receipt/hash check and two independent complete cold imports. No caller-supplied trusted flags, cross-call cache, saved SHA state, partial-proof journal, sampler relaxation or source/queue/resource-limit change. Qualify the changed validation contract with focused pure corruption/parity tests, then independently review the product delta before separately preparing a remaining-only cold execution. No new runtime is authorized by this amendment.


  Single-decode repair review: 14 new and four affected existing pure cases passed; independent review approved the final three-product-file/focused-test delta. Native fit remains unproven. Prepare a separately sealed cold-only continuation using the retained final publication: actual ACK170 and exact pending171, no repeated staging or maintenance. Permit at most eight new calls (initial real state, six-call fresh cold session, final real state), 1,083 cumulatively including all 1,075 prior starts and the last unpaired start/refusal. Outer19s/700MiB keeps the three prior rounded outer durations (580.0s) plus this execution below600s. Shared5s/transport7s and all native limits remain unchanged. Retain every prior artifact and fit the new fixed metadata within existing H272/24MiB and whole512MiB limits; the preliminary H270 count is subject to actual census and review. No runtime is granted until that candidate is independently reviewed and sealed.

- [x] Record evidence in SDD `layout-owner-stream-report.md`; run file-scoped Ruff and `git diff --check`; commit only this task's verified files with `feat: stream pinned bulk owner verification`. Root independently reviews before Task 2.

#### Task 1 protocol correction after measured cold failure

The original two-replay cold gate remains FAILED; its evidence is not reclassified. The single-decode repair did not improve its measured wall time. A separately reviewed three-call diagnostic completed actual state/source regeneration/one persisted replay in 3.4982 s (outer 3.9 s, 204.8 MiB). The full owner proof took 0.7526 s; cold native import took 1.0063 s. Timers overlap and do not establish CPU contention. All 13,696 rows, 171 receipts and original hashes were checked; this diagnostic performed no ACK and is not acceptance evidence.

Approve a narrowly private conditional persisted-replay protocol, following `durable-replay-protocol-static-review.md`. Public `import_batch` and legacy v1 behavior remain compatible. A closed worker-established outcome distinguishes `FreshAppend` from `ExistingVerified`. The latter is constructed only after the existing-receipt branch finishes the full persisted batch/owner proof, regenerated digest/source checks and exact expected-ID-set equality under the writer lock. Its ephemeral witness binds the exact compact receipt, destination/generation path and verified parent/directory identities, source binding, batch ID/digest, domain-separated expected-ID-set digest and that dataset version. No caller-selected flag, remembered partial proof or persisted witness may select the outcome.

A fresh append still requires a separately reopened call returning `ExistingVerified`. If the first call already fully verifies a durable identical batch, repeating that same proof is unnecessary. The private coordinator validates every binding, uses the witnessed version for owner observation, preserves all original manifest/live-receipt checks, and performs the existing transactional pending/cursor/summary ACK. A stale version, malformed witness, changed source or pending intent, missing/corrupt receipt, timeout or lost response leaves work pending. Staging-only outcomes cannot publish owners. Keep every current memory, deadline, pressure, admission, cleanup and original-byte validation guard.

The original author implements this private extension with focused protocol/validation tests and legacy parity checks, followed by independent review. A new, separately frozen native qualification must demonstrate both fresh-append/reopened-proof behavior and actual cold recovery/ACK on the retained final publication. Do not replay the 170 completed intervals or claim the old gate passed. Current retained-case native time is 589.7 s of 600 s and starts are 1083; any additional native scope needs an explicit remaining-time and retained-artifact accounting review before launch. No new runtime is granted by this amendment.

For this revised qualification only, transfer 16 unused temporary-entry slots to retained diagnostic metadata: H288 and T1008, with S128/D1024 unchanged, still exactly 2,448 total entries. H24MiB/T80MiB/whole512MiB and every native memory/time/row/fragment limit remain unchanged. Independent metadata census found S122/D610/H272/T381. The declared 12 additional H entries yield H284, leaving four entries for transient metadata; expected T382 is below1008. Enforce the reduced T bound in census, reservations and sampled open-unlinked accounting. Keep the retired cold C4 configuration and cold pytest directory explicitly charged when active configuration/basetemp paths change; hashes alone do not account external files. The existing diagnostic executable is charged once when incorporated into31 codepins. Archive exact frozen W4/H4 bytes before narrow adapter updates, retain all old configs/seals/pins/logs, and validate37 prior claims/five streams through1083 starts including unpaired1075/1080. The retained cold-only scope is seven real calls through1090 with outer<=9s; cumulative maximum598.7s remains below600. A separate fixed tiny scope proves actual fresh append then a separately spawned persisted replay plus original owner/live checks. Preparation is approved; final code, bindings, tiny-fixture bounds and actual artifact counts must be reviewed and frozen before any native execution.

Before freezing this revised qualification, integrate the already independently tested cancellable background-pressure correction from the installed capture branch. The feature branch still used the observed faulty 50 ms normal-pressure probe, and both qualification sessions use background scheduling. Port the diagnostic/cancellable helper and RSS accounting into the feature branch's existing absolute-deadline flow; do not replace whole runtime/guardian files from the older capture baseline. Preserve the absolute session cutoff, preflight/serialization charging, guardian deadline checks, bounded selector waits, foreground behavior and all memory/stage/session limits. The original pressure-fix process result is not a new qualification result. Independently review the merged paths and repin the existing affected code paths before the already approved one-shot cases; this correction grants no extra retry or native time.

The separate tiny native case is frozen to one four-process background session: FreshAppend, independently reopened ExistingVerified, version-pinned owner read and original LIVE receipt read. Keep shared5s, writes3s/reads2s, task192MiB/aggregate256MiB and configured16MiB pool/64MiB spill. Its outer runner is8s/384MiB with the unique absent `/private/tmp/castle-private-persisted-tiny-20261025-v1` basetemp. Pre/post retained-tree census covers the entire fixture tmp_path with256 entries,80MiB logical/allocated, depth8 and private regular/no-symlink checks; the dedicated dataset still has its original128-artifact/8MiB/256-row/16-fragment/32-version bounds. This retained census does not measure open-unlinked or transient disk peaks. Preserve failures and basetemp; no native run until final code/bindings review.

Task1 outcome (amended protocol, independently accepted; final fixture commit2cce8e53b479f8c4c330e8fa027aebdd451c6905): the tiny FreshAppend/reopened-proof/LIVE case passed in3.2068s shared time (outer3.6s/204.0MiB). The retained52MiB case passed source regeneration, one current complete persisted proof, version-pinned owner read and real ACK171 in4.2139s shared time (outer5.1s/254.1MiB). All13,696 physical rows/171 batch receipts/6,656 members and original54,525,952 text bytes were verified. Seven new START/END pairs bring the retained ledger to1090starts and594.8s cumulative outer time. Final real state has no pending interval, one owner reference and one pending semantic item; source EOF remains unclaimed. Final actual S122/D610/H284/T382 fits the unchanged whole envelope. The old unconditional two-replay failures remain failed. Independent acceptance review SHA32051f5d52ad56a4c018fa56b098e91ad4e021023d33ed3469cb72dadb89a7ae closes this dedicated-owner gate only; full migration, remote reuse, rollover, semantic coverage and activation remain separate gates.

### Task 2: Versioned placement, reservations, routes and atomic metadata ACK

**Files:** Create `lance_layout_codec.py`, `lance_layout_directory.py`, `tests/test_lance_layout_codec.py`, `tests/test_lance_layout_directory.py`; modify `migration_codec.py`, `semantic_queue.py`, `semantic_store_worker.py`, `semantic_runtime.py` only for the declared queue protocol.

**Interfaces:** Implement the contracts/queue operations above. `LayoutDirectory(conn)` owns schema and methods `bind(job_id, plans, envelope)`, `prepare(job_id, expected_cursor, intent)`, `routes(ids)` and `ack(job_id, intent_id, verified_summary)` on the supplied existing transaction. Factor the existing migration ACK transaction only as needed to update its original cursors/counts/owner references and layout routes/gates together. Add strict pure `validate_owner_plan`, `validate_target`, `prepare_layout_interval(reg, exported, target)`, `validate_layout_intent`, `validate_layout_verified`.

- [x] Write RED pure tests `test_layout_versions_preserve_v1_job_and_source_domains`, `test_plan_and_routes_are_body_free_and_bounded`, `test_layout_intent_binds_fixed_target_and_window`. Assert original SQLite/native registration/job IDs are byte-for-byte unchanged; v1 pending receipts still validate; unknown/mixed formats, arbitrary paths, changed target/window/digest and body-bearing fields fail.
- [x] Run RED, implement schemas, rerun GREEN:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 20 tests/test_lance_layout_codec.py tests/test_migration_codec.py --noconftest -x -q`
- [x] Implement transactional metadata placement without creating native partition directories. Reserve a complete owner before prepare; consecutive allocation never rehomes an admitted owner. Unique IDs and fixed unresolved intent reservations prevent competing allocations. Route reads use indexed bounded queries and report unavailable for incomplete/unreconciled coverage. Final ACK flips an owner gate and only the final bounded frame's routes, not all earlier member rows. Retain original ACK idempotency, semantic disposition, native/SQLite phase counters and task-2 queue compatibility.
- [x] Add supervised RED/GREEN `test_layout_bind_prepare_ack_is_atomic_and_idempotent`, `test_stage_gate_and_unresolved_absence`, `test_failed_reservation_never_creates_partition`, `test_v1_queue_and_pending_compatibility`. Inject rollback before ACK commit and lost reply after commit; assert no partial route/frontier/count updates, no double counts, unchanged home, and no body sentinels anywhere in the SQLite artifact. Run with native slot:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 90 tests/test_lance_layout_directory.py --noconftest -x -s`
- [x] Implement read-only route access for exact workers using the same private-file/schema checks and bounded SQL, without importing Lance into the queue worker. Do not call the queue's lower RSS guard after importing Lance or weaken that guard: exact-worker metadata reads use their existing exact supervisor, a read-only connection and no source tables. Unit-test refusal of symlink/hardlink/unknown path/schema and incomplete directory; no `immutable=1` on the changing coordination DB.
- [x] Record `layout-directory-report.md`, scoped Ruff/diff checks and commit `feat: add versioned canonical partition directory`; root reviews the schema and atomic ACK before native integration.

Static integration notes (verified against current code before implementation): `SemanticQueue._connection` commits any normal return, including a conflict dictionary; validation after route/gate writes must raise through rollback or precede every mutation. Check layout attachment before `_migration_state` decodes v1 pending data and before `migration_ack` takes its existing last-ACK shortcut. Add route reads to both `semantic_runtime.store` read classification and `semantic_store_worker` writable selection. Exact-worker directory reads need the planned separate read-only metadata access; do not call the queue path check under its lower RSS guard after loading Lance. Existing v1 registration has no immutable layout envelope, partition registry or completeness authority; missing layout tables cannot mean authoritative global absence. Preserve registration/interval identity derivation and the 192KiB metadata frame bound.

Task2 authority choice: use a closed, explicit frozen envelope bound to the actual trusted queue/generation identity, with no production defaults. Keep the attachment separate from original registration/job identities. Return known published route locations, but treat every missing ID as unavailable at this stage; neither a reservation nor an apparently empty directory proves canonical absence. Task3 must establish the trusted registry/reconstruction authority before authorizing global absence. A route hit is a bounded location/evidence reference, never a substitute for actual Lance equality, original bytes and exposure proof. Reject bool/non-finite counters and bound complete plan/route/ACK metadata before writes. Do not add a generic completeness setter.

Digest validation follows the existing proof boundary: `prepare_layout_interval` computes the real bulk digest from validated exported bodies, but a standalone body-free intent can check only its shape/domain/bindings. Do not add a circular digest-derived batch ID or treat a self-contained metadata hash as publication proof. Changed-digest tests must exercise actual source preparation, equality with the frozen pending intent and ACK's full-intent hash/real bulk receipt binding. Target/window/identity mutations remain independently checkable.

Attachment-loss correction: persist one small nullable, strictly versioned attachment (format plus immutable envelope hash) in the original `migration_jobs` row. Do not overload its active/refused/exhausted `state` field. Upgrade the coordination schema to4 transactionally on the first explicit layout bind; continue reading versions1–3 and avoid migrating untouched v1 queues on ordinary writes. Version4 requires the attachment column and must not be downgraded by the existing connection setup. Missing/partial/mismatched derived layout tables for an attached row are unavailable before v1 pending decoding or any open/mutation/last-ACK shortcut. Losing derived metadata must not reinterpret an already layout-published job as legacy. Destruction of the whole coordination DB remains unavailable pending protected restoration.

First attachment requires a pristine original registration: no pending/ACK/refusal or progressed cursor/frontier/count/owner state from the flat layout. Already attached jobs may bind further owners under the same envelope. Restored pending targets must match the authoritative envelope hash/generation and partition bound. Preserve the original nullable-intent terminal `snapshot_export_exhausted` ACK through the layout API, including lost-reply idempotency, original cursor/coverage semantics and pending semantic counts; refuse while this job still has pending publication, unpublished admitted owners or unresolved reservations. Source exhaustion is not global directory completeness.

Task2 outcome: independent acceptance `task2-layout-native-acceptance-review.md` (SHA256 `a82c5367c4fbc2f6c4c9e792d65020ca2f583547da67dbcd3193310123ecdc7c`) closes this metadata milestone. The frozen eight-file candidate passed25 pure checks and the first actual SQLite run passed11 cases/94 calls in10.60s pytest/10.8s outer with87.6MiB sampled aggregate peak. Each call had fresh stage resources and no runtime refusal; rollback, lost content/terminal replies, corrupt authority and same-queue legacy pending3-to4 compatibility passed. Exact selectors and unchanged limits are in `task2-layout-pure-results.json`; root added only `-p no:cacheprovider`. The unique `/private/tmp/castle-layout-directory-task2-20261025-v1` tree is retained. These are independent bounded metadata sessions with synthetic exact-proof summaries, not native Lance publication, global absence, shared-session publication or activation evidence.

### Task 3: Partitioned exact store, tail maintenance and crash recovery

**Files:** Create `partitioned_exact_store.py`, `tests/test_partitioned_exact_store.py`, `tests/test_layout_coordinator.py`, `tests/test_layout_authority.py`, `tests/test_layout_resolver_hooks.py` and `tests/test_layout_source_planning.py`; modify `bulk_exact_store.py`, `lance_exact_store.py`, `lance_exact_worker.py`, `semantic_runtime.py`, `migration_coordinator.py` and the existing layout codec/directory/queue modules for the approved v5 authority extension; add only versioned owner-plan access in existing source codec/runtime/worker/exporter modules.

**Interfaces:** `PartitionedExactStore(generation, *, writable=False)` implements the three declared exact layout operations. Its resolver reads derived routes read-only inside the supervised exact worker and keeps at most the pinned home dataset plus one remote dataset open. Close each remote before opening another; bound total distinct remote opens using the frozen qualification envelope and refuse rather than accumulating handles. Return bounded canonical rows through existing validation; the two-handle path must qualify under the unchanged exact profile. Add `_layout_fixture_tick(session, registration) -> dict` to the coordinator; it retains original source dispatch by registration domain. Add `SemanticSession.layout_source(descriptor, cursor, window) -> dict` using existing fixed source workers/profiles; initially `plan` and single-owner `export` only. Source plans reuse original streaming summaries, count conservative complete expansion, and never advance publication/cursors.

- [ ] Add protocol RED tests `test_layout_tick_keeps_fixed_partition_through_retry`, `test_layout_tick_refuses_late_success`, `test_owner_plan_never_changes_source_frontier`. Assert same source frame/intent/destination after every lost reply; no route exposure or ACK before checked identical replay and owner/live checks; no new session to extend 5 s. Run:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 20 tests/test_layout_coordinator.py -k 'protocol or fixed_partition or late_success or owner_plan' --noconftest -x -q`
- [ ] Implement fixed generation/partition validation, owner-home lookup and registered fragment/version reads. Keep original `BulkExactStore.import_batch` and public operations compatible; layout-specific dispatch selects the facade. Existing full partition artifact checks remain; root validation must not walk unrelated partitions. Global resolver compares actual canonical rows and first-owner membership witnesses; no body-bearing routing cache or caller-provided route is trusted.
- [ ] Enforce exact pending physical row/fragment/byte reservations under the writer lock before append. Existing receipt replay writes zero rows. Require one active ID BTREE segment; reject union coverage. `layout_maintain` bootstraps before the 1,024-row/32-fragment tail is exceeded, then explicitly calls supported existing-index optimization; use the mutated dataset for in-stage postchecks and a separate fresh read for persistence proof. Charge all retained output; no automatic maintenance on reads and no cleanup. Bad geometry/unknown acknowledgment freezes the reservation until reconciliation.
- [ ] Add supervised RED/GREEN assertions `test_cross_partition_reuse_conflict_and_foreign_stage`, `test_tail_reservation_and_single_segment`, `test_lost_commit_and_ack_recover_same_receipt`, `test_underestimated_owner_stays_incomplete`, `test_selected_partition_path_validation`. Assert one global record ID, unchanged original provenance/receipt, no foreign staged reuse, owner visibility only after gate ACK, unchanged completed frontier on refusal, no extra rows on replay, no rewrite of old canonical files, and no all-partition read walk. Run:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 150 tests/test_partitioned_exact_store.py tests/test_layout_coordinator.py -k 'not protocol' --noconftest -x -s`
- [ ] Add bounded background route-index reconstruction using fixed `layout_read` action `directory_page` and `queue_layout_reconcile(page)` with `format="lance-layout-directory-page-v1"`: pinned partition/version, physical-row cursor, bounded IDs/kinds/hashes/receipt/gate links, next cursor and completion. Validate against actual Lance rows in the exact worker; keep routing globally incomplete until all registered partitions/pending intents reconcile. These are derived-index rebuild cursors, never owner/hash-proof progress. Test missing/corrupt routes, interrupted rebuild and stale partition version in `test_directory_reconstruction_is_bounded_and_blocks_absence`; no interactive repair scan. The trusted partition registry and migration jobs remain prerequisites: destruction of the entire coordination database is explicit unavailability pending their protected restoration, not permission to invent completed frontiers.
- [ ] Run only touched legacy parity selectors as well as the new tests, including original `remember/receipt/page` and shared historical ID reuse. Record commands/metrics in `layout-exact-report.md`; scoped Ruff/diff checks; commit `feat: integrate bounded canonical partition publication`. Root reviews before coalescing.

#### Task3 authority clarification after Task2 qualification

The envelope's opaque qualification hash and total ceilings do not by themselves
provide a native storage charge policy or permission to initialize a generation.
Register one closed, hash-bound qualification descriptor through the private
trusted operator/fixture path before the first layout tick. Its expected hash
comes from the root-frozen qualification, independently of ordinary requests.
Exact/source/queue operations consume that registered authority and cannot replace
it, choose budgets or infer production defaults. Preserve original source/job
registration identities and metadata-only schema4 compatibility. Source plans
combine existing streamed numeric/hash summaries with this explicit charge policy;
they never reserve optimistic deduplication savings or scan the whole owner again
just to size it. The exact registry schema/operation delta requires review before
implementation, not another user confirmation.

Keep a compact authoritative ID-claim set separate from rebuildable route rows.
Claim every possibly new ID atomically before native append and retain published
claims. A missing route for a claimed ID is unavailable, never permission to append
a duplicate. A claim miss can authorize reservation only against a complete,
pinned registry epoch with no unresolved mutation/rebuild. Establish that authority
from a verified empty registered namespace or complete bounded enumeration of
trusted partition pins. Missing authority/schema or an unexplained epoch requires
protected restoration; incomplete routes cannot reconstruct ownership authority.
This protects loss of derived routes, not arbitrary coherent corruption of both
the trusted authority and derived data. All additional metadata is charged in
capacity qualification. Registered partition mutation belongs exclusively to the
layout path; ordinary legacy APIs remain compatible outside that namespace.

The concrete extension is opt-in schema5: one immutable generation qualification
and epoch, sequential authoritative partition identity/geometry/index/tail and
mutation state, and the indexed ID-claim set. Keep completed owner spans in bounded
per-owner fields/rows; the partition row carries only its active owner/current
geometry. Do not interleave another owner into an unfinished ordered span. A
separate closed source wrapper carries generation identity and qualification hash
so the fixed source worker can resolve trusted costs before accessing the source;
the existing inner export and cursors remain unchanged. There is no ordinary
request operation that installs/replaces qualification. A progressed metadata-only
schema4 job cannot acquire native authority merely by upgrading its schema.

Preserve the native source worker's explicit SQLite-connection ban. The trusted
operator also creates a small immutable qualification artifact at a fixed
generation-relative path, bound to the same descriptor hash and generation
identity as registration. Source workers validate its bounded size, private
regular-file/no-link identity and hash before using its costs; they do not open
the coordination database or accept caller-supplied cost bodies. Charge this
metadata artifact in capacity accounting. The SQLite source keeps its existing
source heap/profile rather than inheriting the exact metadata helper's heap limit.

The coordinator's pre-ACK owner and original LIVE checks use a closed private
`layout_read` proof context containing only the pending intent ID and pinned
version, permitted for `bulk_owner` and `receipt`. Validate the actual pending
home, qualification, expected geometry and current native receipt/rows; do not
persist a replay-verified flag or reuse a proof cache. Both checks use the same
home/version. Context-free reads still require the published owner gate. Preserve
the inner legacy receipt arguments; this context is an identity binding, not a
caller-selected permission to expose staged records.

Ordinary legacy reads at a registered home must also refuse: otherwise the native
owner marker could expose a final append before its metadata ACK. Default access
checks precede legacy path traversal/native open, including the ordered private
store's path override; only the facade supplies a bound authority/context override.
The default namespace check covers lexical parent traversal and case aliases of
the actual same directory, while preserving distinct legacy directories on
case-sensitive filesystems. Keep supervision, symlink checks and the second
mutation-authority check under the writer lock. Pure hook tests do not establish
native visibility or resource fit; the integrated fixture must verify this gate.

### Task 4: Source-view coalescing and grouped owner verification

**Bounded background-step amendment (2026-10-26):** The original all-in-one
`_layout_fixture_tick` remains a diagnostic with its original absolute5s deadline;
its large-tail qualification failed and is not accepted by a later retry.
Add private `_layout_fixture_step` using the existing durable pending-intent
boundary. A validated fresh append returns `in_progress`, with the original
source frontier/counters and all publication gates unchanged. It starts no new
session internally. A later external invocation cold-regenerates the exact same
pending request and runs actual Existing full-byte proof, grouped owner/LIVE
proof and atomic ACK inside that invocation's absolute5s deadline. No body,
partial hash, verified flag or new database field crosses the calls. Lost or
late fresh replies remain refusals. This deliberately trades one additional
queue resume/source regeneration per new append for bounded cold-start work.
Qualify the new background lifecycle separately, retaining every original
520-chunk geometry, index-refusal, exact-byte and pending-recovery assertion;
also verify actual unacknowledged owner invisibility. Worker/RSS/frame/heap
limits remain unchanged. This supersedes the all-in-one timing prerequisite
for future background construction only; it does not claim old diagnostics
passed, activate a mover, or complete multi-owner coalescing/capacity work.

**Files:** Modify the existing SQLite/native source codec/runtime/worker/exporter modules, `lance_layout_codec.py`, `lance_layout_directory.py`, `semantic_queue.py`, `semantic_store_worker.py`, `migration_codec.py`, `migration_coordinator.py`, `partitioned_exact_store.py`, `lance_exact_worker.py`, `semantic_runtime.py`; create `tests/test_layout_source_packing.py` and extend `tests/test_layout_coordinator.py`.

**Interfaces:** Extend the Task 3 `layout_source` window to a bounded ordered owner prefix already admitted to one home. Use packing `bulk-v1-ascii-layout-1` in layout envelopes only; original `PACKING` and v1 source behavior remain unchanged. Produce per-owner original phase/key/cursor associations for layout intents; do not infer every owner key from the final cursor. Add `layout_verify_owners(intent_id, version) -> dict` returning bounded ordered manifest hashes, pinned versions and original live receipt proof summaries, checked against the prepared intent; no whole manifests in the response. This supplements, never replaces, the actual identical import replay. The initial single-owner slice still rejects multiple owners; the bounded ordered multi-owner extension remains required later in this task.

**Dependency boundary and qualification order:** Implement the planned grouped owner/LIVE verification first for Task 3's current single-owner pending shape, before closing Task 3 qualification. The earlier failed run stopped before Lance; grouping did not explain or repair that startup failure. A subsequent actual first-LIVE v3 passed FreshAppend, separate complete Existing replay, owner read and original LIVE receipt read, then the ACK worker reached the unchanged shared five-second deadline. ACK commit is unknown; retain that failed evidence without probing or retrying it. This later failure justifies moving the planned single-owner grouping ahead of Task 3 closure, not a timing pass or waived gate. All Task 3 gates, full Task 4 multi-owner coalescing and later capacity qualification remain required.

The initial closed `layout_verify_owners(intent_id, version)` operation accepts exactly one finalized owner from the actual pending intent; staging intervals skip it and multiple owners still refuse. Use one native dataset open at one fixed registered home/version, preserving the existing bounded `_context_batch` rechecks on that immutable handle. Do not add an invocation cache merely to force one receipt lookup. Reuse the actual owner and original LIVE receipt validators, return strict ordered body-free hash summaries, and compare them with freshly exported source/pending/replay evidence before the same atomic ACK. Preserve FreshAppend followed by separate actual Existing complete-byte proof; grouped lookup neither replaces it nor adds a third whole-owner scan. The grouped operation remains a read under2s/192MiB task/256MiB aggregate inside the same5s session, with original memory-ID meaning, route/remote budgets and staged visibility unchanged. No native timing or capacity acceptance follows from this amendment.

**Native first-slice preparation amendment:** Remote run40224307858 qualified the four SQLite-source cases (109starts; shared ticks at most4.5581s), but native source plan and export each cold-opened Lance (.9709s and.9422s), exhausting the unchanged5s session during grouped verification after successful Fresh and full Existing proof. No ACK was attempted. Implement the planned ephemeral first-output reuse as a closed native-only `prepare` action before closing Task3. It computes the same body-free owner plan and one original bounded first export in a single protected source view; the outer cursor stays unchanged and a transport checksum binds plan/export. This explicitly permits preparation before placement, but bind and validation against the actual target remain mandatory before any durable publication. No prepared body/hash enters metadata or survives the tick. Pre/post source checks and frame/resource caps remain unchanged. Already admitted owners and pending intents still cold-regenerate via original `export`; full pending equality, original source bytes/cursors, Fresh→separate Existing→actual grouped proof→atomic ACK stay unchanged. Reject non-native or mid-owner preparation, changed plan/body/checksum, stale/oversized/late/refused preparation or placement. Four passed SQLite paths keep their existing plan/export sequence. New pure and actual native qualification are required; this slice is not full multi-owner coalescing or production acceptance.

Actual multi-owner coalescing must extend the existing native metadata authority together with source/intent/facade contracts: `_NativeLayoutDirectory.bind`, the active-owner lifecycle, per-row publication gates, bounded owner spans and atomic ACK must agree on the same ordered admitted prefix. Keep authoritative claims and foreign-stage refusal intact. The current source and intent shapes admit one owner and associate LIVE identity with the final cursor; packed output instead needs each owner's original phase/key/before/next association. Inherited bulk append places entries before owner markers/LIVE rows, so packed physical ordering and span/shared-scan selection must explicitly preserve each owner's original field/member/marker order. Do not merely relax single-owner guards or let unrelated owner rows pass the strict verifier. These are dependencies of the existing coalescing scope; all resource limits and proof obligations below remain unchanged.

- [ ] Write RED `test_packing_is_deterministic_across_frame_phase_and_home_boundaries` and `test_packing_preserves_each_native_before_next_cursor`. Assert exact 256-entry/finalization and decoded/ASCII/full-frame bounds, one binding/phase/home, per-owner original identity/counters, and identical replay of a frozen prefix. Deadline pressure refuses without clock-dependent repacking of a pending interval.
- [ ] Implement planning/export over one existing protected source view per worker call. Plan a bounded prefix, bind homes before export, then stop at the first home/phase/frame boundary. Reuse ephemeral first output within a tick; regenerate a pending interval from its frozen window. Preserve SQLite live/history/audit distinctions and native typed candidate/alias/raw-JSON contracts; no repeated worker/source reopen per owner.
- [ ] Implement grouped durable owner/live verification in one exact worker and a bounded shared partition scan where needed; do not launch one exact worker per owner or rehash a complete partition independently for every packed owner. Keep ordinary original-byte hashes and original receipt validators. Aggregate result and metadata ACK must fit the same 5 s session and complete frames.
- [ ] Run pure RED/GREEN:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 25 tests/test_layout_source_packing.py -k 'deterministic or native_before_next' --noconftest -x -q`
- [ ] Add supervised `test_coalesced_owners_use_one_source_view_and_grouped_proof`, `test_packed_lost_ack_replays_without_recounting`, and `test_grouped_live_receipts_and_empty_audit_owners`. Assert fewer physical appends than one-owner intervals, exact owner/count/hash parity, no first-append-only ACK, source/descriptor parity, and one cold shared deadline including all source/queue/exact stages. Run with native slot:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 150 tests/test_layout_source_packing.py tests/test_layout_coordinator.py -k 'coalesced or packed or grouped' --noconftest -x -s`
- [ ] Record `layout-packing-report.md`; scoped Ruff/diff checks; commit `feat: coalesce admitted owner exports within bounded sessions`. Root reviews before whole-layout qualification.

### Task 5: One full-geometry rollover and capacity qualification

**Files:** Create `tests/test_lance_layout_lifecycle.py`; create a strict metadata-only qualification report in SDD `layout-capacity-results.md`; update this subplan/spec status with measured scope only. No production caps or activation configuration.

**Interfaces:** Consume the root-frozen rollover manifest and reviewed Task 1–4 APIs. Qualification uses original existing 519,641-row source boundary plus permitted inventory of canonical/page/member/owner/receipt expansion and live delta; a raw source count is not a physical-row allowance. Metadata-only directory cardinality must match that projected expansion, not the small test's number of bodies.

- [ ] Write RED accounting assertions `test_retained_capacity_counts_all_generations_and_orphans`: count each protected source/rollback root once; include every partition/version/index/receipt, simultaneous next output, journal/temp/spill, interrupted artifact and required free headroom. Reject insufficient aggregate capacity before creating a destination. Use permitted inventory snapshots; absent inventory yields `qualification_required`, never a fabricated capacity permit.
- [ ] Freeze one lifecycle at the intended owner/partition envelope: packed realistic small owners, the admitted known large owner, sealed-partition reuse/conflict, deliberately lost commit/ACK reply, cold old/new/owner/page/absent reads, owner-boundary rollover and further successful allocation. Include an underestimated owner that remains explicitly incomplete and cannot authorize mid-owner rollover. No cardinality ladder, cleanup or compaction.
- [ ] Add `test_frozen_owner_boundary_rollover_lifecycle` and run exactly once after native-slot approval:
  `/Users/ladislavbihari/.echelon/venv/bin/python scripts/safe_brain_tests.py --timeout 600 tests/test_lance_layout_lifecycle.py --noconftest -x -s`
  GREEN requires the complete frozen sequence inside existing per-stage/session limits, global ID uniqueness and original-byte/receipt parity, bounded selected-partition work after sealing, successful new-partition continuation, and measured retained-plus-temporary full-corpus capacity admission. A local 52 MiB pass or small rollover alone is insufficient. Directory rebuild/reuse fanout and all refusals are reported explicitly.
- [ ] If inventory or resource/capacity admission fails, record the exact gate and stop. Preserve sources and staged/rollback artifacts; do not adjust ceilings, silently skip owners, create new permits or claim full-corpus readiness. If retention cost prevents admission, return to root for separately designed retirement; this plan does not implement deletion.
- [ ] Run final touched-file Ruff and `git diff --check`, record frozen revisions/input hashes/commands/metrics in the report, and commit `test: qualify bounded Lance owner rollover lifecycle`. Root performs whole-subplan review. Task 3c snapshot/corpus reconciliation and Task 4 semantic/retrieval integration remain separate completion work.

## Static checks and handoff

For each task run `/Users/ladislavbihari/.echelon/venv/bin/ruff check` and `ruff format --check` with only that task's explicit created/modified Python files, then `git diff --check`. Stage only its listed verified product/test files and evidence; use the task's stated conventional commit message. No broad suite or rerun of unrelated green native work. Native test evidence must identify the real guarded operation, not substitute a fake boundary for publication, equality, replay or resource claims.

The five review gates follow the requested sequence. No production envelope has been chosen. Root reviews the concrete diagnostic manifests before their runs and resolves any implementation deviation before it changes the reviewed contract; implementers do not use those gates to add protocols, weaken verification or grow scope.


### Task3 correction: resume a registered layout after interrupted queue writes

The first actual Task3 qualification stopped at the shared five-second deadline
in queue preparation, before any Lance worker. Its failed evidence remains
preserved; this correction does not turn that run into a pass. Host diagnostics
localized substantial startup overhead but did not establish a product latency
cause. Separately, static recovery review found a concrete liveness gap:
`_layout_fixture_tick` starts with a read-only queue connection. An interrupted
SQLite write can leave a hot rollback journal, which cannot be recovered through
`mode=ro` ([SQLite result codes](https://sqlite.org/rescode.html#readonly_rollback),
[recovery protocol](https://www.sqlite.org/lockingv3.html#hot_journals)). The failed
fixture database has not been inspected, so no hot journal is asserted there.

Add one closed internal `queue_layout_resume` operation taking the original full
registration. It replaces the initial state call; it does not add a worker. Its
connection is existing-file `mode=rw`, timeout0, private path validation without
creation, fixed queue heap/cache/mmap/synchronous controls and a transaction.
Require schema5, complete current authority, attached existing job and identical
registration/destination/qualification before returning the same bounded state
and native-layout context. There must be no table creation, upgrade, job insert,
route reconstruction, claim confirmation, pending reset, source cursor/count
advance or synthetic acknowledgement. SQLite may recover its existing journal.
The existing legacy migration opener is deliberately unsuitable because its
attached-job guard must remain intact.

Ordinary state/status/routes and exact queries stay read-only. The recovery entry
uses the already established queue-write profile (3s,128MiB task/192MiB aggregate,
16MiB SQLite heap), inside the unchanged shared five-second session. No cap is
raised. One caller refusal stops that tick; no hidden second attempt or new lease.
A later ordinary tick may resume a bound plan or identical durable pending intent.
Only the original durable native proof and ACK can publish or advance coverage.

Verification: engine-free RED/GREEN tests for exact operation/binding, no further
work after refusal or registration mismatch, existing-path/no-create mode, strict
authority and unchanged read-only APIs. Prepare one small guarded disposable
metadata crash test: real hot journal from interrupted uncommitted changes,
registered recovery restores prior complete state, committed lost-response pending
remains unchanged, and missing DB/job/authority cannot initialize anything. Freeze
its finite calls/files/time separately before execution; do not reopen the failed
qualification fixture or retry the first-five qualification automatically.
