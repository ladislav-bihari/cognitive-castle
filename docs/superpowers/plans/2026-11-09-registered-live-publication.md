# Registered Live Publication Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans inline. This implements the already approved unified-memory Task 4; it does not replace Task 5/6 or authorize an unverified cutover.

**Goal:** Route ordinary canonical publication and capture through registered Lance authority while keeping writable capture coordination separate from the exhausted legacy canonical SQLite file.

**Architecture:** Reuse canonical preparation/receipts and the existing bulk owner encoding. A direct live request has its own source domain, never a fabricated archive descriptor. Durable body-free intent precedes actual registered append/replay proof; only verified receipts authorize scopes/checkpoints. Original SQLite/Lance sources remain protected.

**Tech Stack:** Existing Python canonical/bulk codecs, bounded SQLite control workers, native Lance exact workers. No new model, provider or dependency.

**Spec:** `docs/superpowers/specs/2026-10-24-semantic-preservation-design.md`; Task 4–6 of `docs/superpowers/plans/2026-10-24-unified-lance-memory.md`.

## Global Constraints

- Preserve canonical IDs, provenance, exact UTF-8 text, namespace/key digest and complete receipt equality.
- SQLite control stays body-free. Do not resume canonical writes into the full legacy database, delete originals, change models/goldens or raise quotas/RSS/deadlines to obtain a pass.
- Keep 128/192 MiB control workers and approved 192/256 MiB exact workers; retain existing shared deadlines, single flight, cancellation and cleanup.
- No unregistered flat-store fallback, implicit schema upgrade, source-descriptor fabrication or ordinary-query migration/index build.
- Trusted operator selects generation/qualification. Input scope labels grant no permission; exposure/source hashes and policy revision must still be checked.
- Frozen integration source `66fdb6f` remains separate. Do not commit unqualified new work in its merge, install or activate it.
- Local tests normally remain pure, after normal pressure admission, under the 20-second/256-MiB safe runner. The original fixed macOS15 native gate remains separate. Additional qualification of the actual production tuple may use a frozen isolated checkout, explicit source grant, private synthetic fixtures, pressure1 and the same unchanged resource/deadline guards; it cannot stand in for the macOS15 result or access the installed palace/models.

## Review Focus

- Empty/oversized/forged preparation must fail before durable intent or storage mutation.
- NUL, Unicode, CRLF, whitespace and multi-chunk inputs retain exact receipt/record parity.
- Changed scope, key, caller, digest or replay binding must not reuse an unrelated pending publication.
- A lost response or interruption must not acknowledge first-append success or advance a source checkpoint without actual durable proof.
- A full legacy canonical file must not prevent new control metadata from being written in the independently admitted state store.

## Task 1: Direct live request codec and body-free replay intent

**Files:** Create `cognitive_castle/live_publication_codec.py`; create `tests/test_live_publication_codec.py`.

**Interfaces:**

- `prepare_publication(prepared: PreparedRemember, scopes: list[dict]) -> PreparedPublication`.
- `PreparedPublication` holds `canonical`, `entries`, `manifest`, `intent`; these are preparation only, not a successful publication receipt.
- `validate_intent(intent: dict) -> dict` validates the closed body-free shape and source/owner bindings.
- `regenerate(intent, prepared, scopes) -> PreparedPublication` refuses anything other than identical regeneration.
- `batches(publication) -> iterator[dict]` yields deterministic existing bulk-v1 batches, finalization after all entries, each checked against existing encoded/decoded/count ceilings.

Builder calls: use the source domain `lance-live-source-v1`, intent format `lance-live-publication-v1`, and owner key `direct-live:` plus SHA-256 of canonical namespace/key. Source binding includes canonical digest and exact scope projection; it does not make model profile identity part of exact-source identity. Reuse `semantic_intents.prepare_intent(..., lane="new")` for the semantic outbox. The owner descriptor contains only format, namespace/key, digest and expected receipt; caller content remains in canonical records. Clone scopes so later caller mutation cannot rewrite prepared state. Validate `PreparedRemember` by identical canonical regeneration, not merely its dataclass type.

- [x] Write tests for record/receipt parity, no body/caller in intent, strict schema/tamper refusal, replay conflicts, scope aliasing, maximum escaped multi-chunk input and deterministic bounded finalization.
- [x] Observe RED for the missing codec, implement the interfaces, and observe GREEN in a fresh pure process. The additional pre-copy bounds regression also has RED/GREEN proof.
- [x] Run Ruff/diff checks and record exact source/test evidence in `docs/verification/2026-11-09-registered-live-publication.md`.
- [ ] Commit only after the integration merge state permits an isolated verified commit. Frozen qualification source is retained/pushed separately; no unqualified work enters the integration merge.

## Task 2: Registered live authority and native publication

**Files:** `lance_layout_directory.py`, `semantic_queue.py`, `semantic_store_worker.py`, `partitioned_exact_store.py`; new focused live-authority tests.

- [ ] Introduce a closed direct-live registration path consuming Task 1 intent plus operator-bound generation/envelope. Store hashes, IDs, counters and scope/outbox metadata only.
- [ ] Support fresh operator setup without inventing a migration job or descriptor. Preserve existing schema/authority readers and reject implicit mutation of old generations. Freeze exact schema/API additions before implementation in this task's brief.
- [ ] Reserve global IDs, partition capacity and original live receipt aliases under registered authority; conflicting key/digest/scope cannot steal another owner's claim.
- [ ] Implement registered native publication/receipt operations with actual row/owner/receipt verification. Initial append success alone cannot authorize ACK; identical cold replay and lost-response recovery remain required.
- [ ] RED/GREEN pure binding/ordering tests, then finite native cases for new commit, identical replay, conflicts, interrupted append/ACK, preemption and actual RSS/time/resource accounting. Preserve every failed fixture.

## Task 3: Ordinary publication coordinator

**Files:** new `live_publication.py`; existing `semantic_runtime.py`, `memory_runtime.py`, public memory route adapters; focused coordinator/API tests.

- [ ] Compose prepare, registered exact publication/proof, state ACK and semantic outbox under the original shared deadline. A refusal stops later work; pending proof is resumable.
- [ ] Select the generation only from trusted operator state, requiring the corresponding registered read/capacity capabilities. An activated but unavailable generation must fail closed, never write canonical data into SQLite.
- [ ] Keep ordinary receipt shapes unchanged. Distinguish expected preparation values from actual durable responses. Test every public write entry point and source-policy conflict.

## Task 4: Independent body-free capture state

**Files:** new capture state store/worker and host adapter; existing `memory_capture.py`, `memory_ingest.py`, `capture_publication.py`, capture operator; focused capture tests.

- [ ] Freeze the new control-only schema/operation contracts from the actual existing capture tables. Do not copy `chunks`/FTS/text bodies into it.
- [ ] Split bounded source preparation and verified application across control/exact workers; do not spawn native work from a SQLite worker or import SQLite/native libraries into the host coordinator.
- [ ] Preserve source generations, scope attempts, journal fairness, actual receipt recovery, accounting, CAS and checkpoint-after-proof. Bound retained source/regeneration work within the unchanged slice/session deadlines.
- [ ] Provide explicit bounded metadata import from the frozen old state, retaining its originals and handling interrupted/lost responses. Queue scanning may not claim completeness while required sources remain pending/unknown.
- [ ] Prove fresh successful capture with a quota-full old canonical store untouched, including lost responses, long UTF-8 tails, malformed records, modified original source, scope conflicts and restart.

## Completion boundary

This subplan must produce the working registered publication/capture path, not stop at codec unit tests or another fixture-only mover. Ordinary unified retrieval/context, configured model quality, full corpus/capacity reconciliation, operator activation/rollback, whole-branch review, B3 merge and installed Codex/Claude checks remain mandatory in the parent plan. No goal-complete claim until those facts are verified.
