# Bounded canonical Lance layout

Status: Task1 dedicated-owner streaming and amended cold recovery, and Task2
metadata placement/atomic ACK, are implemented and qualified within their recorded
scopes; Task3 is in progress, with source and resolver changes still awaiting
integrated native qualification. Tasks4–5 remain planned. Not activated. This Task3c subdesign extends the
2026-10-24 semantic-preservation design without changing the user's choice:
Lance holds canonical exact evidence and vectors; SQLite holds coordination
and derived metadata only. The partition layout below is an implementation
decision, not a claim that the user selected a new storage product.

## Intent and constraints

Make all existing history and new dialogue available through one exact and
semantic interface without an ever-growing native working set. Preserve
original text, canonical IDs, provenance, exposure, metadata, receipts and
source lineage. Preserve original archives, protected sources and SQLite
rollback stores. No conversion back into a second canonical SQLite body store.

Keep exact workers at 192 MiB task / 256 MiB aggregate, reads 2 s and writes
3 s. Source, exact, queue and verification stages consume the same 5 s session
deadline. Source-specific profiles and transport limits remain unchanged.
Keep pressure refusal, global admission, preemption and descendant cleanup.
Do not increase these limits to make qualification pass. These are diagnostic
profiles; installed production selection and activation remain a later step.

Background-step amendment (2026-10-26): the private background protocol may
intentionally yield after a fresh append, using the existing durable pending
intent. This is unverified progress: source frontiers/counters and owner gates
do not advance. Each externally invoked step has its own absolute5s deadline
and never creates a session internally. Only a later step's cold source
regeneration, actual Existing full-byte proof, grouped owner/LIVE proof and
atomic ACK can complete publication. Nothing persists a body, partial proof or
verified flag between these invocations. The original all-in-one diagnostic
retains its stricter same-call contract and its recorded failed qualification.

No generic provider framework, caller-selected budgets or executables, new
model, serialized SHA state, durable partial-proof protocol, automatic cleanup
or source deletion belongs to this change.

## Why a storage boundary is necessary

A measured update of 262,144 indexed IDs plus a 1,024-row tail passed in
0.7626 s, using 198,639,616 bytes task RSS and 224,526,336 bytes aggregate RSS.
The index update writes a complete new derived output. The existing store also
enumerates all fragments and validates all retained artifacts on entry.
Coalescing small appends reduces overhead but does not bound these operations
as the full corpus and live dialogue grow.

Compaction alone is insufficient: it still commits dataset-wide row-address
and index changes. In pinned Lance 4.0.1, staged Python Compaction.commit also
constructs default options rather than preserving the plan's deferred-remap
option. Dataset-wide cleanup does not supply application reader/replay leases.
Neither operation is an assumed escape from the resource limits.

Use sequentially allocated, bounded Lance partitions instead. One partition
receives new writes at a time; sealed partitions remain readable. Do not
redistribute existing IDs when another partition is allocated.

## Owner placement and reservations

Before an owner's first prepared interval, bind that owner to a home partition
and reserve its conservative complete expansion: records, all field and
membership pages, owner and original live receipts, deterministic batch
receipts, fragments, manifests, cumulative retained index output, temporary
work and orphan debt. Source adapters must expose bounded numerical/hash
planning facts from their existing streaming summaries; no planning bodies
are stored in SQLite. Do not assume deduplication savings before verifying
the original canonical rows.

All new rows for that owner stay in its home partition. A large owner may use
many bounded batches and a dedicated qualified partition. Roll over between
owners, never during a batch or midway through an admitted owner. An invalid
estimate leaves explicit staged/incomplete work and unchanged publication
state; it does not authorize a larger limit or silent owner subdivision.

Reserve each append's actual resulting tail under the writer lock. Count
every new physical row, including batch and optional live receipts, and the
qualified native fragment bound. Require at most 1,024 unindexed rows and
32 unindexed fragments after append. The general current conservative batch
ceiling is 513 physical rows, not 256. An existing matching receipt appends
nothing. Read paths never create or update an index.

Bootstrap the ID index while its tail is still readable. Later maintenance
uses the supported bounded-tail update, within a bounded partition. Require
exactly one active ID BTREE segment; retained old index directories do not
count as active coverage. Do not union unsupported multiple active segments.

Partition ceilings are qualification inputs, not new production defaults.
Choose row, retained-byte, fragment, version and artifact ceilings together
from measured realistic owner packing. The 262K synthetic pass is not itself
a production partition allowance. Full-corpus retained and temporary capacity
must be admitted before production construction.

## Metadata directory and exact reads

Extend existing metadata coordination with an indexed unique ID directory and
partition/owner reservations. Route canonical records, field/member pages,
owner markers and receipts by ID. Store only IDs, kinds, hashes, locations,
versions, publication-gate references, states and counters. No text, metadata
JSON bodies, scopes, caller payloads or serialized source frames belong here.

An ID's absence from one partition is not global absence. Resolve candidate
IDs in bounded groups before append and compare actual Lance rows with the
existing canonical equality and visibility rules. Reuse an identical visible
canonical row from an older partition. Reject conflicts and foreign staged
records; do not duplicate a canonical ID merely to simplify local reads.

Group reads by their registered partition, bound simultaneous native opens
and overall fanout, and retain the shared deadline. Validate the private root
and selected partition descriptor directly; retain full artifact checks
inside the bounded selected partition. Do not walk all partitions on every
request. Route misses during unresolved publication or directory rebuilding
are explicitly unavailable, not silently authoritative not-found answers.
Do not scan every partition interactively to repair the directory.

Canonical bytes and owner/membership evidence are still read and checked in
Lance. A metadata route or cache is not a substitute for source/hash/exposure
verification. The directory is rebuildable by bounded enumeration of canonical
IDs and receipts. Its reconstruction is a background completeness operation.

## Publication, retry and visibility

Extend the existing durable prepare, identical replay and ACK protocol:

1. Bind owner home/reservation and the interval's fixed destination partition
   before the first write. Preserve source binding, exact cursors, packing,
   digest and counts. Do not repack a pending interval after a lost reply.
2. Commit one batch's new rows and receipt atomically in that partition.
   Resolve reused remote rows through the same bounded directory/resolver.
3. Verify the actual durable batch, exact proof-ID set, final owner and original
   live receipt as required today. Source regeneration and complete verification
   must finish in the current session; interrupted hash work restarts.
4. Atomically ACK directory updates with source frontier, counts and owner
   references in existing metadata coordination. Lost ACKs retain the same
   idempotent identity and fixed destination.

Stage routes remain private and reference their owner's publication gate.
Final ACK changes that gate and the bounded final interval's routes; it must
not rewrite thousands of staged route rows. Already published reused records
stay visible. A final Lance append without its route/frontier ACK remains
recoverable pending work. Directory misses cannot authorize conflicting
allocation while such a commit is unresolved.

Pack consecutive small owners from the same protected source binding within
existing entry/finalization, decoded and ASCII frame limits. Stop before a
source-phase or partition boundary. Coalescing must happen within a source
worker's existing bounded view, rather than repeatedly reopening one source
per owner. Preserve deterministic packing/replay. Add bounded grouped final
owner/receipt verification if needed; do not start one fresh worker per owner
and pretend the whole group still meets the shared deadline.

## Large owners: measure ordinary streaming first

The representative disposable 52 MiB source has 6,656 members. Its source
stream was measured; destination finalization and the full cold replay/ACK
were not. Current verification performs 416 indexed member/record fetch groups
for that case. This does not establish that every actual source owner fits.

Qualify a direct ordered stream of a dedicated pinned partition's bounded
fragment span: descriptor fields followed by membership/record pairs, with
batch receipts interspersed. Validate owner, deterministic IDs, field/ordinal,
offsets, counts and full original-byte hashes while retaining only bounded
current pages/records and ordinary hashlib accumulators. Include pending final
rows and validate committed fragment/version binding. Do not sort or join the
owner's complete text, replace original-byte hashes with page hashes, or persist
hash state. Compaction is disabled for this initial insertion-order contract.

Before the partition registry exists, admit this path only through a fixed
fixture worker bound to a root-reviewed, hash-frozen qualification manifest.
The complete dedicated one-owner dataset is its scan span. Validate the
private destination, expected version and row/fragment limits, and bound a
streaming artifact walk by entry count, depth, file size and retained bytes
before native open. Missing or changed qualification refuses before path
creation or enumeration. Preserve the ordinary legacy import path; it must
not acquire an unregistered ordered operation or caller-selected budgets.

Reused remote canonical records must contribute their actual verified bytes
at the correct ordinal; their original visibility witnesses remain required.
Remote reuse distribution is a separate latency/fanout gate. A successful local
large-owner scan is not evidence that arbitrary scattered reuse fits.

Any actual owner that cannot fit the qualified whole-owner reservation and
complete verification remains explicitly incomplete and prevents the full
goal from being marked complete. No arbitrary-unbounded-owner framework is
introduced before a measured need and a separate design decision.

## Initial retention and completion gates

Seal partitions without compaction or cleanup first. Charge every retained
version, index, temporary artifact and orphan in the full-corpus reservation.
If that measured total does not fit, qualify retirement before production;
do not assume future deletion will provide space. Original archives, protected
sources and SQLite rollback stores are outside retirement authority.

Before later retirement, prove exact current/retained version reachability,
resolved writer/replay state, reader termination or durable pins, and equality
of every rewritten logical row and receipt. An age or replaced UUID is not
such a proof. No deletion operation is authorized by this design.

Qualification sequence: known large-owner streaming plus complete cold replay;
then owner-boundary rollover with realistic packed owners and a directory at
projected expanded-corpus cardinality. Include canonical reuse/conflict across
sealed partitions, lost commit/ACK responses, bounded cold record/page/owner
and absent-ID reads, continued allocation after sealing, and retained-byte
amplification. Use one falsifying lifecycle test, not an indefinite increasing
raw-row matrix.

Source inventory, consistent protected snapshots, full historical/live delta
reconciliation, semantic indexing, lexical/KG/reranking integration, production
profile selection, reviewed B3 merge/install and the real completion audit
remain required. This physical layout does not itself make those tasks done.
