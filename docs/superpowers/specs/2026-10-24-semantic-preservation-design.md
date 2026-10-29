# Preserve semantic memory while bounding resources

Status: the user selected option B with “B na zacatek”. Proceed with B's bounded
qualification first; deployment remains conditional on the specified evidence.
The SQLite historical converter stays stopped. This document corrects an assistant-made
scope choice; it does not describe lexical-only retrieval as the user's desired
replacement for semantic memory.

## Intended result

One ordinary memory interface must find both newly captured and historical
information by exact wording and by meaning, and return the original text with
verifiable provenance. Continuous verbatim capture and slower semantic indexing
have separate durable progress and resource supervision. A second, stale vector
route does not satisfy this objective.

The user requested low memory consumption and retained memory/context quality.
They challenged the switch to SQLite/FTS5 and the loss of ordinary vector-search
benefits, then selected Lance as the main exact-text and semantic palace (B).

## Verified current state

- Installed code is `6402ae52` on B3/develop. Ordinary CLI, Python, MCP, brain and
  context paths use the common exact SQLite catalog. These paths do not currently
  incorporate vector candidates.
- The legacy search pipeline retains dense retrieval, full-text retrieval,
  knowledge-graph candidates, rank fusion and cross-encoder reranking. Explicit
  legacy modes use the old corpus; new capture does not update it.
- Historical conversion was stopped at 483,895 of 519,641 source rows, with zero
  recorded gaps. The temporary controller exited normally at a checkpoint. Its
  pause marker remains present. Original Lance data and vectors are preserved;
  captured exact data is preserved. Routine live capture remains enabled.
- Real exact-text capture and mixed historical/live retrieval passed their
  checks. Those results establish text fidelity and lexical integration, not
  semantic integration.
- Current local configuration selects
  `sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`, dimension 384,
  identity `paraphrase-ml-MiniLM-L12-v2`. Cached safetensor weights occupy
  470,641,600 bytes, about 449 MiB. The configured path uses sentence-transformers
  and Torch. Device selection is automatic; the actual resolved device has not
  been measured in this review.
- Installed package metadata reports LanceDB 0.30.2, pylance 4.0.1, PyArrow 24.0.0,
  Torch 2.12.0, sentence-transformers 5.4.1, Transformers 5.6.2 and ONNX Runtime
  1.25.1. These version checks did not import the model runtimes.
- The actual archive's vector dimension and embedding identity remain unproven.
  A guarded schema-only probe was refused for system memory pressure before its
  native worker ran. Configuration alone is not evidence of stored vector
  compatibility; matching dimensions alone would not establish it either.
- An older provider pilot recorded roughly 3,110 MiB process RSS for a full
  bge-m3/reranker pipeline on a small corpus and different dependency versions.
  It is not a measurement of the current configured model, nor evidence that
  LanceDB itself requires that memory.

## The architectural choice

Both options below must retain semantic retrieval. Choosing an exact-text storage
engine is separate from choosing retrieval capabilities.

| Option | Exact capture and evidence | Semantic retrieval | Consequences |
|---|---|---|---|
| A: exact SQLite ledger plus active Lance semantic index | Keep the bounded exact ledger, provenance and checkpoints; retain historical exact copies if approved | Add current semantic candidates to the common coordinator and durably index new exact records | Capture can commit without loading an embedder or native vector reader. Requires explicit approval to retain SQLite as the exact-text store, and explicit handling of indexing lag. |
| B: Lance as the common exact and semantic palace | Store canonical exact records and vectors in Lance; a small coordination queue may remain separate if approved | The same coordinator serves new and historical records from the Lance-backed corpus | Preserves a Lance-centered storage architecture. Requires new qualification of bounded exact writes/reads, recovery and capture during native/model refusal. Existing exact data must be reconciled before any switch. |

Option B is selected. A remains documented only as an unselected alternative;
qualification failure does not authorize switching to it. Existing exact data
remains a protected migration source until a complete verified replacement exists.

Neither option authorizes deletion, automatic activation of the old miner, an
unbounded model process, an external provider, or a silent model substitution.

## Shared retrieval behavior

Use one canonical retrieval coordinator, not separate public lexical and semantic
APIs with different corpora. Reuse the existing pure rank-fusion implementation
where appropriate. Preserve the configured ordinary pipeline's KG and reranking
behavior when its required indexes are available. Merely retaining disabled code
or moving these capabilities to an obscure legacy mode is not preservation. Their
resource cost must be measured separately. If a currently configured stage cannot
be made safe within the approved budget, report that conflict for a user decision;
do not silently remove it. This does not add an LLM judge to ordinary reads.

1. Resolve the trusted host policy and canonical source generation.
2. Collect bounded lexical and semantic candidates, and applicable KG candidates.
   Apply authorized corpus filters before ranking or result limits. A provider
   unable to enforce a restricted policy must refuse that semantic subrequest.
3. Resolve candidates to canonical identities and validate input hashes, source
   versions and current policy. Reject stale or mismatched vectors.
4. Fuse validated ranked lists, hydrate only bounded exact evidence, and recheck
   policy before returning. Preserve literal pages, offsets and citations.
5. Report semantic availability, model/index generation and pending indexing
   explicitly. A resource-refused lexical result must say that semantic coverage
   is unavailable; it must not masquerade as a complete semantic answer.

The current backend cannot be used unchanged as a read-only provider: opening a
collection can create an FTS index or stamp an embedding identity. Adapt the
read-only and identity-verification work from the local provider drafts only
after reviewing it against the canonical policy and resource contracts.

## Identity and freshness

Preserve historical vectors. Reuse them only after verifying the relevant model
contract: model/revision, tokenizer, pooling, normalization, dimension and input
window. An automatically stamped identity is a declaration, not independent proof
of how old vectors were produced. A changed runtime or quantization requires
compatibility and retrieval-quality evidence.

Bind an existing historical vector to its pinned archive, version and physical
row address, then to its canonical occurrence. Repeated drawer aliases are not
unique identities. A vector for a whole old row must not be presented as a vector
for a different chunk or for a separately authorized fragment of that row.

For new text, commit exact content first and durably enqueue semantic work keyed
by canonical identity, exact input hash and embedding profile. Publish index
progress only with a durable receipt. Retrying must be idempotent; interruptions
must preserve the last committed progress. A short-lived worker must check source
and policy validity again before publishing.

If the historical model contract cannot be verified, complete this delivery by
re-embedding the required historical semantic units under a verified profile,
alongside new records. Partial coverage may be exposed during migration, but does
not pass completion. Reconcile the required corpus and projected storage before
starting bulk re-embedding. An exclusion or a separate partial-delivery agreement
requires the user's explicit decision.

Document the embedding input window. Do not silently truncate a long source and
claim complete semantic coverage of its tail. If windowing changes, preserve
exact offsets and distinguish reused historical vectors from newly indexed
semantic units. Re-embedding, if required, is an explicit resumable migration;
dimension matching is not grounds for blindly reusing incompatible vectors.

## Resource qualification before selecting production behavior

Keep current capture/read guards, pressure refusal, single-flight admission and
original backups. Do not raise limits to force a passing result. On 29 October
2026 the user explicitly approved **192 MiB task / 256 MiB parent plus task**
for the new production Lance exact-memory worker: “povolit produkčně 192/256 MiB”.
The existing SQLite capture/coordination workers retain 128 MiB / 192 MiB;
native history qualification retains 512 MiB / 640 MiB. These separate profiles
are not evidence that semantic queries fit the exact-read ceilings.

The user approved a separate 512 MiB task / 640 MiB parent plus task envelope
for the prepared offline semantic diagnostic, with a five-second total deadline.
Its workers run sequentially on CPU, with no permanently resident model and the
existing pressure refusal. This approval does not select a new production
resource profile. The Lance exact-memory production profile is approved above;
this does not activate the replacement or change its two-second reads,
three-second writes, pressure refusal or single-flight admission. Operations
remain short-lived and sequential. A refusal under an approved profile does not
authorize a larger retry. Capture/checkpoint ordering and the separate SQLite
worker limits remain unchanged.

The proposed ordinary semantic deadline is five seconds end to end, including
model startup, candidate collection, optional configured ranking stages and exact
hydration. No stage may restart that total deadline or overlap another model
worker to evade the aggregate budget. A refusal does not authorize a larger retry.

Measure separately, in disposable supervised processes:

- Pinned vector schema and declared identity, without a model or vector-value scan.
- Query embedding, cold and warm, with the effective device and exact model
  profile recorded; no downloads or external calls as hidden fallback.
- Read-only nearest-neighbor retrieval on the actual archive, including caches,
  filtering, cancellation and whole-process memory accounting.
- Background embedding throughput, publication/restart behavior and indexing lag.
- The incremental cost and quality effect of cross-encoder reranking.

Official Sentence Transformers documentation supports alternative inference
backends and quantization. The configured multilingual model's repository lists
an ARM64 quantized ONNX export at about 118 MB on disk. This is a candidate to
qualify, not an approved replacement or a runtime memory measurement. No such
model was downloaded or run during this review. Preserve language coverage and
verify compatibility with retained vectors before considering deployment.

## Proof required for completion

- These service criteria are assistant proposals for the user's review, not
  retrospectively attributed user requirements: under normal system pressure,
  all frozen acceptance queries must complete semantically within five seconds;
  a new small fact must publish to the semantic index within 120 seconds of its
  exact commit while an old backlog is present. Measure actual cold/warm latency,
  indexing throughput, refusal frequency and lag on representative local work.
  A service that usually returns lexical-only results under healthy qualification
  conditions fails. Elevated-pressure refusal remains deliberate and is reported.
- One ordinary query finds a genuinely new captured fact and historical evidence
  through semantic paraphrases without relying on shared exact keywords. Exact
  citation hashes and text remain correct after process restart.
- Every ordinary query entry point uses that same canonical retrieval contract.
  Workspace startup may restore exact work-state references without inventing a
  semantic query or loading a model; it must not claim semantic search occurred.
- A committed new fact is durably queued, becomes semantically searchable through
  the supervised indexer, and remains searchable after worker interruption/retry.
  Coverage reports the real pending work throughout; no stale separate route is
  called complete.
- Historical source counts, retained originals, canonical occurrences and vector
  identity mappings reconcile. The required historical corpus must have verified
  semantic coverage at completion, whether through proven-compatible retained
  vectors or a completed re-embedding migration. The stopped text-conversion count
  alone cannot prove semantic completion.
- Policy restrictions, corrections, repeated aliases, multilingual queries, long
  sources and stale index entries receive meaningful regressions and real-source
  checks. Record measured quality and latency; do not promise universal 100%
  semantic recall.
- Memory/time/disk ceilings and pressure refusal hold on the installed stack;
  background tasks yield to interactive work. Report refusal cases as well as
  successes. No uncontrolled warm model remains resident.
- Rollback preserves exact captured data, originals and durable queue state.
  Do not reactivate an unsafe old pipeline as an automatic rollback.

## Decisions and next step

### Qualification evidence, 2026-10-24

The user approved an isolated 192 MiB task / 256 MiB aggregate diagnostic.
Production ceilings remain unchanged. Five synthetic records, including Unicode,
NUL, empty text and 384-dimensional vectors, passed cold create, append and read
in separate workers. Sampled task peaks were 127.3, 128.6 and 135.8 MiB;
aggregate peaks were 147.3, 149.0 and 156.1 MiB; elapsed times were 1.25, 0.81
and 0.74 seconds. Optional Pandas was absent; no embedding model was loaded.
The 128 MiB task gate had failed. This is a successful small diagnostic under
the explicitly increased test budget, not production qualification or approval
to change production limits. Larger data, semantic inference and retrieval
remain unqualified.

A read-only probe of pinned archive version 26613 verified a float32 vector
column of width 384 and no schema embedding-profile metadata. All 14 snapshot
identities passed validation. Eight original source identities were unchanged;
six had ctime-only differences. Streaming SHA-256 comparison confirmed those
six originals match their pinned snapshot copies; the cause of the metadata
change remains unknown. No source baseline was rewritten.

The separate metadata table declares `paraphrase-ml-MiniLM-L12-v2` (table version
2; all five metadata-file identities unchanged). This is not a verified model
generation profile because the old backend can stamp the current identity onto
an existing palace. The pinned drawers table describes one inverted text index
covering 519,641 rows and no vector index. Stored vectors remain present; fast
semantic retrieval over the full archive has not yet been qualified.

An isolated candidate for semantic qualification is the official ARM64 qint8
ONNX export of the configured multilingual model, pinned to revision
`e8f8c211226b894fcb81acc59f3b34ba3efd5f42`. Its 118,412,398-byte download was
verified against SHA-256
`783fea82d71a58179b830a4dbd2d58447e640609e98eedf9ffa12622d375a672`.
The cached tokenizer/configuration comes from the same revision. The model's
128-token input window and attention-mask mean pooling are explicit.

The approved synthetic semantic diagnostic passed after tokenization and model
inference were separated into sequential disposable workers. The combined
process had exceeded its 512 MiB task limit while loading the model; tokenizer
initialization alone consumed most of that budget. The split run kept the same
tokenizer, pinned weights, pooling, normalization and default ONNX graph settings.
One shared five-second deadline covered both stages. All three Czech, German and
Spanish paraphrases ranked the expected English record first among eight
synthetic documents. Elapsed time was 1.0194 seconds; sampled peak task RSS was
422.6 MiB and peak parent-plus-task RSS was 443.3 MiB. Tokenization loaded no
inference runtime and inference loaded no tokenizer. This measures synthetic
embedding and in-memory cosine ranking, not Lance retrieval or full-corpus
quality. Historical compatibility, long-input semantic coverage, larger batches,
reranking and the complete retrieval pipeline remain unqualified. The candidate
is not selected for production and production resource limits are unchanged.

Private evidence: `lean-lance-split-192-gate.json`,
`semantic-vector-schema-verified.json` and
`semantic-source-ctime-content-audit.json`, `semantic-stored-identity.json`,
`semantic-vector-index-inventory.json` and `semantic-onnx-split.json` under the operator's
`~/.castle/backups/durable-memory-20261024/relocation/` directory.

The user selected B. The first exact operation gate failed under the unchanged
limits and passed only the explicitly approved small diagnostic at 192/256 MiB.
Continue bounded qualification and inactive preparation; changing the production
resource profile or activating the replacement still requires a decision. Semantic
capability preservation is required; ordinary FTS5 alone is not its substitute.
The builder can select a measured compatible local runtime and implementation
details within these limits. A different model/language coverage, increased
budget, relaxed quality/availability gate or partial historical delivery is a
material change requiring a new explicit decision. Failed qualification does not
authorize any of them. The currently stopped conversion must not restart merely
because an older lexical-only completion checklist says it should.

Sources: current installed code in `searcher.py`, `unified_memory.py`,
`brain_api.py`, `embedding.py` and `backends/lancedb_backend.py`; local provider
draft and its recorded pilot; current private qualification artifacts. Primary
external references (read on 2026-10-24):

- [LanceDB hybrid search](https://docs.lancedb.com/search/hybrid-search)
- [Sentence Transformers inference backends and quantization](https://www.sbert.net/docs/sentence_transformer/usage/efficiency.html)
- [Official model ONNX artifacts](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2/tree/main/onnx)
