# Changelog

All notable changes to [Cognitive Castle](https://github.com/B3Cognition/cognitive-castle) (formerly MemPalace) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Fixed
- `castle status` reads the palace in one pass over the `wing`, `room` and
  `metadata_json` columns instead of paging with `col.get(offset=...)`, which
  rescanned the table from row zero for every 5,000-row page (104 scans, 66 s
  and 1.2 GB on a 520K-drawer palace; now 3 s and 0.4 GB with identical output).
- **`castle prune-revisions` no longer reads drawer text in 10,000-row
  batches.** Legacy whole-file drawers run to 100+ MB each, so one batch of
  them was several GB of text at once and the dry run was OOM-killed on a
  20 GB palace (exit 137). The plan is now built from metadata alone and the
  size report reads `length(text)` eight rows at a time: 20 s and 2.6 GB peak
  on 877k rows where the previous version never finished.

- **Stop/PreCompact hook mines one transcript, bounded.** The Claude Code
  hook ran `castle mine <project dir> --mode convos` every `SAVE_INTERVAL`
  exchanges: every sibling session and every `tool-results/*.txt` dump in
  the directory was re-normalised each fire (109K junk drawers on one
  palace), the growing transcript was re-filed as a complete new revision
  each time (one 594 MB session: 20 revisions, 361K drawers), and the
  palace-wide KG backlog ran afterwards. The hook now mines only the
  transcript it was handed, with `--skip-kg`, and skips transcripts over
  `CASTLE_HOOK_MAX_TRANSCRIPT_MB` (default 100, `0` disables) with a log
  line; those are for an explicit `castle mine`. `_pid_alive` no longer
  trusts pid 0 or 1, so a stale `mine.pid` holding `1` cannot block every
  future mine.
- **Metadata-only reads no longer load text and vectors.** The LanceDB
  `get()` projected every column whatever `include` asked for, so a
  `castle_status` / `list_wings` / `file_already_mined` call materialised
  the 800-char text and the embedding of every matching row — gigabytes on
  an 875K-drawer palace, inside the long-lived MCP server. `get()` now
  selects only the requested columns, the MCP wing/room counts stream the
  two `wing`/`room` columns instead of fetching metadata rows (3 s, ~200 MB
  peak on that palace), and `_fetch_all_metadata` is one linear call rather
  than quadratic `offset` paging.

### Added

- **`castle info-stats --export` (#88).** The v3.4.x research is measured
  on one palace. The new command computes what a second palace would need
  to reproduce its structural claims — per-wing drawer and room counts, text
  length and nearest-neighbour novelty histograms (prior drawers of the
  same wing, reservoir-sampled, `--sample` per wing), exponential vs
  power-law decay fits of novelty over filing order, and a Kruskal-Wallis
  heterogeneity test — and nothing else: no text, vectors, ids or paths,
  and wing names only with `--keep-wing-names` or `--wing-alias`. The
  export is a JSON file to read before sharing by hand. The research
  pipeline's `python -m cli merge-stats a.json b.json` compares exports
  (novelty medians, decay winners, ε², Jensen-Shannon distance of the
  pooled histograms). ρ(A, B) still needs the full pipeline on a snapshot.

- **`castle prune-revisions` (#95).** The miners append a complete new
  revision of a source file's drawers whenever the file changes and keep
  the old ones. For a live transcript that the Stop hook re-mines after
  every turn, each pass stores a full copy of everything mined before: one
  palace held 339k such copies, 40% of its rows, for 30 session files. The
  new command lists them per source file (dry run by default) and with
  `--apply` removes every revision but the newest complete one, plus the
  legacy drawers of the pre-v3 chunker for files that have a current
  revision — which is where the whole-session "giant drawers" of up to
  142 MB came from (344 of them, 12 of 13.5 GB of text; the chunker has
  bounded drawers since v3's `NORMALIZE_VERSION = 3`). An incomplete
  revision newer than the kept one is never touched, and applying is
  refused while a hook miner's pid is alive. Lance frees the space on the
  next `castle optimize --prune-older-than`.

### Fixed

- **Bound Codex transcript capture.** Stop/PreCompact captures now use the
  per-palace nonblocking mining lock and skip archive-wide KG enrichment.
  A live `~/.castle/codex-capture.disabled` marker pauses capture even when a
  running harness retains old hook registrations. Conversation dry runs no
  longer enrich the knowledge graph. See [capture safety](docs/codex-capture-safety.md).

- **KG enrichment on a large palace (#95).** Every run failed with
  `'CognitiveCastleConfig' object has no attribute 'languages'`: the enricher
  read `cfg.languages` where the config's field is `entity_languages`, and
  the unit tests' `MagicMock` config accepted any name (238 failures in one
  `hook.log`; the mock is now `spec_set` to the real config). With the
  crash fixed, a run still could not finish in hours: `list_drawer_ids()`
  materialised every column of the table (`to_arrow()`, 13 GB of text and
  vectors on a 500k-drawer palace) and each 1,000-drawer batch of
  `get_by_ids` was an unindexed `id IN (...)` scan of the whole table. The
  backend now reads the `id` column alone and streams `(id, text)` in one
  pass (`LanceCollection.iter_id_text`), which the enricher leaves as soon
  as every wanted drawer was seen. Entity extraction reads the first 200 KB
  of a drawer (344 whole session files filed as single drawers held 12 of
  13.5 GB of text), and candidate scoring sees only the text within 80
  characters of each occurrence of the name instead of ~40 regexes over
  whole 20-drawer samples — same scores, verified on 2,644 real candidates.

- **Stop/PreCompact hooks stacked transcript miners (#95).** Both hooks
  reach `_ingest_transcript`, which had no concurrency guard: on a long
  session they started a new `castle mine` over the same transcript
  directory every fire — three at once, each writing into the same palace.
  The transcript mine now claims its own lock file (`mine_transcript.pid`,
  separate from `mine.pid` so a project mine never blocks it) atomically
  with `O_CREAT | O_EXCL`, hands it to the child pid, reclaims it when the
  holder is dead, and `_spawn_mine` claims `mine.pid` the same way, closing
  the read-then-write race between two hook fires.

### Added

- **Normalizer memory on large exports.** `normalize()` split the whole file
  into a list of lines (twice, with stripped copies) before parsing, so a
  310 MB Codex rollout that reduces to 18 drawers peaked at ~2.9 GB and tipped
  the miner over its memory cap. The file is now read as bytes and decoded one
  line at a time (a `str` of a mostly-ASCII export with a few emoji is UCS-4,
  4× the bytes), the `>` marker / JSON-start checks no longer copy the text,
  and tool-output records are skipped before `json.loads`; peak stays near
  the file size. `tests/test_normalize.py` guards the bound with `tracemalloc`.

- **Bounded KG enrichment.** Phase 2 of every `castle mine` (and so of
  every Stop-hook ingest) walked *all* drawers not yet covered by a triple in
  one pass. On a palace of 800k drawers that built a mention map of tens of
  GB, the kernel killed the miner, nothing was ever recorded as done, and the
  next hook started over — a background thrash loop that looked like a stuck
  miner. Enrichment now runs in increments of `kg_enrich_max_drawers`
  (default 20,000; `CASTLE_KG_ENRICH_MAX_DRAWERS`, `0` = unbounded) and
  records walked drawers in an `enrich_scanned` table of the knowledge-graph
  SQLite, so drawers without any mention are not re-walked and each run
  continues where the previous stopped. `castle mine --skip-kg` (or
  `CASTLE_SKIP_KG_ENRICH=1`) leaves Phase 2 out entirely for bulk backfills.

- **`castle optimize`.** Every Stop-hook ingest lands as its own LanceDB
  fragment and the Tantivy FTS index is only built at table creation, so a
  palace filed for months ends up with thousands of one-row fragments and
  an FTS index covering a fraction of the rows (one real palace: 4,713
  fragments, 773k of 792k rows unindexed, 127 s per sparse query). The new
  command rebuilds the FTS index over every row, then compacts fragments,
  as a new version — drawers and old versions are untouched. Disk is only
  reclaimed with the explicit `--prune-older-than DAYS` (plus `--prune-unverified`
  for files a killed compaction or index build left behind). `--fts-incremental`
  adds only not-yet-indexed rows to the existing FTS index (Lance
  `optimize_indices`), the routine step after filing; the full rebuild of an
  800k-row palace needs >10 GB, the delta a fraction. `--dry-run` reports
  fragments, versions and FTS coverage; `--json` emits the report. Compaction,
  the incremental index update and the prune are LanceDB's own
  `Table.optimize()` pass (no `pylance` dependency); `--fts-heap-mb` bounds
  the full rebuild's writer heap on a machine that is already swapping.

- **Info-aware filing (opt-in).** Every mined drawer now carries a
  `novelty` metadata score (prior-only nn_novelty — research basis:
  ρ(A,B)=0.982, v3.4.x papers). `castle search --info-weight` (or
  `info_weight_enabled` config) demotes — never drops — near-duplicate
  drawers in ranking. `castle repair --backfill-novelty` tags the
  existing palace (resumable). `castle status` shows per-wing
  information health. Default OFF pending the LongMemEval R@5 gate
  (spec: docs/superpowers/specs/2026-07-26-info-aware-filing-design.md).

## [3.4.1] — 2026-07-24

Follow-up release closing out the v3.4.0 research and shipping the
memory fixes that made the full-palace run finish.

### Research paper closeout

The information-theory paper (`research/info_theory/paper/`) now
carries the completed A↔C and B↔C correlations from a real 792-drawer
subsample against the 65,779-drawer palace:

- $\rho(A, B) = 0.9820$ (95% CI [0.9817, 0.9823], $n=65{,}746$) — unchanged
- **$\rho(A, C) = 0.2023$** (95% CI [0.134, 0.262], $n=792$) — NEW
- **$\rho(B, C) = 0.2015$** (95% CI [0.126, 0.267], $n=782$) — NEW

The weak A↔C / B↔C correlations are the paper's core methodological
finding: the LLM judge measures **contextual predictability** — an
axis of information content that is orthogonal to embedding geometry.
Production systems that need geometric novelty can use the O(1)
`nn_novelty` (rho with LLE = 0.982); systems that need to detect
predictable/boilerplate/continuation content need the LLM signal
because the geometric estimators cannot substitute for it.

Paper's 3 `\todo{}` markers are now closed. Appendix carries the
verbatim system + user prompt template, cost-per-call ($0.031),
invocation flags (including the `--strict-mcp-config` fix), and the
$15 budget cap that stopped the batch at $n = 787$.

### Bug fixes — enabled the full-palace run

- **`ClaudeCliProvider` now passes `--strict-mcp-config --mcp-config
  <empty>`.** Each `claude -p` invocation was spawning user-configured
  MCP servers (context7-mcp, playwright-mcp, ...) as short-lived Node
  subprocesses — ~130 MB × 4 servers = ~520 MB transient per call.
  cgroup memory accounting saw the spike; systemd-oomd targeted the
  process around drawer 200–300 across three restarts. Passing the
  empty MCP config drops the per-call child count from +4 to +1 and
  keeps peak-per-call under 2.2 GB (was ~2.6 GB). The C-stage does
  not need MCP servers — this is pure overhead removal.
- **`_stage_llm_surprise` in `run_experiment.py` now projects the
  lean columns** (`drawer_id`, `text`, `wing`, `filed_at`) before
  `to_pylist()`. The unpatched path materialised the 1024-dim vector
  column as a Python list-of-floats for all 65k drawers — ~1.8 GB of
  Python object overhead. Explicit `del + gc.collect()` after
  building `priors_lookup` and after C-stage entry.
- **`ClaudeCliProvider` timeout bumped from 120 s → 600 s** with per-drawer
  error skipping in `process_subsample` (originally PR #65; carried into
  v3.4.1's stable run).

### Notes

- No public API changes vs 3.4.0 — CLI, MCP tool list, and library
  entry points are identical.
- Cumulative C-stage cost so far: ~$16 across all attempts
  (including debugging restarts). Rerunning against `--force` would
  spend ~$25 for a fresh 925-drawer sample.

---

## [3.4.0] — 2026-07-23

First Castle-branded release since forking from
[MemPalace v3.3.3](https://github.com/MemPalace/mempalace) (2026-04-23).
Summarises 463 non-merge commits of Castle-specific work. See the
[Acknowledgements](README.md#acknowledgements) for the split between
upstream (MemPalace / Milla Jovovich) design and Castle's integration
layer.

### Added

- **LanceDB backend** replaces upstream Chroma across storage, mining,
  and retrieval. All Castle work is exercised against LanceDB; a
  reindex is required for palaces built pre-cutover (see README
  "Migration from MiniLM" section for the exact command). The
  abstraction in `cognitive_castle/backends/base.py` keeps future
  backends possible.
- **3-stage retrieval pipeline** — parallel dense (BAAI/bge-m3 default,
  paraphrase-multilingual-MiniLM legacy) + sparse Tantivy FTS +
  KG-hop → weighted Reciprocal Rank Fusion + recency multiplier →
  cross-encoder rerank (BAAI/bge-reranker-v2-m3 default; opt-in
  mxbai-rerank-large-v2 for English-only corpora).
- **Stage 4 LLM-as-judge re-rank** (`--llm-rerank`) — reorders Stage 3
  top-N via a configured LLM. Graceful fallback to Stage 3 on any
  LLM failure; `JUDGE` audit line records what happened.
- **Stage 5 SOAR symbolic re-ranking** (opt-in via
  `CASTLE_SOAR_ENABLED=1` + `--soar-boost`) — 4 hand-crafted
  productions (recency-boost, same-project, entity-match, type-match)
  add boost-tags with full audit trail. Composable with Stage 4 via
  `--soar-first`.
- **Stage 6 deterministic text-quality re-rank** — powered by the
  vendored `understanding` package (v3.7.0, MIT). 31 standards-based
  metrics (IEEE 830, ISO 29148, readability, cognitive load) applied
  as a two-tier threshold (medium ×1.15, high ×1.25) on top of
  relevance ranking.
- **Claude Code plugin** (`.claude-plugin/`) — auto-registers the MCP
  server + Stop / PreCompact background-mining hooks. Install with
  `/plugin marketplace add /path && /plugin install castle@cognitive-castle`.
- **MCP `initialize.instructions` injection** (PR #48) — bakes
  `PALACE_PROTOCOL` + AAAK spec + live palace state into the client's
  system prompt at session start.
- **`claude-cli` LLM provider** (PR #49) — reuses the parent Claude
  Code's auth for Stage 4 judge instead of requiring a separate
  `ANTHROPIC_API_KEY`.
- **Research subproject** `research/info_theory/` (PR #56) — an
  information-theoretic study of verbatim personal memory. Three
  drop-in estimators (`nn_novelty`, LLE `recon_residual` with
  Tikhonov, `llm_surprise`), full statistical stack (weighted
  power-law + exponential fits with AIC selection, source-aware block
  bootstrap, Kruskal-Wallis + ε², Benjamini-Hochberg FDR at q=0.05,
  LOWESS-based N* saturation), 108 unit tests, resumable driver
  (`run_experiment.py`), CLI orchestrator, reproducibility manifest,
  paper skeleton, CI workflow. First full-palace run headline
  (65,779 drawers, 7 wings): $\rho(A, B) = 0.9820$ with 95% CI
  $[0.9817, 0.9823]$; exponential decay wins over power-law in all
  6 fittable strata; source-type heterogeneity real but modest
  ($H = 5360$, $p \approx 0$, $\varepsilon^2 = 0.081$).
- **Vectorised KNN** `compute_all_neighbors` — ~100× speedup unlocks
  full-palace analysis at 65k drawers in minutes instead of days.

### Improvements

- **CLI rebranding** — `castle` console script (preferred) alongside
  `castle-mcp`. `python -m cognitive_castle.cli` still supported.
- **Attribution** — README restructured; MemPalace inspiration credit
  moved to a prominent box after the pitch. New "Research &
  Mathematics" section at the bottom links to the info-theory paper
  and estimator formulas.
- **CI** — new `research-info-theory.yml` workflow runs the research
  subproject's unit + integration tests on Python 3.11 and 3.12,
  plus a conditional Tier-4 reproducibility check when
  `paper/results.yaml` changes in a PR.

### Bug Fixes

- `load_palace` now returns the drawer identifier as `drawer_id` (not
  the raw LanceDB `id`), matching the cross-loader contract with
  `load_longmemeval`.
- `cmd_pilot` graceful missing-data path — surfaces a clear message
  instead of `TypeError` when the LongMemEval JSON is not present.

### Notes

- The `## [3.3.4] — unreleased` and `## [3.3.5] — unreleased` sections
  below are upstream MemPalace draft entries inherited at fork time.
  They have not been vetted against Castle's LanceDB architecture and
  may not apply. A separate audit will decide which entries land in
  a future Castle release.
- No PyPI publication is automated; the release is tag + GitHub
  release page only.

---

## [3.3.5] — unreleased

### Bug Fixes

- **`mempalace_diary_read` silently dropped entries on agent-name case mismatch.** `tool_diary_write` stored the `agent` metadata verbatim after `sanitize_name`, which preserves case, while `tool_diary_read` filtered by exact match. Writing as `"Claude"` and reading as `"claude"` (or vice-versa) returned zero rows. Both endpoints now lowercase `agent_name` immediately after sanitization, so reads are case-insensitive and the default per-agent wing slug is stable across casings. **Behavior change:** entries written prior to this fix under mixed-case agent names will not match the new lowercase filter; run `mempalace repair` if you need to migrate legacy diary metadata. (#1243)

---

## [3.3.4] — unreleased

### Added

- **`mempalace init` now prompts to mine the same directory.** After entity confirmation, room detection, and gitignore guard, `init` shows a one-line scope estimate (e.g. `~423 files (~12 MB) would be mined into this palace.`) computed from its existing corpus walk, then asks `Mine this directory now? [Y/n]` (default yes) and runs `mine()` in-process if accepted. The estimate fires before the prompt so users on a real corpus aren't surprised by a minutes-long ChromaDB write. Declining prints the exact `mempalace mine <dir>` command for later. (#1181)
- **New `--auto-mine` flag on `mempalace init`** for the non-interactive path (`mempalace init --auto-mine <dir>` skips the mine prompt and runs mine directly). `--yes` retains its existing scope of entity auto-accept only and still prompts for the mine step, so existing scripted callers see no behaviour change; combining `--yes --auto-mine` gives a fully non-interactive setup. (#1181)
- **Cross-wing topic tunnels.** When two wings have confirmed `TOPIC` labels in common (the LLM-refine bucket from `mempalace init --llm`), the miner now drops a symmetric tunnel between them at mine time so the palace graph reflects shared themes (frameworks, vendors, recurring concepts). Tunnels are routed through the existing `create_tunnel` storage so they share dedup and persistence with explicit tunnels. Topic tunnels are stored under a synthetic `topic:<name>` room and tagged with `kind: "topic"` on the stored dict — this keeps them distinct from literal folder-derived rooms of the same name (a wing with both an `Angular` folder room and an `Angular` topic tunnel no longer collides at `follow_tunnels` read time) and gives LLMs scanning `list_tunnels` a visible discriminator. Threshold is configurable via `MEMPALACE_TOPIC_TUNNEL_MIN_COUNT` env var or `topic_tunnel_min_count` in `~/.mempalace/config.json` (default `1`). Manifest-dependency overlap and per-topic allow/deny lists remain out of scope. (#1180)
- **Context-aware corpus detection at `mempalace init`.** A new Pass 0 runs at the start of `init` — before entity detection — and answers one question: *is this corpus an AI-dialogue record, and if so, which platform and what persona names has the user assigned to the agents?* Tier 1 is a free regex heuristic (well-known AI brand terms + turn-marker patterns, with a co-occurrence rule that suppresses ambiguous terms like `Claude`/`Gemini`/`Haiku` when no unambiguous AI signal is present, so French novels and astrology forums don't false-positive). Tier 2 is an LLM call (~$0.01 with Anthropic Haiku, free with local Ollama/LM Studio/llama.cpp/vLLM) that extracts `user_name` and `agent_persona_names` from dialogue structure. Result is persisted to `<palace>/.mempalace/origin.json` with a `schema_version: 1` envelope so downstream tools can read it. Entity classification then routes names matching `agent_persona_names` (case-insensitive) into a new `agent_personas` bucket instead of `people`, so a Claude Code transcript no longer misclassifies the user's `Echo`/`Sparrow`/`Cipher` agents as biological people. `llm_refine` receives the same context as a system-prompt preamble so it can disambiguate other ambiguous candidates with corpus-level knowledge too. Backwards compatible: callers that don't pass `corpus_origin` see the v3.3.3 return shape unchanged. (#TBD)
- **`mempalace init` runs LLM-assisted refinement by default.** v3.3.3 made `--llm` opt-in; the LLM-assisted path is qualitatively better (extracts persona names, refines ambiguous classifications) so it now runs by default. Provider precedence is unchanged — Ollama at `http://localhost:11434` first, then openai-compat, then anthropic with API key. **Never blocks init on a missing LLM**: if no provider is reachable (Ollama not running, no API key set), init prints a one-line message pointing at `--no-llm` and falls through to the heuristic-only path. `--no-llm` is the new explicit opt-out. The legacy `--llm` flag is preserved as a deprecated alias of the default so scripted callers see no behaviour change. Cost story: zero for users with a local LLM (the majority on this repo), ~$0.01 per init for users with `ANTHROPIC_API_KEY` set who explicitly choose `--llm-provider anthropic`, zero for users with no LLM (graceful fallback). (#TBD)
- **`mempalace mine --redetect-origin` flag.** Re-runs corpus-origin detection on the current corpus state and overwrites `<palace>/.mempalace/origin.json`. Useful when the corpus has grown since `mempalace init` and the stored origin may be stale. Heuristic-only by design (the flag is meant to be cheap); re-run `mempalace init` for full Tier 2 LLM refinement. Default `mempalace mine` does not touch `origin.json` — the flag is opt-in. (#TBD)

### Bug Fixes

- **MCP server `tool_diary_write` SIGSEGV when default EF provider differs.** `mcp_server._get_collection` bypassed `ChromaBackend.get_collection` and called `client.get_collection` / `client.create_collection` without `embedding_function=`. ChromaDB 1.x persists the EF *identity* (its `name()`) with the collection but not the EF *instance/configuration*, so the MCP server's reopen silently bound chromadb's built-in `DefaultEmbeddingFunction` — its `name()` matches `mempalace.embedding`'s spoofed `"default"` so the identity check passes, but its provider list is chromadb's default rather than the user's resolved device. The miner / Stop hook ingest path routes through the backend helper and binds the configured EF instead. On bleeding-edge interpreters (python 3.14 + chromadb 1.5.x on Apple Silicon) the default provider selection could SIGSEGV the host process on first `col.add()`, killing the MCP stdio server and leaving every subsequent tool call returning `Connection closed` until Claude Code was relaunched. `_get_collection` now reuses `ChromaBackend._resolve_embedding_function()` on the reopen branches that actually open a collection (warm-cache reads stay zero-cost), matching the miner/backend path. (#1299, follow-up to #1262 / #1289)
- **Cross-wing topic tunnels for hyphenated dir names.** `mempalace init` recorded the `topics_by_wing` registry key under the raw directory name (e.g. `mempalace-public`), while `mempalace.yaml`'s `wing` field used the lower-cased + separator-collapsed slug (`mempalace_public`). At mine time the miner read the slug from the yaml and missed the registry, so `_compute_topic_tunnels_for_wing` returned `0` silently. Real-world: any project whose folder contained a hyphen or space lost every topic tunnel. Now both call sites route through a shared `normalize_wing_name()` in `config.py`. (#1194, follow-up to #1180)
- **CLI `mempalace search` retrieval quality.** The CLI was using pure ChromaDB cosine distance with no BM25 rerank, so drawers containing every query term but embedding as noise (directory listings, diff output, shell logs) scored `Match: 0.0` alongside genuinely irrelevant results with no way to tell them apart. Wired the CLI through the same `_hybrid_rank` the `mempalace_search` MCP tool already used, and surfaced both `cosine=` and `bm25=` scores in the output so users see which component of the match is firing. MCP search was unaffected; this fixes the human-facing CLI parity gap.
- **Legacy-palace distance-metric warning.** CLI search now detects palaces created before `hnsw:space=cosine` was consistently set and prints a one-line notice pointing at `mempalace repair`. Without the warning such palaces silently used L2 distance, under which the similarity display floored every result to `Match: 0.0`. New palaces mined today already set cosine correctly and now have invariant tests pinning that behavior so future refactors can't silently regress it. (#1179)
- **Graceful Ctrl-C during `mempalace mine`.** Interrupting a long mine no longer dumps a multi-frame `KeyboardInterrupt` traceback. The main file-processing loop now catches the signal, prints `files_processed: N/M`, `drawers_filed: K`, and `last_file:` so the user knows what landed, then exits with code 130 (standard SIGINT). Already-filed drawers are upserted idempotently on re-mine via deterministic IDs, so resuming is safe. The hooks PID lock at `~/.mempalace/hook_state/mine.pid` is now also actively cleaned up in a `finally` when its entry points at us — clean exit, error, or interrupt — preventing the next hook fire from briefly waiting on a stale PID. (#1182)
- **`mempalace init` is now idempotent across re-runs.** Running `init` twice on the same project produced different `origin.json` results because the first run wrote `entities.json` into the project directory, and the second run's corpus-origin sampling included that file as corpus content — shifting Tier 1's character-density math. Sampling now skips the per-project artifacts (`entities.json`, `mempalace.yaml`), so re-running `init` produces the same classification it did the first time. Pinned by an integration test in `tests/test_corpus_origin_integration.py`. (#TBD)

---

## [3.3.3] — 2026-04-23

### Bug Fixes

- **Install regression** — `mempalace-mcp` console script is now declared in `pyproject.toml` alongside `.claude-plugin/plugin.json`'s reference to it. In v3.3.2 the two drifted apart (plugin.json shipped the new `"command": "mempalace-mcp"` form before the matching entry point landed), so every fresh `pip install mempalace==3.3.2` produced a Claude Code plugin config pointing at a binary that wasn't installed. (#1093, #340)
- Restore silent-save visibility after the Claude Code 2.1.114 client regression — production transcript saves were failing silently until this PR. (#1021)
- Paginate `status`-path metadata fetches so large palaces don't trip SQLite variable limits. (#851)
- Resolve the Claude plugin hook runner across platform / plugin-dir variations; previously broke on Windows and some macOS layouts. (#942)
- Real `python3` resolution for `.sh` hooks with a `MEMPAL_PYTHON` override path. (#833)
- Add optional `wing` parameter to `tool_diary_write` / `tool_diary_read` and derive per-project wing from the Claude Code transcript path when writing from the stop hook — diary entries from different projects no longer collapse into a shared default wing. (#659)
- Treat empty string as "no filter" in `mempalace_search` `wing`/`room`; LLM agents that default to filling every optional parameter with `""` no longer get bounced with `must be a non-empty string`. (#1097, #1084)
- Broaden `_wing_from_transcript_path` to handle Claude Code project folders without a `-Projects-` segment (e.g. `~/dev/<parent>/<project>`, `~/code/<project>`). The project name is now derived from the final dash-separated token of the encoded folder, so Linux users with code outside `~/Projects/` get per-project diary scoping instead of falling through to `wing_sessions`. (#1145, follow-up to #659)
- `mempalace_diary_read(wing="")` now returns diary entries from every wing this agent has written to, matching the #1097 "empty-string as no filter" pattern. Previously defaulted to `wing_<agent>`, siloing entries that hooks wrote to project-derived wings. (#1145)
- `mempalace mine` now skips the generated `entities.json` file so its contents aren't re-ingested as project content. (#1175)

### Improvements

- **Deterministic hook saves.** Save hook now uses a silent Python API path, so successive hook invocations produce reproducible results and zero data loss on the hot path. (#673)
- **Graph cache with write-invalidation** inside `build_graph()` — warm-path calls no longer rebuild the palace-graph per request. (#661)
- **`mempalace init` entity detection overhaul.** Canonical project names now come from package manifests (`package.json`, `pyproject.toml`, `Cargo.toml`, `go.mod`) and real people come from git commit authors, rather than being inferred from prose. Includes union-find dedup across name/email aliases, bot filtering that keeps `@users.noreply.github.com` humans, and automatic "mine" flagging by contribution share. (#1148)
- **Regex detector accuracy.** CamelCase extraction so `MemPalace`, `ChromaDB`, `OpenAI` aren't fragmented; tighter versioned/hyphenated pattern kills `context-manager` / `multi-word` false positives; dialogue `^NAME:\s` requires ≥2 hits so `Created: <date>` metadata stops classifying field names as people; expanded stopwords for common English participles and descriptors; high-pronoun signal classifies as person rather than dumping to uncertain. (#1148)
- **Init → miner wire-up.** Confirmed entities merge into `~/.mempalace/known_entities.json` on init, which the miner reads to tag drawer metadata for entity-filtered search. Previously init's output was not consumed by the miner; the per-project `entities.json` is kept as an audit trail. (#1157)
- **Case-insensitive project dedup** across manifest, git, and convo sources so casing variants of the same project name collapse into one review entry. (#1175)

### Added

- i18n: Belarusian translation. (#1051)
- i18n: entity detection for German, Spanish, and French locales. (#1001)
- i18n: Traditional + Simplified Chinese entity detection. (#945)
- **`mempalace init --llm`**: optional LLM-assisted entity classification. Defaults to local Ollama (zero-API); also supports any OpenAI-compatible endpoint (LM Studio, llama.cpp server, vLLM, OpenRouter, etc.) and the Anthropic Messages API. Runs interactively with a progress indicator; Ctrl-C cancels cleanly and returns partial results. Useful for prose-heavy folders where the regex detector struggles (diaries, transcripts, research notes). Opt-in only — default init path remains zero-API. (#1150)
- **Claude Code conversation scanner.** `~/.claude/projects/<slug>/` directories now contribute project entities using each session's authoritative `cwd` metadata, avoiding slug-decoding ambiguity. (#1150)

### Known — deferred to v3.3.4

- HNSW parallel-insert SIGSEGV when `hnsw:num_threads` is unset on collection creation (#974) — fix in-flight as #976, awaiting rebase against develop.

---

## [3.3.2] — 2026-04-19

### Bug Fixes

- Fix silent drop of `.jsonl` files in project miner; raise `MAX_FILE_SIZE` cap from 10 MB to 500 MB so large transcripts no longer fall through unnoticed. Adds a tandem **sweeper** — a message-level, timestamp-coordinated, idempotent safety net that catches anything the primary miner missed. (#998)
- `mempalace sweep <target>` CLI to run the sweeper on demand against a transcript file or a directory. (#998)
- Guard `Layer3.search_raw` against `None` doc/meta rows returned by ChromaDB — prevents `AttributeError` crashes on mixed-schema palaces. (#1011, #1013)
- Guard searcher API path, closet loop, and miner status histogram against `None` metadata; matching guards added to `tool_status` / `list_wings` / `list_rooms` / `get_taxonomy` in the MCP server. (#999)
- Upgrade `chromadb` floor to `>=1.5.4` for Python 3.13 / 3.14 compatibility and pin upper bound to `<2` so future breaking majors don't silently install. (#1010)
- Fix Unicode checkmark rendering on Windows terminals that can't encode the `✓` glyph — avoids `UnicodeEncodeError` crashes on first-run output. (#681)
- **`quarantine_stale_hnsw`** — on open, detect HNSW segment directories whose `data_level0.bin` is significantly older than `chroma.sqlite3` and rename them out of the way. Recovers cleanly from HNSW/sqlite drift that otherwise causes SIGSEGV on `count()` / `query(...)` (the chroma-core/chroma#2594 failure mode). Rebuilds the index lazily on next use. (#1000)
- **PID file guard** — `mine` writes a per-source-directory PID file and refuses to start if an existing mine is still running, preventing process stacking that bloats HNSW and wedges concurrent writes. Includes cross-platform PID liveness check (`os.kill(pid, 0)` terminates on Windows, so the guard falls back to a platform-aware probe). (#1023)

### Improvements

- **RFC 001 §10 — typed backend contracts.** `BaseBackend` now returns typed `QueryResult` / `GetResult` dataclasses and `PalaceRef` for palace identity; registry-based backend discovery. Internal refactor; no user-facing API change. (#995)
- **RFC 002 §9 — source adapter scaffolding.** Introduces `BaseSourceAdapter`, adapter registry, and `PalaceContext` — the plumbing that future pluggable ingest sources will target. Internal refactor; no user-facing API change yet. (#1014)

### Documentation

- **RFC 002** — full specification for the source adapter plugin system (future pluggable ingest). (#990)
- First-run help text and `README` now reference the real `~/.claude/projects/<project>/` path shape instead of the placeholder `/path/to/transcripts`. (#996, #1012)

### Internal

- Harden sweeper for production: verbatim tool blocks, full `session_id`, logged failures.
- Address Copilot review on #995: cursor tie-break, honest metrics, accurate comments.
- Test hygiene: avoid ONNX network download in update-length validation tests; dedup update-length-validation tests; fix Windows file-lock in cache-invalidation test.

---

## [3.3.1] — 2026-04-16

### New Features

**Multi-language entity detection** — lexical patterns (person verbs, pronouns, dialogue markers, project verbs, stopwords, candidate character classes) now live in the optional `entity` section of each locale JSON under `mempalace/i18n/<lang>.json`. Every public function in `entity_detector` accepts a `languages=` tuple and unions patterns across enabled locales. Default stays `("en",)` so existing English-only callers are unchanged. (#911)

- **Five new fully-supported locales** with CLI strings, AAAK compression instructions, and entity-detection patterns:
  - Brazilian Portuguese `pt-br` (#156)
  - Russian `ru` (#760)
  - Italian `it` (#907)
  - Hindi `hi` (#773)
  - Indonesian `id` (#778)
- **`MempalaceConfig.entity_languages`** — persistent palace-level language selection; `MEMPALACE_ENTITY_LANGUAGES` env override; `mempalace init --lang en,pt-br` flag that saves to `~/.mempalace/config.json` (#911)
- **Per-language `candidate_pattern`** — non-Latin scripts register their own character class, so names like `João`, `Инна`, `राज` are no longer silently dropped by the ASCII-only default (#911)
- **VSCode devcontainer** matching the CI environment (#881)
- `MEMPAL_VERBOSE` env toggle — developers see diaries surfaced in chat while the default remains silent (#871)
- `created_at` timestamps included in search results (#846)

### Bug Fixes

**i18n / Unicode**

- Script-aware word boundaries for combining-mark scripts — Python's `\b` fails on Devanagari vowel signs (`ा ी ु`), Arabic, Hebrew, Thai, Tamil, Khmer etc., truncating names like `अनीता` → `अनीत` and making person-verb patterns never fire. Locales now declare an optional `boundary_chars` field and the i18n loader expands `\b` into a script-aware lookaround boundary (#932)
- Case-insensitive BCP 47 language code resolution — `--lang PT-BR`, `zh-cn`, `Pt-Br` previously fell through to English silently; now resolve to the canonical locale file via lowercase matching, with the entity-pattern cache keyed on the canonical form so casing variations share one cache entry (#928)
- Wire i18n candidate patterns into `miner._extract_entities_for_metadata()`, `palace.build_closet_lines()`, and `entity_registry.extract_unknown_candidates()` — three code paths that still hardcoded ASCII-only `[A-Z][a-z]{2,}` and silently missed Cyrillic, accented Latin, and non-Latin entity metadata tags (#931)
- Explicit `encoding="utf-8"` on `Path.read_text()` calls across entity_registry, instructions_cli, split_mega_files, and onboarding tests — prevents Windows GBK (and other non-UTF-8) locales from corrupting UTF-8 files (#946, #776)
- `ko.json` `status_drawers` used `{drawers}` instead of `{count}`, showing the raw template string instead of the number (#758)
- Move `test_i18n.py` from inside the installed package into `tests/` so pytest actually collects it; remove the `sys.path.insert` hack (#758)
- `Dialect.from_config()` defaulted to `current_lang()` (module-global) when config had no `lang` key — replaced with explicit `"en"` fallback for determinism (#758)

**Other**

- Guard `KnowledgeGraph.close()` and `query_relationship`/`timeline`/`stats` methods with the instance lock to prevent concurrent-access corruption (#887, #884)
- Replace invalid `{"decision": "allow"}` with `{}` in hook responses — the string wasn't a valid decision value and triggered schema warnings (#885)
- `entity_registry.research()` defaults to local-only — previously made outbound Wikipedia HTTPS requests without explicit user opt-in; callers now must pass `allow_network=True` (#811)
- Precompact hook no longer blocks compaction when it fails or takes too long (#856, #858, #863)
- Redirect stdout to stderr during MCP server import so library logging can't corrupt the JSON-RPC channel (#225, #864)
- `mempalace init` auto-adds per-project files to `.gitignore` in git repositories so users don't accidentally commit `mempalace.yaml` / `entities.json` (#185, #866)
- Searcher guards against empty ChromaDB query results that previously raised on edge-case corpora (#195, #865)
- Return empty status instead of an error on a cold-start palace with no drawers yet (#830, #831)
- Restrict file permissions on sensitive palace data (#814)
- Slack transcript importer writes a provenance header and preserves speaker IDs (#815)
- Allow `mempalace mine` to run in directories without a local `mempalace.yaml` and surface the missing-yaml warning on stderr (#604)
- Security hook injection fix (#812)
- Save hook auto-mines transcripts even when `MEMPAL_DIR` is unset (#840)
- Pin the Pages custom domain via a shipped `CNAME` in the deploy artifact (#877)
- Version drift safeguard — sync pyproject + `version.py` + README badge in one place (#876)
- Deploy docs workflow now runs on `develop` only, preventing accidental main-branch deploys (#845)

### Improvements

- Regex compilation optimization for entity extraction — pre-compile per-entity pattern sets once and cache by `(name, languages)` tuple, so multi-language callers don't thrash the cache (#880)
- Knowledge-graph value sanitization now preserves natural punctuation (commas, colons, parentheses) that commonly appears in KG subject/object values (#873)

### Documentation

- Clarify that `mempalace init` requires a `<dir>` argument in CLI help text (#210, #862)
- Domain name and specific impostor sites called out in the scam-alert section (#869)
- Tightened `SECURITY.md` with a real version-support policy and the GHPVR-only reporting channel (#810)
- Fixed stale `pyproject.toml` URLs (#853)
- v4 planning prep (#852)

### Internal

- `palace_graph` tunnel helper test coverage (#908)

---

## [3.3.0] — 2026-04-13

### New Features
- Closet layer — a compact searchable index of pointers to verbatim drawers, enabling fast topical lookup without reading all content (#788)
- BM25 hybrid search — closets boost ranking, drawers remain the source of truth (#795, #829)
- Entity metadata on every drawer for filterable search (#829)
- Diary ingest — day-based rooms for conversation transcripts (#829)
- Cross-wing tunnels — explicit links between rooms in different wings for multi-project agents (#829)
- Drawer-grep — returns the best-matching chunk plus adjacent context drawers (#829)
- Offline fact checker against the entity registry and knowledge graph (#829)
- LLM-based closet regeneration — optional, bring-your-own endpoint, no mandatory API key (#793)
- Hall detection — routes drawer content to `emotions` / `technical` / `family` / `memory` / `identity` / `consciousness` / `creative` halls, enabling hall-based graph connectivity within wings (#835)

### Bug Fixes
- Repair `max_seq_id` corruption caused by `_fix_blob_seq_ids` misinterpreting chromadb 1.5.x's sysdb-10 BLOB format (`b'\x11\x11'` + ASCII digits) as legacy 0.6.x big-endian BLOBs. The shim now skips the `max_seq_id` table entirely and guards the `embeddings` branch with a prefix check. New subcommand `mempalace repair --mode max-seq-id [--from-sidecar <path>]` restores affected palaces. Fixes silent drawer-write drops that began after chromadb 1.5.x upgrades on palaces that still had BLOB-typed `max_seq_id` rows at migration time.
- Set `hnsw:space=cosine` metadata on all collection creation sites — fixes broken similarity scoring under ChromaDB's default L2 distance (#807, #218)
- File-level locking prevents duplicate drawers when agents mine the same file concurrently (#784, #826)
- Hybrid closet+drawer retrieval — closets boost ranking, never gate results (#795)
- Stop hooks from making agents write in chat — saves tokens on every turn (#786)
- Strip system tags, hook output, and Claude UI chrome from drawers before filing (#785)
- Verbatim-safe `strip_noise` scoped to Claude Code JSONL only (#785)
- Prevent diary entry ID collisions via microsecond timestamp and full content hash (#819)
- Auto-rebuild stale drawers via `NORMALIZE_VERSION` schema gate
- Enforce atomic topics in closets and extract richer pointers
- Sync `version.py` to match `pyproject.toml` (#820)
- Remove unused `main` import from `mempalace/__init__.py` (#827)
- README audit — fix 7 stale claims (tool count, version badge, wake-up token cost, `dialect.py` lossless disclaimer, `pyproject.toml` version) with 42 regression-guard tests (#835)

### Improvements
- Optimize entity detection with regex caching and pre-compilation (#828)
- Extract locked filing block into helper to keep `mine_convos` under C901 complexity

### Documentation
- Add `docs/CLOSETS.md` — closet layer overview
- Fix stale `milla-jovovich/*` org URLs in website and plugin manifests (#787)
- Fix remaining stale org URLs in contributor docs (#808)
- Rewrite `README.md` and `mempalaceofficial.com` benchmark pages to remove category-error cross-system comparisons (R@5 retrieval recall had been listed next to competitor QA accuracy under one column), remove the retracted "+34% palace boost" claim from the surfaces where it had remained, replace the `100%` Haiku-rerank headline with the honest held-out `98.4%` R@5, drop the LoCoMo `100%` top-50 row (retrieval-bypass artefact), and fix the broken `aya-thekeeper/mempal` reproduction URL (#875)
- Add `docs/HISTORY.md` as the canonical home for corrections, retractions, and public notices; move the 2026-04-07 "Note from Milla & Ben" and the 2026-04-11 impostor-domain notice out of `README.md`
- Add v3.3.0 reproduction result JSONLs and the deterministic `seed=42` 50/450 LongMemEval split under `benchmarks/` — every BENCHMARKS.md claim reproduces exactly

### Internal
- Add test coverage for `mine_lock`, closets, entity metadata, BM25, and diary
- Verify `mine_lock` via disjoint critical-section intervals
- Serialize `mine_lock` concurrency test with multiprocessing
- Make diary state path assertion platform-neutral
- Add `TestTunnels` coverage for cross-wing tunnel operations
- Ruff format with CI-pinned version (0.4.x); format `mempalace/palace.py`

---

## [3.2.0] — 2026-04-12

### Packaging
- Remove `chromadb<0.7` upper bound — unblocks installs against chromadb 1.x palaces (#690)
- Bump version to 3.2.0 across `pyproject.toml`, `mempalace/version.py`, README badge, and OpenClaw SKILL (#761)

### Security
- Harden palace deletion, WAL redaction, and MCP search input handling (#739)
- Consistent input validation, argument whitelisting, concurrency safety, and WAL fixes (#647)
- Remove hardcoded credential paths from benchmark runners (#177)
- Remove global SSL verification bypass in convomem_bench (#176)

### Bug Fixes
- Parse Claude.ai privacy export with `messages` key and sender field (#685, #677)
- Detect mtime changes in `_get_client` to prevent stale HNSW index (#757)
- Hash full content in `tool_add_drawer` drawer ID — stable re-mines (#716)
- Remove 10k drawer cap from status display (#707, #603)
- Correct typo in entity_detector interactive classification prompt (#755)
- Prevent convo_miner from re-processing 0-chunk files on every run (#732, #654)
- Remove silent 8-line AI response truncation in convo_miner (#708, #692)
- Store full AI response in convo_miner exchange chunking (#695)
- Fix `mine --dry-run` TypeError on files with room=None (#687, #586)
- Skip arg whitelist for handlers accepting `**kwargs` (#684, #572)
- Allow Unicode in `sanitize_name()` — Latvian, CJK, Cyrillic (#683, #637)
- Auto-repair BLOB seq_ids from chromadb 0.6→1.5 migration (#664)
- Remove no-op `ORT_DISABLE_COREML` env var (#653, #397)
- Disambiguate hook block reasons to name MemPalace explicitly (#666)
- Use epsilon comparison for mtime to prevent unnecessary re-mining (#610)
- Correct token count estimate in compress summary (#609)
- Implement MCP ping health checks (#600)
- Align `cmd_compress` dict keys with `compression_stats()` return values (#569)
- Skip unreachable reparse points in `detect_rooms_from_folders` on Windows (#558)
- Prevent HNSW index bloat from duplicate `add()` calls (#544, #525)
- Purge stale drawers before re-mine to avoid hnswlib segfault (#544)
- Mitigate system prompt contamination in search queries (#385, #333)
- Count Codex `user_message` turns in `_count_human_messages` (#373, #347)
- Paginate large collection reads and surface errors in MCP tools (#371, #339, #338)
- Expand `~` in split command directory argument (#361)
- Ignore `wait_for_previous` argument to support Gemini MCP clients (#322)
- Close KnowledgeGraph SQLite connections in test fixtures (#450)
- Remove duplicate cache variable declarations in mcp_server.py (#449)
- Add `--yes` flag to init instructions for non-interactive use (#682, #534)
- Add `mcp` command with setup guidance (#315)

### New Features
- i18n support — 8 languages (en, es, fr, de, ja, ko, zh-CN, zh-TW) (#718)
- New MCP tools: get/list/update drawer, hook settings, export (#667, #635)
- `mempalace migrate` — recover palaces from different ChromaDB versions (#502)
- Add OpenClaw/ClawHub skill (#491)
- Backend seam for pluggable storage backends (#413)

### Improvements
- Disable broken auto-bump workflow (#414)
- Improve agent readiness — AGENTS.md, dependabot, CODEOWNERS, labels (#497)

### Documentation
- Add CLAUDE.md and mission/principles to AGENTS.md (#720)
- Add VitePress documentation site (#439)
- Add warning about fake MemPalace websites (#598)
- Fix stale org URLs and PR branch target in contributor docs (#679)
- Fix misaligned architecture diagram (#734, #733)
- Add ROADMAP.md — v3.1.1 stability patch and v4.0.0-alpha plan

### Internal
- ruff format convo_miner.py (#741)
- ruff format all Python files (#675)
- CI: trigger tests on develop branch PRs and pushes (#674)
- CI: fix GitHub Pages publishing (#691)

---

## [3.1.0] — 2026-04-09

### Security
- Harden inputs, fix shell injection, optimize DB access (#387)
- Sanitize SESSION_ID in save hook to prevent path traversal (#141)
- Sanitize error responses and remove `sys.exit` from library code (#139)
- Shell injection fix in hooks, Claude Code mining, chromadb pin (#114)

### Bug Fixes
- MCP null args hang, repair infinite recursion, OOM on large files (#399)
- Release ChromaDB handles before rmtree on Windows (#392)
- Use `os.utime` in mtime test for Windows compatibility (#392)
- Negotiate MCP protocol version instead of hardcoding (#324)
- Use upsert and deterministic IDs to prevent data stagnation (#140)
- Make `drawer_id` deterministic for idempotent writes (#387)
- Honest AAAK stats — word-based token estimator, lossy labels (#147)
- Room detection checks keywords against folder paths (#145)
- Use actual detected room in mine summary stats (#165)
- Honour `--palace` flag in mcp_server (#264)
- Preserve default KG path when `--palace` not passed (#270)
- `--yes` flag skips all interactive prompts in init (#123)
- Repair command, split args, Claude export, room keywords (#119)
- Replace Unicode separator in convo_miner.py for Windows compatibility (#129)
- Coerce MCP integer arguments to native Python int (#84)
- Batch ChromaDB reads to avoid SQLite variable limit (#66)
- Respect nested .gitignore rules during mining (#78)
- Narrow bare `except Exception` to specific types where safe (#54)
- Mark MD5 as non-security in miner drawer ID generation (#53)
- Remove dead code and duplicate set items in entity_registry.py (#42)
- Silence ChromaDB telemetry warnings and CoreML segfault on Apple Silicon (#236)
- Unify package and MCP version reporting (#16)
- Fix broken AAAK Dialect link in README (#238)
- Update input prompt for entity confirmation (#83)
- Preserve CLI exit codes, log tracebacks, sanitize search errors (#139)
- Enable SQLite WAL mode and add consistent LIMIT to KG timeline (#136)
- Add limit=10000 safety cap to all unbounded ChromaDB `.get()` calls (#137)
- Re-mine modified files, idempotent `add_drawer`, cleanup ChromaDB handles (#140)
- Resolve formatting, regression logic, and pytest defaults (#270)
- Use `parse_known_args` to allow importing mcp_server during pytest (#270)

### New Features
- Package MemPalace as standard Claude and Codex plugins (#270)
- Add OpenAI Codex CLI JSONL normalizer (#61)
- Add Codex plugin support with hooks, commands, and documentation (#270)
- Add command documentation for help, init, mine, search, and status (#270)

### Improvements
- Cache ChromaDB `PersistentClient` instead of re-instantiating per call (#135)
- Tighten chromadb version range and add `py.typed` marker (#142)
- Consolidate split known-names config loading (#22)
- CI: add separate jobs for Windows and macOS testing
- CI: Upgrade GitHub Actions for Node 24 compatibility (#55)

### Documentation
- Add Gemini CLI setup guide and integration section (#106)
- Add beginner-friendly hooks tutorial (#103)
- Align MCP setup examples with shipped server (#21)
- Honest README update — own the mistakes, fix the claims

### Internal
- Expand test coverage from 20 to 92 tests, migrate to uv (#131)
- Add scale benchmark suite — 106 tests (#223)
- Increase test coverage from 30% to 85%, fix Windows encoding bugs (#281)
- Add WAL mode and entity timeline limit assertions
- Add coverage for `file_already_mined` mtime check

---

## [3.0.0] — 2026-04-06

Initial public release.

- Palace architecture with day-based rooms, drawers (verbatim), and closets (searchable index)
- AAAK compression dialect for memory folding
- Knowledge graph with entity detection and timeline queries
- MCP server for Claude, Codex, and Gemini integration
- CLI: `init`, `mine`, `search`, `status`, `compress`, `repair`, `split`
- Benchmark suite with recall and scale tests
- README with MCP flow, local model flow, and specialist agent documentation

---

[Unreleased]: https://github.com/MemPalace/mempalace/compare/v3.2.0...HEAD
[3.2.0]: https://github.com/MemPalace/mempalace/compare/v3.1.0...v3.2.0
[3.1.0]: https://github.com/MemPalace/mempalace/compare/v3.0.0...v3.1.0
[3.0.0]: https://github.com/MemPalace/mempalace/releases/tag/v3.0.0
