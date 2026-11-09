# Develop integration — incomplete qualification

The integration branch composes develop `36e3247` with the unfinished semantic
preservation branch. It is not merged into B3 or installed. The installed checkout
remains clean at `0011ad1`. Original data, service configuration, production worker
limits, deadlines, models and golden results are unchanged.

## Runtime evidence

Source `146d5af9d36545a5cc45a56d694f78a61ad4db95` preserves on-time pressure levels
1/2, refuses critical/unknown/late samples, and retains absolute deadlines,
cancellable probes, foreground priority and child cleanup. All named tests from
both merge sides survive the three conflicted test files.

| Check | Result | Sampled aggregate RSS | Time |
|---|---|---|---|
| Local pure pressure/deadline contracts | 48 passed | 40.0 MiB | 3.1 s |
| Capture journal/ingest/scope regression | 94 passed | 57.8 MiB | 4.0 s |
| Semantic routing/indexer/vector contracts | 50 passed | 45.4 MiB | 0.8 s |
| Publication/coordination contracts | 23 passed | 64.5 MiB | 1.2 s |

The remote results and XML were independently inspected from
[run 42652691539](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/42652691539).
Each phase retained the existing 20-second/256-MiB test harness ceiling.

The five actual process-lifecycle cases in
[run 42651970998](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/42651970998)
remain unqualified. Attempt 1 acquired no macOS runner and was cancelled by GitHub
after 15 minutes with an explicit hosted-runner capacity annotation. Attempt 2
reuses identical code and limits; its result must be inspected before committing
the integration. No product test failed in attempt 1 because none started.

## CI corrections

Develop's dev dependency had moved to Ruff 0.17.3 while its required-version and
workflow install remained 0.15.12. All four pins now agree. Ruff's newly enabled
Markdown formatting is excluded, preserving existing documentation; the complete
remaining scope passes lint and format (384 files).

An isolated diagnostic reproduced a real test-context defect: the existing
engine-free coordinator assertion passes fresh, then fails after global
`tests/conftest.py` imports SQLite. Coverage 7.13.5 also imports SQLite. This is
separate evidence, not a diagnosis of all 266 reported failures.

`scripts/run_ci_tests.py` retains the ordinary suite with its fixtures and runs
bounded memory files in separate processes without the global conftest. Tests
that inspect `sys.modules` run unchanged in a separate, uninstrumented phase;
their complementary cases retain coverage. Inherited coverage startup injection
is removed. The combined six-job coverage matrix and 85% gate remain intact.
Empty/unexpected test phases cannot be hidden by unrelated successful files.

Seven harness tests passed in
[run 42654557166](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/42654557166)
at 75.8 MiB/2.3 s, including real subprocess and coverage experiments. The final
snapshot additionally preserves every file from the previous CI isolation list.
[Run 42655065108](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/42655065108)
also passed 7/7 at 75.5 MiB/1.9 s; its XML and result were independently inspected.
The partition inventory is 194 files: 97 legacy and 97 bounded, with no overlap,
omission or required global legacy fixture in the bounded group. This is harness qualification, not a
passing full platform suite or measured 85% coverage.

[Run 42655563861](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/42655563861)
now qualifies all 97 bounded files on Linux using source
`010e130e2b88a3b132991372d50f3438b7026e50` and ordinary project dev dependencies.
Its dependency installation passed. All 97 bounded files completed, producing
1,758 cases: 1,423 passed, 297 skipped and 38 failed. This is a retained RED run,
not full-platform or production semantic qualification.

The 38 failures were classified from the downloaded XML: 23 cases require
supported macOS supervisor admission, 11 import-purity cases were missed by the
initial coverage selector (helper calls or dynamic `sys` imports), two unit cases
imported psutil after simulating a different platform, one private-report fixture
was placed inside the repository, and one CLI check inherited deliberate MCP
import-time stdout protection. Corrections retain original assertions: add
macOS-only marks to 15 parametrized functions, preload the actual host psutil
backend before unit simulation, initialize the existing MCP stdout restoration,
extend import-guard selection through helper references, and keep private test
scratch outside the repository. Function-body AST comparison confirms all 15
platform-marked tests retain their original bodies. Corrected runs remain pending.

The macOS lifecycle attempt 2 also acquired no hosted runner and ended with the
same capacity annotation. Neither attempt executed a test. No immediate third
retry or platform/limit substitution was made; the lifecycle gate remains open.

## Remaining gates

- Inspect full bounded Linux and process-lifecycle results, then commit integration.
- Obtain and triage the actual 266-failure report; no full-branch green claim yet.
- Execute the repaired full platform test/coverage split before B3 integration.
- Complete foreground registered Lance publication, ordinary retrieval/context
  integration, configured semantic quality, full-corpus reconciliation/capacity,
  independent review, operator cutover and installed Codex/Claude verification.
- Do not enable Claude GitHub workflows or upgrade pylance as part of this work.

## Current installed storage inventory (metadata only)

On November 9, a read-only filesystem inventory measured 10.20 GiB allocated
under the palace: 4.01 GiB LanceDB, 4.75 GiB historical sidecar stores, 1.33 GiB
live SQLite and approximately 0.10 GiB SUE data. The live store is included in
the parent total, not an additional reservation. Available filesystem space was
13.27 GiB at the recorded observation; concurrent capture remains active.

The existing historical job metadata reports `state=indexing`,
`expected_rows=519641`, `indexed_rows=483895`, eight stores and zero recorded
gaps. Thus 35,746 expected rows are not evidenced as indexed by that metadata.
Zero recorded gaps does not imply complete coverage. No engine/database/body was
opened and these historical metadata claims have not been reconciled row by row.

This is neither an atomic source snapshot nor a capacity permit. Full migration
must account for protected originals/rollback roots, expanded destination rows,
semantic indexes, journals, temporary outputs and free headroom. Original files
were neither modified nor removed. The bounded metadata observations are retained
in this plan's `capacity-footprint-20261109.json` and
`history-job-metadata-20261109.json` evidence files.
