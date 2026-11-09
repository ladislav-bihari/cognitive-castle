# Develop Integration Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans inline. Keep the full unified-memory objective active.

**Goal:** Reconcile the unfinished Lance branch with current develop without regressing running capture, then verify the review findings before resuming foreground Lance publication.

**Architecture:** Preserve develop's admitted normal/warning pressure levels and existing live-store budget. Compose them with the feature branch's absolute deadline, cancellation, resource sampling and child cleanup. Retain tests for both behaviors. Integration happens on its own branch in the existing isolated worktree; the installed checkout is unchanged.

**Tech Stack:** Python, existing guarded pytest runner; project-pinned Ruff0.17.3. Pylance remains4.0.1 and PyArrow24.0.0.

**Spec:** docs/superpowers/specs/2026-10-24-semantic-preservation-design.md; current develop36e3247 and user-supplied merge audit.

## Global Constraints

- No merge into B3, installation or service/hook changes until the complete goal and review gates pass.
- Preserve128/192MiB SQLite workers and192/256MiB approved Lance workers, existing deadlines and single flight.
- Develop's4GiB disk budget and pressure1/2 policy are existing upstream behavior, not new increases introduced here.
- No broad local native/model/palace test run. Pure scoped tests use20s/256MiB admission; native lifecycle checks run in isolated hosted macOS.
- Preserve all original/archive/rollback data and retained failed fixtures. Do not change models/goldens/caps to force passes.

## Review Focus

- Warning pressure still permits bounded capture; critical/unknown pressure refuses.
- A late normal or warning sample cannot authorize work after the original deadline.
- Waiting foreground work preempts background pressure probes and workers; all owned children are reaped.
- Queue/spool entries survive preemption and resume without duplicate writes.
- Sparse green qualification slices are not evidence of a green whole branch or completed memory unification.

### Task1: Reconcile runtime pressure/deadline behavior

Files: memory_runtime.py, brain_reader_guardian.py; three conflicted pressure/priority test files; new test_pressure_policy_integration.py.

- [ ] Freeze both merge sides and enumerate all unique tests.
- [ ] Observe RED on original feature code for current develop warning admission and strict late-sample rejection, with AST-only tests.
- [ ] Resolve policy to1/2 admitted on time,4/unknown refused; retain absolute deadlines and foreground priority. Preserve upstream native tests and feature-specific deadline cases.
- [ ] Run relevant pure pressure/preemption/resource tests, then fixed native lifecycle checks remotely. Record actual failures without weakening safeguards.

### Task2: Repair formatting and investigate branch failures

Files: tests/lance_owner_stream_fixture.py, tests/test_lance_owner_stream.py; validation report and this ledger.

- [ ] Format only reported files with project-pinned Ruff; check the full source/test/script format and lint scope.
- [ ] Obtain or locate the266-failure report, classify actual failures by code path and environment, and reproduce a representative failure within approved bounds. Never rerun an uncontrolled full native suite locally.
- [ ] Verify integrated capture journal/semantic contracts and affected upstream tests in isolated processes.
- [ ] Commit the integration only when conflicts are resolved and relevant checks pass. Publish exact outstanding gates; resume the registered foreground writer with the full objective unchanged.
