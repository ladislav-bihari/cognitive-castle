# Memory storage verification, 29 October 2026

The packed migration and published-snapshot paths have real Lance evidence.
The complete unified-memory delivery is **not finished or activated**.
This report does not establish full-corpus capacity, semantic completeness,
ordinary capture/recall integration, or installed Codex/Claude behavior.

## Corrections

- Published reads open the exact registered BEFORE version while an append or
  maintenance operation is pending. Namespace, schema, geometry, index and
  publication checks remain active. A missing snapshot refuses without fallback.
  Internal mutation and proof operations continue to inspect the current version.
- The dedicated append guard previously accepted only its first qualified owner.
  A real packed append therefore returned `corrupt` even though preparation and
  the earlier unit tests passed. The base guard retains its single-owner rules;
  the registered packed override validates the complete ordered candidate set,
  original row hashes and bound batch receipt. Resource checks are unchanged.
- Semantic queries now distinguish text hashes from full-source exposure hashes.
  Exposure grants apply before nearest-neighbor limits and are checked again
  after exact hydration. The original internal text filter remains separate.
  Tests cover identical text from different origins and a closer unauthorized
  candidate. Native vector/exposure prefiltering has also been verified below.
  Host policy-epoch and ordinary-route integration remain outstanding.

## Evidence

| Scope | Evidence | Result |
| --- | --- | --- |
| Snapshot, packed authority, source and original contracts | Runs 40360517545, 40939247023, 40939245885 | 592 tests passed without native storage |
| Actual inherited append guard and regressions | RED 40940016946; GREEN 40940140812, 40940228386, 40940228923 | 628 tests passed; includes 28 original dedicated-owner/reservation cases |
| Guarded source proof-reply budget | [Run 40939650921](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/40939650921) | 3 worker starts, 2.2 s, sampled aggregate peak 100.2 MiB |
| Two-owner packed publication and lost ACK | [Run 40940316653](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/40940316653) | 21 worker starts; 12.2 s overall; sampled aggregate peak 186.2 MiB |
| Published old snapshot during the next same-home append | Same run | 30 worker starts; 20.5 s overall; sampled aggregate peak 176.5 MiB |
| SQL failure after the second owner span update, then cold recovery | [Run 40941861515](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/40941861515) | 26 worker starts; all 14 metadata tables unchanged after rollback; 21.0 s overall; sampled aggregate peak 186.1 MiB |
| Exposure grants and semantic/vector control flow | Guarded local Mac XML in the execution ledger | 36 tests, 0.5 s, sampled aggregate peak 39.5 MiB; 23 additional original contracts, 2.2 s, 60.7 MiB |
| Actual native exposure prefilter and read-only queries | [Run 40943086351](https://github.com/ladislav-bihari/cognitive-castle/actions/runs/40943086351) | 8 worker starts; 7.7 s overall; sampled aggregate peak 184.0 MiB |

The native packed steps took 2.4696 and 2.6671 seconds. The same-home snapshot
steps took 1.5979, 3.3562, 2.5261 and 3.7995 seconds. Every step retained its
absolute five-second session deadline. Exact workers retained 192 MiB task /
256 MiB aggregate diagnostic limits, two-second reads and three-second writes.
Queue workers retained 128/192 MiB and the 16 MiB SQLite heap limit.
RSS limits use sampled tripwires; transient peaks are possible.

Both native cases verify original text, provenance and LIVE receipts. Owners
remain hidden after Fresh until cold Existing replay, grouped proof and ACK.
Lost-ACK replay does not duplicate counts. Already published source, receipt
and owner reads succeed while the next same-home append is still unpublished.
The original source tree and qualification artifact remain byte-for-byte intact.

The first packed run failed before ACK at the inherited single-owner guard;
its whole fixture archive is retained. The corrected run uses new disposable
fixtures, source snapshot `d3361d43784304686db34dfc97b2974ecc2ff624` and CI snapshot
`854b64b1e5f36d87b0fa753169729df067784435`.
Its complete retained TAR is 1,566,720 bytes, SHA256
`e6b2e93c45aac6149e3eb12a3764a473265bf04c25fa60149cfef7b439ee2c60`.
Only JSON/log members were inspected; failed databases were not reopened.

The first exposure regression selection failed on Linux because an existing,
unmarked Mac-specific session test correctly refused platform supervision.
The same 36 selectors subsequently passed on the actual Mac under the guarded
test runner. No production guard was weakened to make that suite pass.

## Production and completion gates

The successful native runs used the 192/256 MiB exact-memory profile. Observed
exact worker peaks were approximately 135–138 MiB. On 29 October 2026 the user
explicitly approved that profile for production: “povolit produkčně 192/256 MiB”.
The exact guardian/session already enforce these values. SQLite coordination
retains 128/192 MiB, and deadlines/admission/cleanup remain unchanged. This
approval resolves the resource-policy gate; integration and verified activation
are still required before changing the installed ordinary routes.

The actual-SQL fault case used genuine Lance replay/group proofs. After the
second owner span update, the injected failure rolled back all 14 coordination
tables. Both owners remained hidden and the original frontier/counts stayed
unchanged. A subsequent cold replay/group proof and ACK published both owners.
Its three steps took 3.4058, 4.0013 and 4.1028 seconds. The complete 808,960-byte
TAR is retained with SHA256
`ce2c1df6382fb79837546616ae53c70fc095a959d837a49097916f366195e291`.

The native vector-grant case passed with genuine canonical records containing
identical text from two different origins. A closer unauthorized vector did not
displace the authorized result under limit 1. Empty grants, a text hash supplied
as an exposure grant, unrestricted queries and the intersection of both filters
had the expected results. Querying preserved every file fingerprint. This case
uses fixed synthetic vectors, loads no embedding model and proves no relevance
quality. Its 122,880-byte retained TAR has SHA256
`7fad584cb39b8dbbab919366d9a77b9185d0e0138f8157875281149aa08b016b`.

Remaining delivery gates include ordinary Lance-backed remember/capture and
recall/context, lexical/vector/KG fusion and configured reranking, source and
semantic reconciliation for the complete historical boundary, retained disk
capacity, installed host behavior, independent branch review and B3 integration.
The active GitHub account has read access only to B3, but the separately stored
Testimonial account was verified to have admin/push access. This check used an
ephemeral credential and did not change the active account. Access therefore
does not require user intervention. Feature commits are pushed to the personal
fork; they have not been merged into B3 or installed.
