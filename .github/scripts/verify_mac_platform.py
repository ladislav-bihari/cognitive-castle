import json,os,sys,platform,subprocess,re,tarfile,stat,hashlib
from pathlib import Path
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-mac-platform-results'
SOURCE='66fdb6fcbde898fceb915517e8ea685cefeb9e5b'
SCOPES={'platform': {'tests': ['tests/test_native_export_codec.py::test_native_session_refusal_uses_history_profile_and_original_cursor', 'tests/test_semantic_indexer.py::test_exact_receipt_survives_late_coordination_refusal_with_three_second_total', 'tests/test_semantic_indexer.py::test_fixed_stage_caps_share_absolute_deadline', 'tests/test_semantic_indexer.py::test_session_releases_admission_after_preemption_and_cannot_launch_more_work', 'tests/test_semantic_runtime.py::test_background_yields_to_interactive_and_host_has_no_native_imports', 'tests/test_semantic_runtime.py::test_deadline_consumes_validation_and_both_stages', 'tests/test_semantic_runtime.py::test_infer_rejects_unissued_handoff_and_malformed_worker_vector', 'tests/test_semantic_runtime.py::test_migration_source_queue_exact_share_one_deadline_and_fixed_caps', 'tests/test_semantic_runtime.py::test_real_supervisor_preflight_cannot_extend_shared_session_deadline', 'tests/test_semantic_runtime.py::test_shared_deadline_expiry_and_admission_release', 'tests/test_semantic_runtime.py::test_source_response_validation_cannot_escape_absolute_deadline', 'tests/test_semantic_runtime.py::test_success_arriving_after_absolute_deadline_is_unavailable', 'tests/test_semantic_runtime.py::test_tokenizer_response_must_bind_requested_source_set', 'tests/test_sqlite_export_codec.py::test_supervisor_refusal_retains_source_cursor_and_fixed_profile'], 'cases': 18, 'seconds': 20}, 'sqlite': {'tests': ['tests/test_sqlite_source_resume.py::test_native_corrupt_skipped_prefix_refuses_without_advancing'], 'cases': 5, 'seconds': 20}, 'lifecycle': {'tests': ['tests/test_cancellable_pressure_probe.py::test_native_slow_normal_and_waiter_leave_no_pressure_child', 'tests/test_memory_capture_priority.py::test_native_background_capture_yields_only_to_foreground_and_cleans_up', 'tests/test_memory_capture_priority.py::test_native_background_capture_preserves_queue_and_spool_then_resumes', 'tests/test_memory_preemption.py::test_live_waiter_interrupts_delayed_pressure_probe'], 'cases': 5, 'seconds': 20}}
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true'
 assert sys.platform=='darwin' and platform.machine()=='arm64' and platform.python_version()=='3.12.10'
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 changes=subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines()
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/verify_mac_platform.py' for p in changes)
admit()
if sys.argv[1]=='prepare':
 assert not RESULTS.exists();RESULTS.mkdir(mode=0o700)
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,scopes=SCOPES,rss_mib=256,sqlite_worker_starts=10,limits='Original production limits/deadlines unchanged. Fixed disposable fixture data, no Lance/models/installed palace/activation. Lifecycle assertions preserve foreground priority and child reaping.'),indent=2)+'\n')
 for name in SCOPES:(RESULTS/name).mkdir(mode=0o700)
elif sys.argv[1]=='run':
 name=sys.argv[2];scope=SCOPES[name];base=RESULTS/name
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout',str(scope['seconds']),'--rss-mib','256',*scope['tests'],'--noconftest','-p','no:cacheprovider','--basetemp',str(base/'pytest'),'--junitxml',str(base/'result.xml'),'-x','-q','-s'])
elif sys.argv[1]=='verify':
 name=sys.argv[2];scope=SCOPES[name];base=RESULTS/name
 root=ET.parse(base/'result.xml').getroot();cases=root.findall('.//testcase')
 errors=[n for kind in ['failure','error','skipped'] for n in root.findall('.//'+kind)]
 metrics=re.findall(r'\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed',(base/'output.log').read_text())
 assert len(metrics)==1;peak,elapsed=map(float,metrics[0])
 passed=int(sys.argv[3])==0 and len(cases)==scope['cases'] and not errors and peak<=256 and elapsed<=scope['seconds']
 (base/'result.json').write_text(json.dumps(dict(status='PASS' if passed else 'FAIL',cases=len(cases),failures=len(errors),rss_mib=peak,seconds=elapsed,source=SOURCE),indent=2)+'\n')
 assert passed
elif sys.argv[1]=='archive':
 stack=[RESULTS];entries=[];size=allocated=0
 while stack:
  p=stack.pop();st=p.lstat();assert stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode)
  entries.append(p);size+=st.st_size;allocated+=st.st_blocks*512
  assert len(entries)<=8192 and max(size,allocated)<=64*1024**2
  if stat.S_ISDIR(st.st_mode):stack.extend(p.iterdir())
 with tarfile.open(ROOT/'mac-platform-evidence.tar','w',dereference=False) as tar:
  for p in sorted(entries):tar.add(p,arcname=p.relative_to(ROOT),recursive=False)
 print(json.dumps(dict(bytes=(ROOT/'mac-platform-evidence.tar').stat().st_size,sha256=hashlib.sha256((ROOT/'mac-platform-evidence.tar').read_bytes()).hexdigest())))
else:raise SystemExit('closed operation')
