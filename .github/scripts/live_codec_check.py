import json,os,sys,subprocess,re,stat,platform
from pathlib import Path
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-live-codec-results'
SOURCE='63dd397a47a0ad770c7164afda306ea153aba4da'
MODE='green'
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true' and sys.platform=='linux' and platform.python_version()=='3.12.11'
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/live_codec_check.py' for p in subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines())
admit()
if sys.argv[1]=='prepare':
 assert not RESULTS.exists();RESULTS.mkdir(mode=0o700)
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,mode=MODE,seconds=20,rss_mib=256,scope='Existing capture plus split body-free journal recovery; actual SQLite only; no user data, models, palace, native qualification or activation'),indent=2)+'\n')
elif sys.argv[1]=='run':
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout','20','--rss-mib','256','tests/test_layout_live_control.py::test_serialized_live_owners_share_workspace_but_keep_retained_capacity','tests/test_partitioned_exact_store.py::test_shared_workspace_still_refuses_actual_retained_spill_overflow','tests/test_worker_plugin_policy.py','tests/test_capture_control_status.py','tests/test_selected_status.py','tests/test_semantic_authority_pin.py','tests/test_layout_semantic_routing.py','tests/test_capture_handoff.py','tests/test_memory_ingest.py','tests/test_capture_publication_journal.py','tests/test_capture_publication_ids.py','tests/test_capture_recovery_codec.py','tests/test_capture_recovery_control.py','tests/test_capture_control_store.py','tests/test_fast_memory.py','tests/test_capture_control_counts.py','tests/test_memory_scope.py','tests/test_capture_control_worker.py','tests/test_capture_control_runtime.py','tests/test_capture_live_coordinator.py','tests/test_semantic_unscoped.py','tests/test_memory_selection.py','tests/test_memory_selected_runtime.py','tests/test_memory_selected_dispatch.py','tests/test_layout_read_binding.py','tests/test_lance_live_publication.py','tests/test_capture_control_discovery.py','tests/test_capture_pending_slices.py','tests/test_memory_coordinator.py::test_busy_hooks_persist_then_resume_with_workspace','tests/test_memory_coordinator.py::test_worker_refusal_keeps_metadata_for_next_tick','tests/test_memory_coordinator.py::test_fresh_priority_survives_worker_refusal_after_queue_ack','tests/test_memory_coordinator.py::test_fresh_slices_resume_original_prefix_without_skipping_and_remain_fair','tests/test_memory_coordinator.py::test_hook_source_behind_cursor_is_captured_before_old_backlog','--noconftest','-p','no:cacheprovider','--basetemp',str(Path(os.environ['RUNNER_TEMP'])/'castle-live-control-pytest'),'--junitxml',str(RESULTS/'result.xml'),'-x','-q'])
elif sys.argv[1]=='verify':
 import tarfile
 base=Path(os.environ['RUNNER_TEMP'])/'castle-live-control-pytest'
 count=total=0;omitted_links=[]
 with tarfile.open(RESULTS/'fixtures.tar.gz','w:gz',dereference=False) as archive:
  for parent,dirs,files in os.walk(base,followlinks=False):
   for name in list(dirs):
    path=Path(parent)/name
    if path.is_symlink():
     omitted_links.append(str(path.relative_to(base)));dirs.remove(name)
   assert len(omitted_links)<=8192
   for path in [Path(parent),*(Path(parent)/name for name in files)]:
    info=path.lstat()
    if stat.S_ISLNK(info.st_mode):
     omitted_links.append(str(path.relative_to(base)));assert len(omitted_links)<=8192;continue
    assert stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
    count+=1;total+=info.st_size if stat.S_ISREG(info.st_mode) else 0
    assert count<=8192 and total<=64*1024**2
    archive.add(path,arcname=str(path.relative_to(base.parent)),recursive=False)
 (RESULTS/'fixture-manifest.json').write_text(json.dumps(dict(entries=count,bytes=total,omitted_symlink_aliases=omitted_links,scope='Synthetic control SQLite only; no native storage/user palace'))+chr(10))
 root=ET.parse(RESULTS/'result.xml').getroot();cases=root.findall('.//testcase');fail=root.findall('.//failure')
 metrics=re.findall(r'\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed',(RESULTS/'output.log').read_text())
 assert len(metrics)==1;peak,elapsed=map(float,metrics[0]);assert peak<=256 and elapsed<=20
 assert not root.findall('.//error') and not root.findall('.//skipped')
 if MODE=='red':
  assert int(sys.argv[2])==1 and len(cases)==1 and len(fail)==1
  assert cases[0].get('name')=='test_serialized_live_owners_share_workspace_but_keep_retained_capacity[native-policy]'
  assert 'AssertionError' in fail[0].get('message','') and 'conflict' in fail[0].get('message','')
 else:
  assert int(sys.argv[2])==0 and len(cases)==285 and not fail
  capacity=[c.get('name') for c in cases if c.get('name','').startswith('test_serialized_live_owners_share_workspace_but_keep_retained_capacity[')]
  assert len(capacity)==2 and any('[False-' in n for n in capacity) and any('[True-' in n for n in capacity)
  assert any(c.get('name')=='test_shared_workspace_still_refuses_actual_retained_spill_overflow' for c in cases)
 (RESULTS/'result.json').write_text(json.dumps(dict(status='PASS',cases=len(cases),failures=len(fail),rss_mib=peak,seconds=elapsed,source=SOURCE,scope='Existing capture and metadata-only handoff regression; not registered capture activation or native proof'),indent=2)+'\n')
else:raise SystemExit('closed operation')
