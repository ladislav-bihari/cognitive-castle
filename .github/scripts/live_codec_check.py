import json,os,sys,subprocess,re,stat,platform
from pathlib import Path
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-live-codec-results'
SOURCE='845797226f7a87351bbbd9d3f1648345bb648cd8'
MODE='green'
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true' and sys.platform=='linux' and platform.python_version()=='3.12.11'
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/live_codec_check.py' for p in subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines())
admit()
if sys.argv[1]=='prepare':
 assert not RESULTS.exists();RESULTS.mkdir(mode=0o700)
 assert not (Path(os.environ['RUNNER_TEMP'])/'castle-live-regression-pytest').resolve().is_relative_to(ROOT)
 assert not (Path(os.environ['RUNNER_TEMP'])/'castle-live-regression-pytest').exists()
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,mode=MODE,seconds=20,rss_mib=256,scope='Pure direct-live, registered layout, SQLite/native export codec and CI isolation contracts; no database, models, palace, native qualification or activation'),indent=2)+'\n')
elif sys.argv[1]=='run':
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout','20','--rss-mib','256','tests/test_live_publication_codec.py','tests/test_live_registration.py','tests/test_migration_codec.py','tests/test_lance_layout_codec.py','tests/test_native_export_codec.py','tests/test_sqlite_export_codec.py','tests/test_ci_test_partition.py','--noconftest','-p','no:cacheprovider','--basetemp',str(Path(os.environ['RUNNER_TEMP'])/'castle-live-regression-pytest'),'--junitxml',str(RESULTS/'result.xml'),'-x','-q'])
elif sys.argv[1]=='verify':
 root=ET.parse(RESULTS/'result.xml').getroot();cases=root.findall('.//testcase');fail=root.findall('.//failure')
 metrics=re.findall(r'\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed',(RESULTS/'output.log').read_text())
 assert len(metrics)==1;peak,elapsed=map(float,metrics[0]);assert peak<=256 and elapsed<=20
 assert not root.findall('.//error')
 skips={(t.get('classname'),t.get('name')):t.find('skipped').get('message') for t in cases if t.find('skipped') is not None}
 assert skips=={('tests.test_native_export_codec', 'test_native_session_refusal_uses_history_profile_and_original_cursor[worker_memory_limit]'): 'requires supported macOS supervisor admission', ('tests.test_native_export_codec', 'test_native_session_refusal_uses_history_profile_and_original_cursor[total_memory_limit]'): 'requires supported macOS supervisor admission', ('tests.test_native_export_codec', 'test_native_session_refusal_uses_history_profile_and_original_cursor[worker_deadline]'): 'requires supported macOS supervisor admission', ('tests.test_native_export_codec', 'test_native_session_refusal_uses_history_profile_and_original_cursor[background_preempted]'): 'requires supported macOS supervisor admission', ('tests.test_native_export_codec', 'test_native_session_refusal_uses_history_profile_and_original_cursor[system_memory_pressure]'): 'requires supported macOS supervisor admission', ('tests.test_sqlite_export_codec', 'test_supervisor_refusal_retains_source_cursor_and_fixed_profile'): 'requires supported macOS supervisor admission'}
 if MODE=='red':
  assert int(sys.argv[2])==1 and len(cases)==len(fail)==1
  assert cases[0].get('name')=='test_direct_publication_preserves_original_records_and_expected_receipt'
  assert 'live_publication_codec' in fail[0].get('message','') and 'ImportError' in fail[0].get('message','')
 else:assert int(sys.argv[2])==0 and len(cases)==110 and not fail
 (RESULTS/'result.json').write_text(json.dumps(dict(status='EXPECTED_MISSING_CODEC' if MODE=='red' else 'PASS',cases=len(cases),passed=len(cases)-len(skips)-len(fail),skipped=len(skips),failures=len(fail),rss_mib=peak,seconds=elapsed,source=SOURCE,scope='Preparation/framing only; not durable publication or restored capture'),indent=2)+'\n')
else:raise SystemExit('closed operation')
