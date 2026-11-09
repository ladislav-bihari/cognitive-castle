import os,json,sys,subprocess,re,tarfile,stat,hashlib,platform
from pathlib import Path
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-pressure-results'
SOURCE='146d5af9d36545a5cc45a56d694f78a61ad4db95'
TESTS=['tests/test_cancellable_pressure_probe.py::test_native_slow_normal_and_waiter_leave_no_pressure_child','tests/test_memory_capture_priority.py::test_native_background_capture_yields_only_to_foreground_and_cleans_up','tests/test_memory_capture_priority.py::test_native_background_capture_preserves_queue_and_spool_then_resumes','tests/test_memory_preemption.py::test_live_waiter_interrupts_delayed_pressure_probe']
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true'
 assert sys.platform=='darwin' and platform.machine()=='arm64' and platform.python_version()=='3.12.10'
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 changes=subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines()
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/verify_pressure.py' for p in changes)
if sys.argv[1]=='prepare':
 admit();assert not RESULTS.exists();RESULTS.mkdir(mode=0o700)
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,cases=5,seconds=20,rss_mib=256,scope='Synthetic process lifecycle, pressure admission/preemption and disposable SQLite queue/spool only; no Lance/models/user palace or activation. Original production deadlines/RSS unchanged.'),indent=2)+'\n')
elif sys.argv[1]=='run':
 admit();base=RESULTS/'pytest';assert not base.exists()
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout','20','--rss-mib','256',*TESTS,'--noconftest','-p','no:cacheprovider','--basetemp',str(base),'--junitxml',str(RESULTS/'result.xml'),'-x','-q','-s'])
elif sys.argv[1]=='verify':
 admit();log=(RESULTS/'output.log').read_text();root=ET.parse(RESULTS/'result.xml').getroot()
 cases=root.findall('.//testcase');fail=[n for kind in ['failure','error','skipped'] for n in root.findall('.//'+kind)]
 metrics=re.findall(r'\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed',log)
 assert len(metrics)==1;peak,elapsed=map(float,metrics[0])
 passed=int(sys.argv[2])==0 and len(cases)==5 and not fail and peak<=256 and elapsed<=20
 (RESULTS/'result.json').write_text(json.dumps(dict(status='PASS' if passed else 'FAIL',cases=len(cases),failures=len(fail),rss_mib=peak,seconds=elapsed,source=SOURCE),indent=2)+'\n')
 assert passed
elif sys.argv[1]=='archive':
 admit();entries=[];stack=[RESULTS];size=allocated=0
 while stack:
  p=stack.pop();info=p.lstat();assert stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
  entries.append(p);size+=info.st_size;allocated+=info.st_blocks*512
  assert len(entries)<=8192 and max(size,allocated)<=64*1024**2
  if stat.S_ISDIR(info.st_mode):stack.extend(p.iterdir())
 with tarfile.open(ROOT/'pressure-evidence.tar','w',dereference=False) as tar:
  for p in sorted(entries):tar.add(p,arcname=p.relative_to(ROOT),recursive=False)
 print(json.dumps(dict(bytes=(ROOT/'pressure-evidence.tar').stat().st_size,sha256=hashlib.sha256((ROOT/'pressure-evidence.tar').read_bytes()).hexdigest())))
else:raise SystemExit('closed operation')
