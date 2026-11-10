import json,os,sys,subprocess,re,stat,platform
from pathlib import Path
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-live-codec-results'
SOURCE='5d1b812dbed83963a349bd587857b5c58fa65531'
MODE='red'
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true' and sys.platform=='linux' and platform.python_version()=='3.12.11'
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/live_codec_check.py' for p in subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines())
admit()
if sys.argv[1]=='prepare':
 assert not RESULTS.exists();RESULTS.mkdir(mode=0o700)
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,mode=MODE,seconds=20,rss_mib=256,scope='Existing capture plus split body-free journal recovery; actual SQLite only; no user data, models, palace, native qualification or activation'),indent=2)+'\n')
elif sys.argv[1]=='run':
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout','20','--rss-mib','256','tests/test_index_schedule.py::test_explicit_schedule_initialization_upgrades_only_on_operator_call','--noconftest','-p','no:cacheprovider','--basetemp',str(Path(os.environ['RUNNER_TEMP'])/'castle-live-control-pytest'),'--junitxml',str(RESULTS/'result.xml'),'-x','-q'])
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
 assert not root.findall('.//error')
 assert len(root.findall('.//skipped')) == 3
 assert all(c.get('name','').startswith('test_real_admission_and_index_driver_resume_without_reinference[') for c in cases if c.find('skipped') is not None)
 if MODE=='red':
  assert int(sys.argv[2])==1 and len(cases)==1 and len(fail)==1
  assert cases[0].get('name')=='test_explicit_schedule_initialization_upgrades_only_on_operator_call'
  assert 'AttributeError' in fail[0].get('message','') and 'initialize_index_schedule' in fail[0].get('message','')
 else:
  assert int(sys.argv[2])==0 and len(cases)==474 and not fail
  capacity=[c.get('name') for c in cases if c.get('name','').startswith('test_serialized_live_owners_share_workspace_but_keep_retained_capacity[')]
  assert sum(c.get('classname')=='tests.test_index_control_prepare' for c in cases)==9
  assert sum(c.get('classname')=='tests.test_index_coordinator' for c in cases)==22
  assert sum(c.get('classname')=='tests.test_memory_selection_lease' for c in cases)==12
  assert sum(c.get('classname')=='tests.test_index_native_prepare' for c in cases)==17
  assert sum(c.get('classname')=='tests.test_index_preparation_coordinator' for c in cases)==19
  assert sum(c.get('classname')=='tests.test_registered_recovery' for c in cases)==17
  assert sum(c.get('classname')=='tests.test_semantic_indexer' for c in cases)==17
  assert len(capacity)==2 and any('[False-' in n for n in capacity) and any('[True-' in n for n in capacity)
  assert any(c.get('name')=='test_shared_workspace_still_refuses_actual_retained_spill_overflow' for c in cases)
 (RESULTS/'result.json').write_text(json.dumps(dict(status='PASS',cases=len(cases),failures=len(fail),rss_mib=peak,seconds=elapsed,source=SOURCE,scope='Bounded operator semantic coordinator and existing capture regression; three real macOS-admission cases are explicitly skipped on Linux; no native/model or production activation proof'),indent=2)+'\n')
else:raise SystemExit('closed operation')
