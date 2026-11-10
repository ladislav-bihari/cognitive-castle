import json,os,platform,re,stat,subprocess,sys,tarfile
from pathlib import Path
from importlib.metadata import version
import xml.etree.ElementTree as ET
SOURCE='6db2299ab9df7aed2b0c2ba51796b926a8706478'
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-live-native-results'
SCRATCH=Path(os.environ['RUNNER_TEMP'])/'castle-live-native-20261110'
PHASES={
 'happy':('test_direct_native_publication_reopens_and_refuses_changed_request',20),
 'recovery':('test_direct_native_lost_append_response_resumes_exact_pending_batch',18),
}
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true'
 assert sys.platform=='darwin' and platform.machine()=='arm64' and platform.mac_ver()[0].split('.')[0]=='15'
 assert platform.python_version()=='3.12.10'
 assert {n:version(n) for n in ('pylance','pyarrow','numpy')}=={'pylance':'4.0.1','pyarrow':'24.0.0','numpy':'2.4.4'}
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/direct_live_native.py' for p in subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines())
 assert not SCRATCH.resolve().is_relative_to(ROOT)
admit()
mode=sys.argv[1]
if mode=='prepare':
 assert not RESULTS.exists() and not SCRATCH.exists()
 RESULTS.mkdir(mode=0o700);SCRATCH.mkdir(mode=0o700)
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,host='macOS15arm64/Python3.12.10',packages={n:version(n) for n in ('pylance','pyarrow','numpy')},phases=PHASES,seconds_per_phase=20,aggregate_rss_mib=256,control_rss_mib=[128,192],exact_rss_mib=[192,256],session_seconds=5,scope='Synthetic direct publication and lost-response recovery; no installed palace, models, activation or capacity/corpus permit'),indent=2)+chr(10))
elif mode=='run':
 phase=sys.argv[2];assert phase in PHASES
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout','20','--rss-mib','256','tests/test_lance_live_native.py','--noconftest','-p','no:cacheprovider','--basetemp',str(SCRATCH/phase),'--junitxml',str(RESULTS/(phase+'.xml')),'-k',PHASES[phase][0],'-x','-s','--tb=short'])
elif mode=='verify':
 phase=sys.argv[2];assert phase in PHASES
 root=ET.parse(RESULTS/(phase+'.xml')).getroot();cases=root.findall('.//testcase')
 assert int(sys.argv[3])==0 and len(cases)==1 and cases[0].get('name')==PHASES[phase][0]
 assert not root.findall('.//failure') and not root.findall('.//error') and not root.findall('.//skipped')
 log=(RESULTS/(phase+'.log')).read_text()
 metrics=re.findall(r'\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed',log)
 assert len(metrics)==1;peak,elapsed=map(float,metrics[0]);assert peak<=256 and elapsed<=20
 starts=[json.loads(line.split('LAYOUT_NATIVE_START ',1)[1]) for line in log.splitlines() if 'LAYOUT_NATIVE_START ' in line]
 stages=[json.loads(line.split('LAYOUT_NATIVE_STAGE ',1)[1]) for line in log.splitlines() if 'LAYOUT_NATIVE_STAGE ' in line]
 count=PHASES[phase][1];assert len(starts)==len(stages)==count
 assert [s['call'] for s in starts]==[s['call'] for s in stages]==list(range(1,count+1))
 for s in stages:
  assert s['status'] in {'ok','conflict'} and s.get('reason') is None and len(s['resources'])==1
  q=s['operation'].startswith(('queue_','direct_operator_'));task,total=(128,192) if q else (192,256)
  r=s['resources'][0]
  assert 0<r['peak_task_rss_bytes']<=task*1024**2 and 0<r['peak_total_rss_bytes']<=total*1024**2 and 0<r['elapsed_seconds']<=5
 (RESULTS/(phase+'.json')).write_text(json.dumps(dict(status='PASS',source=SOURCE,cases=1,starts=count,rss_mib=peak,seconds=elapsed,scope='Finite synthetic native direct-live case, not full corpus/default route/capture/production readiness'),indent=2)+chr(10))
elif mode=='archive':
 count=total=0;links=[]
 with tarfile.open(RESULTS/'fixtures.tar.gz','w:gz',dereference=False) as archive:
  for parent,dirs,files in os.walk(SCRATCH,followlinks=False):
   for name in list(dirs):
    p=Path(parent)/name
    if p.is_symlink():links.append(str(p.relative_to(SCRATCH)));dirs.remove(name)
   for p in [Path(parent),*(Path(parent)/name for name in files)]:
    info=p.lstat()
    if stat.S_ISLNK(info.st_mode):links.append(str(p.relative_to(SCRATCH)));continue
    assert stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
    count+=1;total+=info.st_size if stat.S_ISREG(info.st_mode) else 0
    assert count<=8192 and total<=64*1024**2 and len(links)<=8192
    archive.add(p,arcname=str(p.relative_to(SCRATCH.parent)),recursive=False)
 (RESULTS/'fixture-manifest.json').write_text(json.dumps(dict(entries=count,bytes=total,omitted_symlink_aliases=links),indent=2)+chr(10))
else:raise SystemExit('closed operation')
