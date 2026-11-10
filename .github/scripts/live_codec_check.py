import json,os,sys,subprocess,re,stat,platform
from pathlib import Path
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[2]
RESULTS=ROOT/'ci-live-codec-results'
SOURCE='1ba497912ecb2796d5d06389060c7dfe61512273'
MODE='green'
def admit():
 assert os.environ.get('GITHUB_ACTIONS')=='true' and sys.platform=='linux' and platform.python_version()=='3.12.11'
 assert subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()==SOURCE
 assert all(p.startswith('.github/workflows/') or p=='.github/scripts/live_codec_check.py' for p in subprocess.check_output(['git','diff','--name-only','HEAD^','HEAD'],text=True).splitlines())
admit()
if sys.argv[1]=='prepare':
 assert not RESULTS.exists();RESULTS.mkdir(mode=0o700)
 (RESULTS/'scope.json').write_text(json.dumps(dict(source=SOURCE,mode=MODE,seconds=20,rss_mib=256,scope='Temporary SQLite control-schema transactions only; not OS admission or native Lance qualification; no user data, models, palace, native qualification or activation'),indent=2)+'\n')
elif sys.argv[1]=='run':
 os.execv(sys.executable,[sys.executable,'scripts/safe_brain_tests.py','--timeout','20','--rss-mib','256','tests/test_layout_live_control.py','--noconftest','-p','no:cacheprovider','--basetemp',str(Path(os.environ['RUNNER_TEMP'])/'castle-live-control-pytest'),'--junitxml',str(RESULTS/'result.xml'),'-x','-q'])
elif sys.argv[1]=='verify':
 import tarfile
 base=Path(os.environ['RUNNER_TEMP'])/'castle-live-control-pytest'
 count=total=0
 with tarfile.open(RESULTS/'fixtures.tar.gz','w:gz',dereference=False) as archive:
  for parent,dirs,files in os.walk(base,followlinks=False):
   assert all(not (Path(parent)/name).is_symlink() for name in dirs)
   for path in [Path(parent),*(Path(parent)/name for name in files)]:
    info=path.lstat();assert stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
    count+=1;total+=info.st_size if stat.S_ISREG(info.st_mode) else 0
    assert count<=8192 and total<=64*1024**2
    archive.add(path,arcname=str(path.relative_to(base.parent)),recursive=False)
 (RESULTS/'fixture-manifest.json').write_text(json.dumps(dict(entries=count,bytes=total,scope='Synthetic control SQLite only; no native storage/user palace'))+chr(10))
 root=ET.parse(RESULTS/'result.xml').getroot();cases=root.findall('.//testcase');fail=root.findall('.//failure')
 metrics=re.findall(r'\[guard\] sampled aggregate peak ([0-9.]+) MiB; ([0-9.]+)s; completed',(RESULTS/'output.log').read_text())
 assert len(metrics)==1;peak,elapsed=map(float,metrics[0]);assert peak<=256 and elapsed<=20
 assert not root.findall('.//error') and not root.findall('.//skipped')
 if MODE=='red':
  assert int(sys.argv[2])==1 and len(cases)==7 and len(fail)==1
  assert cases[0].get('name')=='test_direct_publication_preserves_original_records_and_expected_receipt'
  assert 'live_publication_codec' in fail[0].get('message','') and 'ImportError' in fail[0].get('message','')
 else:
  assert int(sys.argv[2])==1 and len(cases)==7 and len(fail)==1
  assert 'AttributeError' in fail[0].get('message','') and 'live_open' in fail[0].get('message','')
 (RESULTS/'result.json').write_text(json.dumps(dict(status='EXPECTED_MISSING_LIVE_OPEN',cases=len(cases),failures=len(fail),rss_mib=peak,seconds=elapsed,source=SOURCE,scope='Preparation/framing only; not durable publication or restored capture'),indent=2)+'\n')
else:raise SystemExit('closed operation')
