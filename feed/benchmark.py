import asyncio,datetime,fcntl,hashlib,json,multiprocessing as mp,os,pathlib,random,shutil,socket,statistics,struct,subprocess,sys,time
ROOT=pathlib.Path(os.environ.get('NODE1_FEED_BENCH_ROOT','./data/feed'))
import websockets
HEADER=struct.Struct('!QQI')

def atomic_text(path, text):
 tmp=path.with_name(path.name+'.tmp')
 with tmp.open('w') as f:
  f.write(text);f.flush();os.fsync(f.fileno())
 tmp.replace(path)
 fd=os.open(path.parent,os.O_RDONLY)
 try:os.fsync(fd)
 finally:os.close(fd)

def prepare_round(root, number):
 folder=root/f'round-{number}'
 if (folder/'summary.json').exists():
  previous=json.loads((folder/'summary.json').read_text())
  if previous['meta']['round']!=number or previous['meta']['measurement_seconds']!=300:raise RuntimeError('unexpected checkpoint metadata')
  if not (folder/'pairs.jsonl').exists():raise RuntimeError('checkpoint is missing pair data')
  return folder,True
 if folder.exists():
  interrupted=root/'interrupted-attempts';interrupted.mkdir(exist_ok=True)
  folder.rename(interrupted/(f'round-{number}-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')))
 folder.mkdir()
 return folder,False

def utc(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def quantile(a,p):
 if not a:return None
 k=(len(a)-1)*p;i=int(k);return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(k-i)
def stats(v):
 a=sorted(v)
 if not a:return {'n':0}
 return {'n':len(a),'mean_ms':statistics.mean(a),**{f'p{int(p*100):02d}_ms':quantile(a,p) for p in [.01,.05,.10,.25,.50,.75,.90,.95,.99]},'p999_ms':quantile(a,.999),'min_ms':a[0],'max_ms':a[-1],'node1_first':sum(x>0 for x in a),'official_first':sum(x<0 for x in a),'ties':sum(x==0 for x in a),'node1_first_pct':100*sum(x>0 for x in a)/len(a)}

def receiver(source,core,folder,deadline,ready):
 os.sched_setaffinity(0,{core})
 async def collect():
  url=os.environ.get('OFFICIAL_FEED_URL','wss://feed.mainnet.chain.robinhood.com') if source=='official' else os.environ['NODE1_FEED_URL']
  frames=0;totalbytes=0;jitter=[];connections=[];errors=[];begin=time.monotonic_ns();cpu_begin=time.process_time();stop=False
  path=pathlib.Path(folder)
  def status():
   v={'source':source,'core':core,'frames':frames,'bytes':totalbytes,'connections':connections,'errors':errors,'loop_lag_ms':{'n':len(jitter),'p50':quantile(sorted(jitter),.5),'p99':quantile(sorted(jitter),.99),'max':max(jitter,default=0)},'process_cpu_seconds':time.process_time()-cpu_begin,'elapsed_seconds':(time.monotonic_ns()-begin)/1e9}
   tmp=path/(source+'-status.tmp');tmp.write_text(json.dumps(v));tmp.replace(path/(source+'-status.json'))
  async def heartbeat():
   last=time.monotonic()
   while not stop:
    t=time.monotonic();await asyncio.sleep(.01);jitter.append(max(0,(time.monotonic()-t-.01)*1000))
    if time.monotonic()-last>=5:status();last=time.monotonic()
  beat=asyncio.create_task(heartbeat())
  try:
   with (path/(source+'.frames')).open('wb',buffering=1048576) as f:
    while not deadline.value or time.monotonic_ns()<deadline.value:
     try:
      async with websockets.connect(url,additional_headers={'Arbitrum-Feed-Client-Version':'2'},compression=None,max_size=64*1024*1024,max_queue=1024,open_timeout=15,ping_interval=20,ping_timeout=20,close_timeout=2) as ws:
       now=time.monotonic_ns();connections.append({'monotonic_ns':now,'utc':utc(),'peer':ws.remote_address});ready.value=now
       while not deadline.value or time.monotonic_ns()<deadline.value:
        try:raw=await asyncio.wait_for(ws.recv(decode=False),timeout=1)
        except asyncio.TimeoutError:continue
        mono=time.monotonic_ns();wall=time.time_ns()
        if isinstance(raw,str):raw=raw.encode()
        f.write(HEADER.pack(mono,wall,len(raw)));f.write(raw);frames+=1;totalbytes+=len(raw)
     except Exception as e:
      err={'utc':utc(),'monotonic_ns':time.monotonic_ns(),'type':type(e).__name__}
      response=getattr(e,'response',None)
      if response is not None:err['http_status']=response.status_code
      errors.append(err);status();await asyncio.sleep(min(30,2**min(len(errors),5)))
  finally:
   stop=True;beat.cancel();await asyncio.gather(beat,return_exceptions=True);status()
 asyncio.run(collect())

def load_frames(path):
 data={};duplicates=0;conflicts=0;parse_errors=0;nonmessage=0;backwards=0;previous=None
 with path.open('rb') as f:
  while h:=f.read(HEADER.size):
   if len(h)!=HEADER.size:raise RuntimeError('truncated frame header')
   t,wall,n=HEADER.unpack(h);raw=f.read(n)
   if len(raw)!=n:raise RuntimeError('truncated frame body')
   try:o=json.loads(raw)
   except Exception:parse_errors+=1;continue
   messages=o.get('messages',[])
   if not messages:nonmessage+=1
   for m in messages:
    seq=int(m['sequenceNumber']);digest=hashlib.sha256(json.dumps(m,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    if previous is not None and seq<previous:backwards+=1
    previous=seq
    if seq in data:
     duplicates+=1
     if data[seq]['sha256']!=digest:conflicts+=1
    else:data[seq]={'mono_ns':t,'wall_ns':wall,'sha256':digest,'block_hash':m.get('blockHash')}
 return data,{'unique_messages':len(data),'duplicates':duplicates,'duplicate_content_conflicts':conflicts,'parse_errors':parse_errors,'nonmessage_frames':nonmessage,'out_of_order':backwards}

def analyze(folder,meta):
 records={};quality={};states={}
 for n in ['node1','official']:
  records[n],quality[n]=load_frames(folder/(n+'.frames'));states[n]=json.loads((folder/(n+'-status.json')).read_text())
 lo=meta['measurement_start_ns'];hi=meta['measurement_end_ns'];universe=set(records['node1'])|set(records['official']);rows=[];unmatched={'node1_only':[],'official_only':[]};mismatches=[];excluded=0;cohort=[]
 recovery=[]
 for s in states.values():
  recovery += [(c['monotonic_ns'],c['monotonic_ns']+20_000_000_000) for c in s['connections'][1:]]
 for seq in sorted(universe):
  a=records['node1'].get(seq);b=records['official'].get(seq);first=min(x['mono_ns'] for x in [a,b] if x)
  if not lo<=first<hi:continue
  cohort.append(seq)
  if a is None or b is None:unmatched['node1_only' if a else 'official_only'].append(seq);continue
  if a['sha256']!=b['sha256']:mismatches.append(seq);continue
  if any(l<=first<r for l,r in recovery):excluded+=1;continue
  rows.append({'round':meta['round'],'sequence':seq,'block_hash':a['block_hash'],'sha256':a['sha256'],'node1_mono_ns':a['mono_ns'],'official_mono_ns':b['mono_ns'],'first_wall_ns':min(a['wall_ns'],b['wall_ns']),'offset_s':(first-lo)/1e9,'lead_ms':(b['mono_ns']-a['mono_ns'])/1e6})
 minutes=[]
 for i in range(5):minutes.append({'minute':i+1,**stats([x['lead_ms'] for x in rows if i*60<=x['offset_s']<(i+1)*60])})
 gaps=[]
 for a,b in zip(cohort,cohort[1:]):
  if b>a+1:gaps.append({'after':a,'before':b,'count':b-a-1})
 summary={'meta':meta,'summary':stats([x['lead_ms'] for x in rows]),'minutes':minutes,'quality':quality,'receivers':states,'cohort_n':len(cohort),'unmatched':unmatched,'content_mismatches':mismatches,'reconnect_warmup_excluded':excluded,'union_sequence_gaps':gaps,'first_sequence':cohort[0] if cohort else None,'last_sequence':cohort[-1] if cohort else None}
 atomic_text(folder/'pairs.jsonl',''.join(json.dumps(x)+'\n' for x in rows));atomic_text(folder/'summary.json',json.dumps(summary,indent=2));return summary

def cpu_snapshot():
 return {'utc':utc(),'monotonic_ns':time.monotonic_ns(),'loadavg':pathlib.Path('/proc/loadavg').read_text().strip(),'proc_stat':pathlib.Path('/proc/stat').read_text().splitlines()[0]}

def main():
 os.umask(0o077);os.environ['NODE1_FEED_URL'];os.sched_setaffinity(0,{0});ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
 lock=(ROOT/'runner.lock').open('a')
 fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
 if (ROOT/'summary.json').exists():
  final=json.loads((ROOT/'summary.json').read_text())
  if len(final.get('rounds',[]))==6:
   print(json.dumps({'event':'already_complete','combined':final['combined']}),flush=True);return
  raise RuntimeError('invalid final checkpoint')
 (ROOT/'start.json').write_text(json.dumps({'utc':utc(),'pid':os.getpid(),'rounds':6,'measurement_seconds_per_round':300,'warmup_seconds':20,'drain_seconds':10,'metric':'official receive minus Node1 receive, positive means Node1 earlier'}))
 for i in range(1,7):
  folder,finished=prepare_round(ROOT,i)
  if finished:
   print(json.dumps({'event':'resume_skip_completed','round':i}),flush=True);continue
  if shutil.disk_usage(ROOT).free<2*1024**3:raise RuntimeError('less than 2 GiB free; refusing another round')
  deadline=mp.Value('q',0);ready={n:mp.Value('q',0) for n in ['node1','official']};cores={'node1':0,'official':0}
  procs=[mp.Process(target=receiver,args=(n,cores[n],str(folder),deadline,ready[n])) for n in ['node1','official']]
  for p in procs:p.start()
  end=time.monotonic()+45
  while not all(x.value for x in ready.values()) and time.monotonic()<end:time.sleep(.1)
  if not all(x.value for x in ready.values()):
   deadline.value=time.monotonic_ns()
   for p in procs:p.join(20)
   raise RuntimeError('one or more feeds could not connect; inspect redacted status')
  start=max(x.value for x in ready.values())+20_000_000_000;finish=start+300_000_000_000;deadline.value=finish+10_000_000_000
  meta={'round':i,'measurement_start_ns':start,'measurement_end_ns':finish,'measurement_start_utc':datetime.datetime.fromtimestamp((time.time_ns()+start-time.monotonic_ns())/1e9,datetime.timezone.utc).isoformat(),'measurement_seconds':300,'warmup_seconds':20,'drain_seconds':10,'cores':cores,'collector_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()}
  (folder/'meta.json').write_text(json.dumps(meta,indent=2));print(json.dumps({'event':'round_started',**meta}),flush=True)
  samples=[]
  while time.monotonic_ns()<deadline.value:
   samples.append(cpu_snapshot());(ROOT/'progress.json').write_text(json.dumps({'utc':utc(),'round':i,'measurement_elapsed_s':max(0,min(300,(time.monotonic_ns()-start)/1e9))}));time.sleep(5)
  for p in procs:
   p.join(20)
   if p.is_alive():p.terminate();p.join();raise RuntimeError('receiver failed to finish')
   if p.exitcode:raise RuntimeError('receiver failed')
  atomic_text(folder/'host-load.json',json.dumps(samples))
  for raw in folder.glob('*.frames'):
   with raw.open('rb') as f:os.fsync(f.fileno())
  summary=analyze(folder,meta);print(json.dumps({'event':'round_completed','round':i,'summary':summary['summary'],'unmatched':{k:len(v) for k,v in summary['unmatched'].items()},'mismatches':len(summary['content_mismatches'])}),flush=True)
 allrows=[];rounds=[]
 for i in range(1,7):
  folder=ROOT/f'round-{i}';rounds.append(json.loads((folder/'summary.json').read_text()));allrows += [json.loads(l) for l in (folder/'pairs.jsonl').read_text().splitlines()]
 final={'finished_utc':utc(),'combined':stats([x['lead_ms'] for x in allrows]),'rounds':rounds}
 atomic_text(ROOT/'summary.json',json.dumps(final,indent=2));print(json.dumps({'event':'complete','combined':final['combined']}),flush=True)
if __name__=='__main__':main()
