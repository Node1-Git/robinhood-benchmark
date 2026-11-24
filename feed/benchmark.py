import asyncio,datetime,fcntl,hashlib,json,multiprocessing as mp,os,pathlib,random,shutil,socket,statistics,struct,subprocess,sys,time
import websockets
HEADER=struct.Struct('!QQI')
ROOT=pathlib.Path(os.environ.get('NODE1_FEED_BENCH_ROOT','./data/feed'))

sys.path.append(str(pathlib.Path(__file__).parent))
import adapters

def atomic_text(path, text):
    tmp=path.with_name(path.name+'.tmp')
    with tmp.open('w') as f:
        f.write(text);f.flush();os.fsync(f.fileno())
    tmp.replace(path)
    fd=os.open(path.parent,os.O_RDONLY)
    try:os.fsync(fd)
    except OSError: pass
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
    folder.mkdir(exist_ok=True)
    return folder,False

def utc(): return datetime.datetime.now(datetime.timezone.utc).isoformat()
def quantile(a,p):
    if not a:return None
    k=(len(a)-1)*p;i=int(k);return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(k-i)
def stats(v):
    a=sorted(v)
    if not a:return {'n':0}
    return {'n':len(a),'mean_ms':statistics.mean(a),**{f'p{int(p*100):02d}_ms':quantile(a,p) for p in [.01,.05,.10,.25,.50,.75,.90,.95,.99]},'p999_ms':quantile(a,.999),'min_ms':a[0],'max_ms':a[-1],'provider_first':sum(x>0 for x in a),'baseline_first':sum(x<0 for x in a),'ties':sum(x==0 for x in a),'provider_first_pct':100*sum(x>0 for x in a)/len(a)}

def receiver(source,url,core,folder,deadline,ready):
    os.sched_setaffinity(0,{core})
    if url.startswith('grpc'):
        grpc_receiver(source, url, core, folder, deadline, ready)
    else:
        ws_receiver(source, url, core, folder, deadline, ready)

def ws_receiver(source,url,core,folder,deadline,ready):
    async def collect():
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

def grpc_receiver(source,url,core,folder,deadline,ready):
    # Placeholder for Eira gRPC. In a real scenario, this would use grpcio with the compiled Eira protobuf.
    path=pathlib.Path(folder)
    frames=0;totalbytes=0;connections=[];errors=[]
    now=time.monotonic_ns();connections.append({'monotonic_ns':now,'utc':utc(),'peer':url});ready.value=now
    status={'source':source,'core':core,'frames':frames,'bytes':totalbytes,'connections':connections,'errors':errors,'process_cpu_seconds':0,'elapsed_seconds':0}
    tmp=path/(source+'-status.tmp');tmp.write_text(json.dumps(status));tmp.replace(path/(source+'-status.json'))
    
    with (path/(source+'.frames')).open('wb',buffering=1048576) as f:
        while not deadline.value or time.monotonic_ns()<deadline.value:
            time.sleep(0.1)

def load_frames(path):
    data={}; duplicates=0; conflicts=0; parse_errors=0; nonmessage=0; backwards=0; previous=None
    with path.open('rb') as f:
        while h:=f.read(HEADER.size):
            if len(h)!=HEADER.size:break
            t,wall,n=HEADER.unpack(h);raw=f.read(n)
            if len(raw)!=n:break
            
            parsed = adapters.parse_nitro(raw)
            if not parsed:
                parsed = adapters.parse_tx(raw)
            
            if not parsed:
                parse_errors+=1
                continue
                
            for p in parsed:
                seq = p['sequence']
                tx_hash = p['tx_hash']
                digest = p['digest']
                
                key = seq if seq is not None else tx_hash
                
                if seq is not None:
                    if previous is not None and seq < previous: backwards+=1
                    previous = seq
                    
                if key in data:
                    duplicates+=1
                    if data[key]['sha256'] != digest: conflicts+=1
                else:
                    data[key] = {'mono_ns':t, 'wall_ns':wall, 'sha256':digest, 'block_hash':p['block_hash']}
    return data,{'unique_messages':len(data),'duplicates':duplicates,'duplicate_content_conflicts':conflicts,'parse_errors':parse_errors,'nonmessage_frames':nonmessage,'out_of_order':backwards}

def get_sources():
    sources = {'official': os.environ.get('OFFICIAL_FEED_URL', 'wss://feed.mainnet.chain.robinhood.com')}
    for k, v in os.environ.items():
        if k.endswith('_FEED_URL') and k != 'OFFICIAL_FEED_URL':
            name = k[:-9].lower()
            sources[name] = v
    if 'node1' not in sources and 'NODE1_FEED_URL' in os.environ:
        sources['node1'] = os.environ['NODE1_FEED_URL']
    return sources

def analyze(folder,meta):
    records={};quality={};states={}
    source_names = meta.get('sources', ['official', 'node1'])
    for n in source_names:
        frames_path = folder/(n+'.frames')
        if not frames_path.exists(): continue
        records[n],quality[n]=load_frames(frames_path)
        try:
            states[n]=json.loads((folder/(n+'-status.json')).read_text())
        except Exception:
            states[n] = {'connections': [], 'errors': [], 'process_cpu_seconds':0, 'elapsed_seconds':0}

    lo=meta['measurement_start_ns'];hi=meta['measurement_end_ns']
    
    baseline = 'official' if 'official' in records else source_names[0]
    if baseline not in records:
        return {'meta':meta,'summary':{},'quality':{},'receivers':{},'cohort_n':0,'unmatched':{},'content_mismatches':0,'reconnect_warmup_excluded':0,'first_sequence':None,'last_sequence':None}
        
    baseline_records = records[baseline]
    
    cohort=[]
    rows=[]
    unmatched={n:[] for n in source_names}
    mismatches=[]
    excluded=0
    
    recovery=[]
    for s in states.values():
        if 'connections' in s:
            recovery += [(c['monotonic_ns'],c['monotonic_ns']+20_000_000_000) for c in s['connections'][1:]]
            
    for key in sorted(baseline_records.keys(), key=lambda x: str(x)):
        b = baseline_records[key]
        if not lo <= b['mono_ns'] < hi: continue
        
        cohort.append(key)
        
        row = {'round':meta['round'], 'key': key, 'block_hash':b['block_hash'], 'sha256':b['sha256'], f'{baseline}_mono_ns':b['mono_ns']}
        row['first_wall_ns'] = b['wall_ns']
        
        valid = True
        for n in source_names:
            if n == baseline: continue
            if n not in records: continue
            a = records[n].get(key)
            if a is None:
                unmatched[n].append(key)
                valid = False
            elif a['sha256'] != b['sha256']:
                mismatches.append(key)
                valid = False
            else:
                row[f'{n}_mono_ns'] = a['mono_ns']
                row['first_wall_ns'] = min(row['first_wall_ns'], a['wall_ns'])
                row[f'lead_ms_{n}'] = (b['mono_ns'] - a['mono_ns']) / 1e6
                
        if not valid: continue
        
        first = min([b['mono_ns']] + [records[n][key]['mono_ns'] for n in source_names if n != baseline and n in records])
        if any(l <= first < r for l,r in recovery):
            excluded+=1
            continue
            
        row['offset_s'] = (first - lo) / 1e9
        rows.append(row)
        
    summary_data = {}
    for n in source_names:
        if n == baseline: continue
        summary_data[n] = stats([x[f'lead_ms_{n}'] for x in rows if f'lead_ms_{n}' in x])
        
    summary={'meta':meta,'summary':summary_data,'quality':quality,'receivers':states,'cohort_n':len(cohort),'unmatched':unmatched,'content_mismatches':mismatches,'reconnect_warmup_excluded':excluded,'first_sequence':cohort[0] if cohort else None,'last_sequence':cohort[-1] if cohort else None}
    atomic_text(folder/'pairs.jsonl',''.join(json.dumps(x)+'\n' for x in rows))
    atomic_text(folder/'summary.json',json.dumps(summary,indent=2))
    return summary

def cpu_snapshot():
    return {'utc':utc(),'monotonic_ns':time.monotonic_ns(),'loadavg':pathlib.Path('/proc/loadavg').read_text().strip(),'proc_stat':pathlib.Path('/proc/stat').read_text().splitlines()[0]}

def main():
    os.umask(0o077);os.sched_setaffinity(0,{0});ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
    sources = get_sources()
    source_names = list(sources.keys())
    
    lock=(ROOT/'runner.lock').open('a')
    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    if (ROOT/'summary.json').exists():
        final=json.loads((ROOT/'summary.json').read_text())
        if len(final.get('rounds',[]))==6:
            print(json.dumps({'event':'already_complete','combined':final.get('combined')}),flush=True);return
        raise RuntimeError('invalid final checkpoint')
    (ROOT/'start.json').write_text(json.dumps({'utc':utc(),'pid':os.getpid(),'rounds':6,'measurement_seconds_per_round':300,'warmup_seconds':20,'drain_seconds':10,'sources':source_names}))
    
    for i in range(1,7):
        folder,finished=prepare_round(ROOT,i)
        if finished:
            print(json.dumps({'event':'resume_skip_completed','round':i}),flush=True);continue
        if shutil.disk_usage(ROOT).free<2*1024**3:raise RuntimeError('less than 2 GiB free; refusing another round')
        
        deadline=mp.Value('q',0);ready={n:mp.Value('q',0) for n in source_names};cores={n:0 for n in source_names}
        procs=[mp.Process(target=receiver,args=(n,sources[n],cores[n],str(folder),deadline,ready[n])) for n in source_names]
        for p in procs:p.start()
        
        end=time.monotonic()+45
        while not all(x.value for x in ready.values()) and time.monotonic()<end:time.sleep(.1)
        if not all(x.value for x in ready.values()):
            deadline.value=time.monotonic_ns()
            for p in procs:p.join(20)
            raise RuntimeError('one or more feeds could not connect; inspect redacted status')
            
        start=max(x.value for x in ready.values())+20_000_000_000;finish=start+300_000_000_000;deadline.value=finish+10_000_000_000
        meta={'round':i,'measurement_start_ns':start,'measurement_end_ns':finish,'measurement_start_utc':datetime.datetime.fromtimestamp((time.time_ns()+start-time.monotonic_ns())/1e9,datetime.timezone.utc).isoformat(),'measurement_seconds':300,'warmup_seconds':20,'drain_seconds':10,'cores':cores,'sources':source_names,'collector_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()}
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
        summary=analyze(folder,meta);print(json.dumps({'event':'round_completed','round':i}),flush=True)
        
    allrows=[];rounds=[]
    for i in range(1,7):
        folder=ROOT/f'round-{i}'
        try:
            rounds.append(json.loads((folder/'summary.json').read_text()))
            allrows += [json.loads(l) for l in (folder/'pairs.jsonl').read_text().splitlines()]
        except Exception: pass
    
    baseline = 'official' if 'official' in source_names else source_names[0]
    combined_stats = {}
    for n in source_names:
        if n == baseline: continue
        combined_stats[n] = stats([x[f'lead_ms_{n}'] for x in allrows if f'lead_ms_{n}' in x])
        
    final={'finished_utc':utc(),'combined':combined_stats,'rounds':rounds}
    atomic_text(ROOT/'summary.json',json.dumps(final,indent=2));print(json.dumps({'event':'complete'}),flush=True)

if __name__=='__main__':main()