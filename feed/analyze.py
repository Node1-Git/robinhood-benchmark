import collections,csv,hashlib,json,math,pathlib,random,statistics,sys
P=pathlib.Path(sys.argv[1])
def pct(a,p):
    a=sorted(a)
    if not a:return None
    k=(len(a)-1)*p;i=int(k);return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(k-i)
def summary(a):
    if not a:return {'n':0}
    return {'n':len(a),'mean_ms':statistics.mean(a),'median_ms':statistics.median(a),'p01_ms':pct(a,.01),'p05_ms':pct(a,.05),'p95_ms':pct(a,.95),'p99_ms':pct(a,.99),'p999_ms':pct(a,.999),'min_ms':min(a),'max_ms':max(a),'provider_wins':sum(x>0 for x in a),'baseline_wins':sum(x<0 for x in a),'ties':sum(x==0 for x in a),'win_pct':100*sum(x>0 for x in a)/len(a)}
def block_ci(groups,seed=20260911,replicates=2000):
    rng=random.Random(seed);median=[];mean=[];wins=[];groups=list(groups)
    for _ in range(replicates):
        x=[v for g in rng.choices(groups,k=len(groups)) for v in g]
        if not x: continue
        median.append(statistics.median(x));mean.append(statistics.mean(x));wins.append(sum(v>0 for v in x)*100/len(x))
    if not median: return {}
    return {'method':'resample nonoverlapping 60-second blocks','replicates':replicates,'median_ms_95ci':[pct(median,.025),pct(median,.975)]}

def analyze(region):
    root=P/region;master=json.loads((root/'summary.json').read_text())
    sources = list(master.get('combined', {}).keys())
    
    rows=[]
    for n in range(1,7):
        folder=root/f'round-{n}'
        if not folder.exists(): continue
        try:
            r=[json.loads(x) for x in (folder/'pairs.jsonl').read_text().splitlines()]
            rows+=r
        except Exception: pass

    result={'region':region, 'sources': sources, 'effective_seconds':1800, 'summary': {}, 'confidence': {}}
    for n in sources:
        vals = [r[f'lead_ms_{n}'] for r in rows if f'lead_ms_{n}' in r]
        result['summary'][n] = summary(vals)
        groups = collections.defaultdict(list)
        for r in rows:
            if f'lead_ms_{n}' in r:
                groups[(r['round'], int(r['offset_s']//60))].append(r[f'lead_ms_{n}'])
        result['confidence'][n] = block_ci(groups.values())
        
    (root/'analysis.json').write_text(json.dumps(result,indent=2))
    if rows:
        with (root/'pairs.csv').open('w') as f:
            w=csv.DictWriter(f,fieldnames=list(rows[0].keys()));w.writeheader();w.writerows(rows)
    return result

results=[analyze(r) for r in (sys.argv[2:] or ['.'])]
(P/'analysis.json').write_text(json.dumps(results,indent=2))
print(json.dumps([{k:v for k,v in r.items() if k in ['region','summary','confidence']} for r in results],indent=2))
