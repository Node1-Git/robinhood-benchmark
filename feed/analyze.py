import collections,csv,hashlib,json,math,pathlib,random,statistics,sys
P=pathlib.Path(sys.argv[1])
def pct(a,p):
 a=sorted(a)
 if not a:return None
 k=(len(a)-1)*p;i=int(k);return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(k-i)
def summary(a):
 if not a:return {'n':0}
 return {'n':len(a),'mean_ms':statistics.mean(a),'median_ms':statistics.median(a),'p01_ms':pct(a,.01),'p05_ms':pct(a,.05),'p95_ms':pct(a,.95),'p99_ms':pct(a,.99),'p999_ms':pct(a,.999),'min_ms':min(a),'max_ms':max(a),'node1_wins':sum(x>0 for x in a),'official_wins':sum(x<0 for x in a),'ties':sum(x==0 for x in a),'win_pct':100*sum(x>0 for x in a)/len(a),'lead_over_1ms_pct':100*sum(x>1 for x in a)/len(a),'lead_over_5ms_pct':100*sum(x>5 for x in a)/len(a),'within_1ms_pct':100*sum(abs(x)<=1 for x in a)/len(a),'official_lead_over_1ms_pct':100*sum(x < -1 for x in a)/len(a)}
def block_ci(groups,seed=20260911,replicates=2000):
 rng=random.Random(seed);median=[];mean=[];wins=[];groups=list(groups)
 for _ in range(replicates):
  x=[v for g in rng.choices(groups,k=len(groups)) for v in g]
  median.append(statistics.median(x));mean.append(statistics.mean(x));wins.append(sum(v>0 for v in x)*100/len(x))
 return {'method':'resample nonoverlapping 60-second blocks within each region, with replacement; percentile intervals','replicates':replicates,'seed':seed,'blocks':len(groups),'median_ms_95ci':[pct(median,.025),pct(median,.975)],'mean_ms_95ci':[pct(mean,.025),pct(mean,.975)],'node1_win_pct_95ci':[pct(wins,.025),pct(wins,.975)],'scope':'sampling variability in this observed session, not an SLA or a population guarantee'}
def analyze(region):
 root=P/region;master=json.loads((root/'summary.json').read_text());rows=[];groups=collections.defaultdict(list);perround=[];minute=[];checks=[];allseq=set();overlap=0
 for n in range(1,7):
  folder=root/f'round-{n}';s=json.loads((folder/'summary.json').read_text());r=[json.loads(x) for x in (folder/'pairs.jsonl').read_text().splitlines()];v=[x['lead_ms'] for x in r]
  assert len(r)==s['summary']['n']
  assert len(set(x['sequence'] for x in r))==len(r)
  for x in r:
   assert x['lead_ms']==(x['official_mono_ns']-x['node1_mono_ns'])/1e6
   assert 0<=x['offset_s']<300
   if x['sequence'] in allseq:overlap+=1
   allseq.add(x['sequence']);groups[(n,int(x['offset_s']//60))].append(x['lead_ms'])
  assert abs(statistics.median(v)-s['summary']['p50_ms'])<1e-8
  assert abs(statistics.mean(v)-s['summary']['mean_ms'])<1e-8
  perround.append({'round':n,'utc':s['meta']['measurement_start_utc'],**summary(v),'node1_peer':s['receivers']['node1']['connections'][0]['peer'],'official_peer':s['receivers']['official']['connections'][0]['peer']})
  for m in range(5):minute.append({'round':n,'minute_in_round':m+1,'minute_total':(n-1)*5+m+1,**summary(groups[(n,m)])})
  loads=json.loads((folder/'host-load.json').read_text());usage=[];steal=[]
  for prev,cur in zip(loads,loads[1:]):
   a=[int(x) for x in prev['proc_stat'].split()[1:9]];b=[int(x) for x in cur['proc_stat'].split()[1:9]];d=[y-x for x,y in zip(a,b)];total=sum(d)
   if total:usage.append(100*(1-(d[3]+d[4])/total));steal.append(100*d[7]/total)
  checks.append({'host_cpu_busy_pct_mean':statistics.mean(usage),'host_cpu_busy_pct_max':max(usage),'host_steal_pct_max':max(steal),'round':n,'cohort':s['cohort_n'],'matched':len(r),'node1_only':len(s['unmatched']['node1_only']),'official_only':len(s['unmatched']['official_only']),'message_mismatches':len(s['content_mismatches']),'union_gap_count':sum(x['count'] for x in s['union_sequence_gaps']),'recovery_excluded':s['reconnect_warmup_excluded'],'errors':{k:len(v['errors']) for k,v in s['receivers'].items()},'connections':{k:len(v['connections']) for k,v in s['receivers'].items()},'cpu_pct_of_one_core':{k:100*v['process_cpu_seconds']/v['elapsed_seconds'] for k,v in s['receivers'].items()},'loop_lag':{k:v['loop_lag_ms'] for k,v in s['receivers'].items()},'decode_quality':s['quality']})
  rows+=r
 vals=[r['lead_ms'] for r in rows];assert len(rows)==master['combined']['n'];assert overlap==0
 peer_groups=collections.defaultdict(list)
 for rnd in perround:
  peer_groups[rnd['official_peer'][0]] += [r['lead_ms'] for r in rows if r['round']==rnd['round']]
 paired_by_minute=[{k:r[k] for k in ['minute_total','n','median_ms','win_pct']} for r in minute]
 result={'region':region,'metric':'official receipt minus Node1 receipt, milliseconds; positive = Node1 faster','effective_seconds':1800,'summary':summary(vals),'confidence':block_ci(groups.values()),'relative_delay_ms':{'node1':{f'p{p}':pct([max(-v,0) for v in vals],p/100) for p in [50,90,95,99]},'official':{f'p{p}':pct([max(v,0) for v in vals],p/100) for p in [50,90,95,99]}},'rounds':perround,'minutes':minute,'quality':checks,'by_official_peer':{k:summary(v) for k,v in peer_groups.items()},'round_overlap':overlap}
 (root/'analysis.json').write_text(json.dumps(result,indent=2))
 with (root/'pairs.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
 with (root/'minutes.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=list(minute[0]));w.writeheader();w.writerows(minute)
 return result
results=[analyze(r) for r in (sys.argv[2:] or ['.'])]
(P/'analysis.json').write_text(json.dumps(results,indent=2));print(json.dumps([{k:v for k,v in r.items() if k in ['region','summary','confidence','relative_delay_ms']} for r in results],indent=2))
