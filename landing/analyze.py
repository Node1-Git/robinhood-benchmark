import pathlib,json,statistics,collections,hashlib,os
P=pathlib.Path(os.environ.get('LANDING_DATA_DIR','./data/landing')).resolve()
rows=json.loads((P/'results.json').read_text());summary=json.loads((P/'summary.json').read_text());assert summary['completed'] and len(rows)==100
candidate_file=P/'transactions.json'
if candidate_file.exists():candidates=json.loads(candidate_file.read_text())
else:candidates={'rounds':[{'nonce':r['nonce'],'transactions':[{'hash':h} for h in r['hashes']]} for r in json.loads((P/'candidate-hashes.json').read_text())]}
names=['node1-2b','node1-2c','official-1','official-2','official-3']
assert len({r['nonce'] for r in rows})==100
responses={};logs={}
for name in names:
 logs[name]=json.loads((P/(name+'.json')).read_text());assert len(logs[name])==100 and all('response' in x for x in logs[name])
 responses[name]=dict(collections.Counter(str(x['response']['http'])+':'+(str(x['response']['rpc'].get('error',{}).get('code')) if x['response']['rpc'].get('error') else 'result') for x in logs[name]))
for n,r in enumerate(rows):
 assert r['round']==n+1 and r['nonce']==candidates['rounds'][n]['nonce'];i=names.index(r['winner']);assert r['hash'].lower()==candidates['rounds'][n]['transactions'][i]['hash'].lower();assert r['receipt_status']==1
 assert abs(r['launch_skew_us']-(max(r['send_begin_ns'])-min(r['send_begin_ns']))/1000)<1e-8
 for j,name in enumerate(names):
  l=logs[name][n];assert l['hash']==candidates['rounds'][n]['transactions'][j]['hash'];assert l['send_begin_ns']==r['send_begin_ns'][j]
wins=collections.Counter(r['winner'] for r in rows);gas=sum(r['gas_used']*r['effective_gas_price'] for r in rows)/1e18
report={'rounds':100,'wins':dict(wins),'node1_total':sum(wins[x] for x in names[:2]),'direct_total':sum(wins[x] for x in names[2:]),'launch_skew_us_median':statistics.median(r['launch_skew_us'] for r in rows),'launch_skew_us_max':max(r['launch_skew_us'] for r in rows),'flagged_skew_rounds':sum(r['launch_skew_over_100us'] for r in rows),'actual_gas_eth':gas,'response_counts':responses}
(P/'analysis.json').write_text(json.dumps(report,indent=2))
lines=['# Landing Race Results','','100 rounds; five distinct signed candidates per round, sharing sender and nonce. Each candidate was submitted through exactly one ingress path.','', '| Path | Winning transactions | Win rate |','|---|---:|---:|']
for name in names:lines.append(f'| {name} | {wins[name]} | {wins[name]}% |')
lines+=['',f"Node1 combined: **{report['node1_total']}%** (two ingress paths). Direct combined: **{report['direct_total']}%** (three ingress paths).",'',f"Actual transaction fees: **{gas:.9f} ETH**.",'','All 500 candidates were prebuilt and signed before the experiment. Equivalent zero-value self-transfers used identical gas settings and equal-length nonzero test markers. Variant assignment to paths was randomized each round. Five persistent TLS connections were warmed before a 30-second busy-spin warm-up; senders continued spinning between rounds. The next nonce was released only after the prior winner was observed.','',f"Measured TLS-send invocation skew: median **{report['launch_skew_us_median']:.3f} µs**, maximum **{report['launch_skew_us_max']:.3f} µs**. Rounds above 100 µs: **{report['flagged_skew_rounds']}**. These are application timestamps, not NIC transmission timestamps.",'','Winners were identified by the successfully included transaction hash and checked against the locally signed candidate mapping. HTTP acknowledgements were not used to select winners. All 100 rounds remain included.','', 'This is a same-nonce competing-transaction experiment. A winning hash identifies the successful submission path under these conditions; replacement/admission behavior can affect outcomes, so it is not a direct measurement of propagation latency. Node1 may fan out internally. Results describe this test origin and time window, not all locations or an ongoing guarantee.','', '## Submission responses','']
for name in names:lines.append(f'- {name}: '+json.dumps(responses[name]))
(P/'report.md').write_text('\n'.join(lines)+'\n')
(P/'SHA256SUMS').write_text('\n'.join(hashlib.sha256(f.read_bytes()).hexdigest()+'  '+f.name for f in sorted(P.iterdir()) if f.is_file() and f.name!='SHA256SUMS')+'\n')
print(json.dumps(report,indent=2))
