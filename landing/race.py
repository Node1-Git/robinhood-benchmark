import os,time,json,pathlib,socket,ssl,multiprocessing as mp,http.client,traceback
P=pathlib.Path(os.environ.get('LANDING_DATA_DIR','./data/landing')).resolve()
NAMES=['node1-2b','node1-2c','official-1','official-2','official-3']
HOSTS=['robinhood-ohio-2b.node1.me','robinhood-ohio-2c.node1.me']+['sequencer.mainnet.chain.robinhood.com']*3
IPS=[None,None]+os.environ.get('SEQUENCER_IPS','').split(',')
HOSTS[:2]=[os.environ.get('NODE1_2B_HOST',HOSTS[0]),os.environ.get('NODE1_2C_HOST',HOSTS[1])]

def request(host,method,params,uuid=None):
 body=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params},separators=(',',':')).encode()
 headers=f'POST / HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nConnection: keep-alive\r\nContent-Length: {len(body)}\r\n'
 if uuid:headers+='Authorization: Bearer '+uuid+'\r\n'
 return (headers+'\r\n').encode()+body

def parse_response(data):
 split=data.find(b'\r\n\r\n')
 if split<0:return None
 head=data[:split].decode('latin1');status=int(head.split()[1]);headers={}
 for l in head.split('\r\n')[1:]:
  k,v=l.split(':',1);headers[k.lower()]=v.strip().lower()
 body=data[split+4:]
 if 'content-length' in headers:
  n=int(headers['content-length'])
  if len(body)<n:return None
  body=body[:n]
 elif 'chunked' in headers.get('transfer-encoding',''):
  chunks=[];pos=0
  while True:
   e=body.find(b'\r\n',pos)
   if e<0:return None
   n=int(body[pos:e].split(b';')[0],16);pos=e+2
   if len(body)<pos+n+2:return None
   if n==0:break
   chunks.append(body[pos:pos+n]);pos+=n+2
  body=b''.join(chunks)
 else:raise RuntimeError('response missing framing')
 try:d=json.loads(body)
 except ValueError:d={'non_json':True}
 # Only bounded JSON-RPC result/error objects; never retain request headers.
 return {'http':status,'rpc':d,'close':headers.get('connection')=='close'}

def connect(i,uuid):
 host=HOSTS[i];ip=IPS[i] or socket.gethostbyname(host)
 raw=socket.create_connection((ip,443),timeout=10);raw.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
 s=ssl.create_default_context().wrap_socket(raw,server_hostname=host)
 s.sendall(request(host,'eth_chainId',[],uuid if i<2 else None));data=b''
 while not (r:=parse_response(data)):
  b=s.recv(65536)
  if not b:raise RuntimeError('warmup EOF')
  data+=b
 if r['close']:raise RuntimeError('server closes warmup connection')
 s.setblocking(False);return s,ip

def worker(i,txs,uuid,round_index,target_ns,states,stop,send_begin,send_end):
 os.sched_setaffinity(0,{i});requests=[request(HOSTS[i],'robinhood_send_raw_transaction' if i<2 else 'eth_sendRawTransaction',[r['transactions'][i]['raw']],uuid if i<2 else None) for r in txs['rounds']]
 logs=[];s=None;last=-1
 try:
  s,ip=connect(i,uuid);states[i]=1
  while not stop.value:
   n=round_index.value
   if n<0 or n==last:continue
   # Wait by spinning; all candidates and complete HTTP requests already in memory.
   while not stop.value and time.monotonic_ns()<target_ns.value:pass
   if stop.value:break
   packet=requests[n];offset=0;send_begin[i]=time.monotonic_ns();deadline=send_begin[i]+15_000_000_000
   while offset<len(packet):
    if time.monotonic_ns()>deadline:raise TimeoutError('send timeout')
    try:offset+=s.send(packet[offset:])
    except (ssl.SSLWantReadError,ssl.SSLWantWriteError,BlockingIOError):pass
   send_end[i]=time.monotonic_ns();states[i]=2;data=b'';r=None
   while time.monotonic_ns()<deadline and not stop.value:
    try:
     b=s.recv(65536)
     if not b:raise RuntimeError('response EOF')
     data+=b;r=parse_response(data)
     if r:break
    except (ssl.SSLWantReadError,ssl.SSLWantWriteError,BlockingIOError):pass
   if r is None:raise TimeoutError('response timeout')
   logs.append({'round':n+1,'path':NAMES[i],'peer':ip,'hash':txs['rounds'][n]['transactions'][i]['hash'],'send_begin_ns':send_begin[i],'send_end_ns':send_end[i],'response_ns':time.monotonic_ns(),'response':r})
   if r['close']:s.close();s,ip=connect(i,uuid)
   last=n;states[i]=3
 except Exception as e:
  logs.append({'error_type':type(e).__name__,'error':'details omitted to protect configuration','round':round_index.value+1});states[i]=-1
 finally:
  if s:s.close()
  (P/(NAMES[i]+'.json')).write_text(json.dumps(logs,indent=2))

class RPC:
 def __init__(self):self.conn=None
 def call(self,method,params):
  if not self.conn:self.conn=http.client.HTTPSConnection('rpc.mainnet.chain.robinhood.com',timeout=10)
  try:
   self.conn.request('POST','/',json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}),{'Content-Type':'application/json'});r=self.conn.getresponse();d=json.loads(r.read())
   if 'error' in d:raise RuntimeError('read RPC error '+str(d['error']))
   return d['result']
  except Exception:
   self.conn.close();self.conn=None;raise

def main():
 os.sched_setaffinity(0,{5});os.umask(0o077)
 assert len(IPS)==5 and all(IPS[2:]),'Set SEQUENCER_IPS to three comma-separated public sequencer IPs'
 P.mkdir(parents=True,exist_ok=True)
 import fcntl
 lock=(P/'run.lock').open('w');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 assert not (P/'started.json').exists(),'Already started; never replay automatically'
 txs=json.loads((P/'transactions.json').read_text());uuid=pathlib.Path(os.environ['NODE1_UUID_FILE']).read_text().strip();rpc=RPC()
 assert int(rpc.call('eth_chainId',[]),16)==4663
 for tag in ['latest','pending']:assert int(rpc.call('eth_getTransactionCount',[txs['address'],tag]),16)==txs['initial_nonce'],'Nonce changed; rebuild before any send'
 states=mp.Array('i',5,lock=False);beg=mp.Array('q',5,lock=False);end=mp.Array('q',5,lock=False);idx=mp.Value('i',-1,lock=False);target=mp.Value('q',0,lock=False);stop=mp.Value('i',0,lock=False)
 procs=[mp.Process(target=worker,args=(i,txs,uuid if i<2 else None,idx,target,states,stop,beg,end)) for i in range(5)]
 results=[];failure=None
 try:
  for proc in procs:proc.start()
  deadline=time.monotonic()+35
  while not all(s==1 for s in states):
   assert -1 not in states,'connection warmup failed'
   assert time.monotonic()<deadline,'warmup ready timeout'
   time.sleep(.01)
  print('Five TLS connections warmed. Spinning for 30 seconds.',flush=True);time.sleep(30)
  (P/'started.json').write_text(json.dumps({'started_utc':time.time(),'rounds':100,'warmup_seconds':30,'max_gas_cost_wei':txs['max_gas_cost_wei']}))
  for n,row in enumerate(txs['rounds']):
   assert -1 not in states,'worker failed'
   target.value=time.monotonic_ns()+200_000_000
   for i in range(5):states[i]=1
   idx.value=n
   deadline=time.monotonic()+45;receipt=None;winner=None
   while time.monotonic()<deadline:
    for i,t in enumerate(row['transactions']):
     receipt=rpc.call('eth_getTransactionReceipt',[t['hash']])
     if receipt is not None:winner=i;break
    if receipt is not None:break
    time.sleep(.1)
   assert receipt is not None,'No candidate included; stop without retrying nonce'
   assert int(receipt['status'],16)==1,'included transaction reverted'
   ackdeadline=time.monotonic()+16
   while not all(s==3 for s in states):
    if -1 in states or time.monotonic()>ackdeadline:raise RuntimeError('worker response incomplete; stop')
    time.sleep(.001)
   skew=(max(beg)-min(beg))/1e3
   record={'round':n+1,'nonce':row['nonce'],'winner':NAMES[winner],'hash':receipt['transactionHash'],'block_number':int(receipt['blockNumber'],16),'transaction_index':int(receipt['transactionIndex'],16),'receipt_status':int(receipt['status'],16),'gas_used':int(receipt['gasUsed'],16),'effective_gas_price':int(receipt.get('effectiveGasPrice','0x0'),16),'target_ns':target.value,'send_begin_ns':list(beg),'send_end_ns':list(end),'launch_skew_us':skew,'launch_skew_over_100us':skew>100}
   results.append(record);(P/'results.json').write_text(json.dumps(results,indent=2));print(json.dumps({'round':n+1,'winner':NAMES[winner],'launch_skew_us':round(skew,2)}),flush=True)
   # Keep sender workers spinning between rounds; only coordinator performs reads.
   if n==9:print('First ten rounds completed; continuing same prebuilt experiment.',flush=True)
 except Exception as e:failure=type(e).__name__+': benchmark stopped; inspect local status';print(failure,flush=True)
 finally:
  stop.value=1
  for proc in procs:
   if proc.pid:proc.join(20)
   if proc.is_alive():proc.terminate();proc.join()
  summary={'completed':len(results)==100,'rounds':len(results),'failure':failure,'wins':{name:sum(r['winner']==name for r in results) for name in NAMES},'skew_over_100us_rounds':sum(r['launch_skew_over_100us'] for r in results)}
  (P/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary),flush=True)
 if failure:raise SystemExit(1)
if __name__=='__main__':main()
