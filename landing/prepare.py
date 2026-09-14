import json,pathlib,random,requests,os,getpass
from eth_account import Account
P=pathlib.Path(os.environ.get('LANDING_DATA_DIR','./data/landing')).resolve();session=requests.Session()
def rpc(method,params):
 r=session.post('https://rpc.mainnet.chain.robinhood.com',json={'jsonrpc':'2.0','id':1,'method':method,'params':params},timeout=25);r.raise_for_status();d=r.json()
 if 'error' in d:raise RuntimeError(str(d['error']))
 return d['result']
if __name__=='__main__':
 os.umask(0o077);P.mkdir(parents=True,exist_ok=True)
 assert not (P/'transactions.json').exists() and not (P/'started.json').exists(),'Use a fresh data directory; never overwrite a prepared or started run'
 a=Account.from_key(getpass.getpass('Wallet private key (hidden, not saved): '));chain=int(rpc('eth_chainId',[]),16);assert chain==4663
 latest=int(rpc('eth_getTransactionCount',[a.address,'latest']),16);pending=int(rpc('eth_getTransactionCount',[a.address,'pending']),16);assert latest==pending,'Pending wallet transactions; stop'
 balance=int(rpc('eth_getBalance',[a.address,'latest']),16);assert rpc('eth_getCode',[a.address,'latest'])=='0x','Wallet has code; stop'
 price=int(rpc('eth_gasPrice',[]),16);fee=price*3
 gas=int(rpc('eth_estimateGas',[{'from':a.address,'to':a.address,'value':'0x0','data':'0x01010101'}]),16)*2
 maxcost=100*gas*fee;assert balance>maxcost,'Insufficient balance';assert maxcost<=10**15,'Estimated maximum exceeds 0.001 ETH; stop for review'
 rng=random.Random(20260914);rounds=[]
 for i in range(100):
  variants=[]
  for j in range(5):
   tag=bytes([1+i//255,1+i%255,1+j,1]).hex()
   tx={'chainId':chain,'nonce':latest+i,'gas':gas,'gasPrice':fee,'to':a.address,'value':0,'data':'0x'+tag}
   signed=a.sign_transaction(tx);variants.append({'raw':'0x'+signed.raw_transaction.hex(),'hash':'0x'+signed.hash.hex()})
  rng.shuffle(variants);rounds.append({'nonce':latest+i,'transactions':variants})
 config={'address':a.address,'chain_id':chain,'initial_nonce':latest,'gas':gas,'gas_price':fee,'max_gas_cost_wei':maxcost,'rounds':rounds}
 (P/'transactions.json').write_text(json.dumps(config));print(json.dumps({'address':a.address,'nonce':latest,'rounds':100,'signed_candidates':500,'balance_eth':balance/1e18,'max_gas_cost_eth':maxcost/1e18}))
