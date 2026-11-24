import json
import hashlib

def parse_nitro(raw_data):
    try:
        o = json.loads(raw_data)
        messages = o.get('messages', [])
        parsed = []
        for m in messages:
            seq = int(m.get('sequenceNumber'))
            # Some feeds might provide txHash or blockHash inside the message
            block_hash = m.get('blockHash')
            # For Nitro, the identity is sequenceNumber, content is the whole message
            digest = hashlib.sha256(json.dumps(m, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            parsed.append({'sequence': seq, 'tx_hash': None, 'digest': digest, 'block_hash': block_hash, 'raw_msg': m})
        return parsed
    except Exception:
        return []

def parse_tx(raw_data):
    try:
        o = json.loads(raw_data)
        # Assuming tx-level feeds send a single tx or list of txs
        if isinstance(o, dict):
            messages = [o]
        elif isinstance(o, list):
            messages = o
        else:
            return []
            
        parsed = []
        for m in messages:
            tx_hash = m.get('hash') or m.get('txHash') or m.get('transactionHash')
            if not tx_hash: continue
            digest = hashlib.sha256(json.dumps(m, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            parsed.append({'sequence': None, 'tx_hash': tx_hash, 'digest': digest, 'block_hash': m.get('blockHash'), 'raw_msg': m})
        return parsed
    except Exception:
        return []
