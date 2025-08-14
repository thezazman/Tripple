import re, hashlib, json, logging
def canonical_text(s:str)->str: return re.sub(r"\s+"," ",(s or '').strip().lower())
def compute_hash(t:dict, algo:str='sha256')->str:
    body=f"{canonical_text(t.get('subject',''))}|{canonical_text(t.get('predicate',''))}|{canonical_text(t.get('object',''))}|{canonical_text(t.get('domain',''))}"
    h=hashlib.new(algo); h.update(body.encode()); return f"{algo}:{h.hexdigest()}"
def clamp_conf(x): 
    try: return max(0.0, min(1.0, float(x)))
    except Exception: return 0.0
def jsonl_iter(path):
    with open(path,'r',encoding='utf-8') as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: yield json.loads(line)
            except Exception as e: logging.error('Bad JSONL in %s: %s', path, e)
