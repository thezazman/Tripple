import json, pathlib, logging
from typing import Dict, Any, List, Tuple
from .util import compute_hash, jsonl_iter
class TripletStore:
    def __init__(self, root:pathlib.Path):
        self.root=root; self.data_dir=root/'data'; self.idx_path=self.data_dir/'_index.json'; self._index={}
        if self.idx_path.exists():
            try: self._index=json.loads(self.idx_path.read_text(encoding='utf-8'))
            except Exception as e: logging.error('index load: %s', e); self._index={}
    def _save_index(self):
        tmp=self.idx_path.with_suffix('.tmp'); tmp.write_text(json.dumps(self._index,ensure_ascii=False,indent=2),encoding='utf-8'); tmp.replace(self.idx_path)
    def domain_file(self, dom:str)->pathlib.Path: return self.data_dir/f"{dom}.jsonl"
    def add(self, trips:List[Dict[str,Any]])->Tuple[int,int]:
        added=0; skipped=0; writers={}
        try:
            for t in trips:
                h=t.get('hash') or compute_hash(t); dom=t.get('domain')
                if not dom: skipped+=1; continue
                if h in self._index: skipped+=1; continue
                fp=writers.get(dom)
                if fp is None: fp=open(self.domain_file(dom),'a',encoding='utf-8'); writers[dom]=fp
                fp.write(json.dumps(t, ensure_ascii=False)+'\n'); self._index[h]=dom; added+=1
            return added, skipped
        finally:
            for fp in writers.values(): fp.close()
            self._save_index()
    def iter_domain(self, dom:str):
        p=self.domain_file(dom); return jsonl_iter(p) if p.exists() else []
    def stats(self)->Dict[str,Any]:
        out={'total':0,'by_domain':{}}
        for p in self.data_dir.glob('*.jsonl'):
            cnt=sum(1 for _ in jsonl_iter(p)); out['by_domain'][p.stem]=cnt; out['total']+=cnt
        return out
    def rewrite_domain(self, dom:str, items:List[Dict[str,Any]]):
        p=self.domain_file(dom); tmp=p.with_suffix('.tmp')
        with open(tmp,'w',encoding='utf-8') as f:
            for t in items:
                if not t.get('hash'): t['hash']=compute_hash(t)
                f.write(json.dumps(t, ensure_ascii=False)+'\n')
        tmp.replace(p)
        for h,d in list(self._index.items()):
            if d==dom: del self._index[h]
        for t in jsonl_iter(p): self._index[t['hash']]=dom
        self._save_index()
    def dedup(self)->int:
        removed=0; seen=set()
        for p in list(self.data_dir.glob('*.jsonl')):
            if p.name=='_index.json': continue
            uniq=[]
            for t in jsonl_iter(p):
                h=t.get('hash') or compute_hash(t); t['hash']=h
                if h in seen: removed+=1; continue
                seen.add(h); uniq.append(t)
            tmp=p.with_suffix('.tmp')
            with open(tmp,'w',encoding='utf-8') as f:
                for t in uniq: f.write(json.dumps(t, ensure_ascii=False)+'\n')
            tmp.replace(p)
        self._index={}
        for p in self.data_dir.glob('*.jsonl'):
            for t in jsonl_iter(p): self._index[t['hash']]=p.stem
        self._save_index(); return removed
