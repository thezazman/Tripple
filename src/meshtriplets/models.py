from typing import Dict, Any, Tuple
import datetime as _dt, re
from .util import compute_hash, clamp_conf
def validate_triplet(t:Dict[str,Any], controls:Dict[str,Any], domcfg:Dict[str,Any])->Tuple[bool,list]:
    errs=[]
    for k in controls.get('required_fields',[]): 
        if k not in t: errs.append(f'missing field: {k}')
    for k in ('subject','predicate','object','domain','source','license','method'):
        v=t.get(k,'')
        if not isinstance(v,str): errs.append(f'{k} must be str')
        else:
            if len(v.strip())<controls.get('min_text_len',1): errs.append(f'{k} too short')
            if len(v)>controls.get('max_text_len',220): errs.append(f'{k} too long')
    if not isinstance(t.get('evidence',[]), list): errs.append('evidence must be list[str]')
    if clamp_conf(t.get('confidence',0.0))<controls.get('min_confidence',0.6): errs.append('confidence below min')
    allowed = domcfg.get('allowed_predicates') if controls.get('enforce_domain_predicates',True) else controls.get('allowed_predicates',[])
    if allowed and t.get('predicate') not in allowed: errs.append('predicate not allowed for domain')
    try: _=_dt.datetime.fromisoformat(t.get('created_at','').replace('Z','+00:00'))
    except Exception: errs.append('created_at not ISO8601')
    expected = compute_hash(t, controls.get('hash_algo','sha256'))
    if not t.get('hash'): t['hash']=expected
    elif t['hash']!=expected: errs.append('hash mismatch vs canonical content')
    return (len(errs)==0, errs)
