import re, datetime as dt
from .util import canonical_text
_NEG=['is not','are not','isn\'t','aren\'t','never','no ']
_PAT=[(re.compile(r'(?P<s>[^.]+?)\s+is\s+(?:an?\s+)?(?P<o>[^.]+?)\.', re.I),'IsA',0.65),
      (re.compile(r'(?P<s>[^.]+?)\s+are\s+(?:an?\s+)?(?P<o>[^.]+?)\.', re.I),'IsA',0.65),
      (re.compile(r'(?P<s>[^.]+?)\s+uses\s+(?P<o>[^.]+?)\.', re.I),'Uses',0.62),
      (re.compile(r'(?P<s>[^.]+?)\s+causes\s+(?P<o>[^.]+?)\.', re.I),'Causes',0.62)]
def extract_rule_based(text:str, default_domain:str, source:str, min_confidence:float=0.6):
    now=dt.datetime.utcnow().replace(microsecond=0).isoformat()+'Z'; out=[]
    for pat,pred,conf in _PAT:
        for m in pat.finditer(text):
            if any(n in m.group(0).lower() for n in _NEG): continue
            s=canonical_text(m.group('s')); o=canonical_text(m.group('o'))
            if not s or not o: continue
            out.append({'subject':s,'predicate':pred,'object':o,'domain':default_domain,'confidence':conf,'source':source,'evidence':[m.group(0).strip()],'created_at':now,'license':'CC0','method':'rule'})
    return out
