import argparse, sys, json, pathlib, logging, datetime as dt, os, re
from .controls import load_global_controls, load_domain_controls, list_domains
from .store import TripletStore
from .models import validate_triplet
from .extractor import extract_rule_based
from .util import compute_hash, canonical_text
def resolve_root(v): 
    return pathlib.Path(v).expanduser().resolve() if v else pathlib.Path(os.environ.get("MESHTRIPLETS_REPO", pathlib.Path.cwd()))
def _canonical_predicate(p, allowed):
    if not p: return p
    low=p.strip().lower()
    for a in allowed:
        if a.lower()==low: return a
    return p[:1].upper()+p[1:].lower()
def cmd_list_domains(a):
    root=resolve_root(a.repo); ds=list_domains(root)
    for name,cfg in ds.items(): print(f"{name:40} - {cfg.get('description','')}")
def _load_stdin():
    data=sys.stdin.read().strip()
    if not data: return []
    out=[]
    for line in data.splitlines():
        line=line.strip()
        if not line: continue
        try: out.append(json.loads(line))
        except Exception: pass
    if out: return out
    try:
        obj=json.loads(data); return obj if isinstance(obj,list) else [obj]
    except Exception: return []
def cmd_add(a):
    root=resolve_root(a.repo); controls=load_global_controls(root); domcfg=load_domain_controls(root, a.domain)
    store=TripletStore(root)
    trips=_load_stdin() if a.stdin else json.loads(pathlib.Path(a.file).read_text(encoding='utf-8'))
    if isinstance(trips, dict): trips=[trips]
    valid=[]
    for t in trips:
        t['domain']=a.domain or t.get('domain')
        if not t.get('created_at'): t['created_at']=dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
        if not t.get('hash'): t['hash']=compute_hash(t, controls.get('hash_algo','sha256'))
        ok,errs=validate_triplet(t,controls,domcfg)
        if ok: valid.append(t)
    added,skipped=store.add(valid)
    print(json.dumps({"added":added,"skipped":skipped,"submitted":len(trips)}))
def cmd_ingest(a):
    root=resolve_root(a.repo); controls=load_global_controls(root); store=TripletStore(root)
    data=pathlib.Path(a.file).read_text(encoding='utf-8') if a.file else sys.stdin.read().strip()
    trips=[]; bad=0
    for line in data.splitlines():
        line=line.strip()
        if not line: continue
        try: trips.append(json.loads(line))
        except Exception: bad+=1
    if not trips:
        try:
            obj=json.loads(data); trips=obj if isinstance(obj,list) else [obj]
        except Exception as e: print(json.dumps({"error":"failed to parse input","detail":str(e)})); return
    domcache={}; valid=[]; rej=0
    for t in trips:
        dom=(t.get('domain') or '').strip()
        if not dom: rej+=1; continue
        if dom not in domcache:
            try: domcache[dom]=load_domain_controls(root,dom)
            except Exception: rej+=1; continue
        if not t.get('created_at'): t['created_at']=dt.datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
        if not t.get('hash'): t['hash']=compute_hash(t, controls.get('hash_algo','sha256'))
        ok,errs=validate_triplet(t,controls,domcache[dom])
        if ok: valid.append(t)
        else: rej+=1
    added,skipped=store.add(valid)
    print(json.dumps({"submitted":len(trips),"added":added,"skipped":skipped,"rejected":rej,"bad_lines":bad}))
def cmd_extract(a):
    root=resolve_root(a.repo); controls=load_global_controls(root); domcfg=load_domain_controls(root, a.domain); store=TripletStore(root)
    text=pathlib.Path(a.infile).read_text(encoding='utf-8') if a.infile else sys.stdin.read()
    trips=extract_rule_based(text, default_domain=a.domain, source=a.source, min_confidence=a.min_confidence)
    valid=[]
    for t in trips:
        if not t.get('hash'): t['hash']=compute_hash(t, controls.get('hash_algo','sha256'))
        ok,errs=validate_triplet(t,controls,domcfg)
        if ok: valid.append(t)
    added,skipped=store.add(valid)
    print(json.dumps({"extracted":len(trips),"added":added,"skipped":skipped}))
def cmd_normalize(a):
    root=resolve_root(a.repo); controls=load_global_controls(root); store=TripletStore(root)
    changed=0
    for dom,cfg in list_domains(root).items():
        allowed=cfg.get('allowed_predicates',[]); items=[]
        for t in store.iter_domain(dom):
            t['subject']=canonical_text(t.get('subject'))
            t['object']=canonical_text(t.get('object'))
            t['predicate']=_canonical_predicate(t.get('predicate'), allowed)
            newh=compute_hash(t, controls.get('hash_algo','sha256'))
            if t.get('hash')!=newh: t['hash']=newh; changed+=1
            items.append(t)
        store.rewrite_domain(dom, items)
    print(json.dumps({"changed_hashes":changed}))
def cmd_validate(a):
    root=resolve_root(a.repo); controls=load_global_controls(root); ds=list_domains(root); store=TripletStore(root)
    total=0; bad=0
    for dom in ds.keys():
        domcfg=load_domain_controls(root,dom)
        for t in store.iter_domain(dom):
            total+=1; ok,errs=validate_triplet(t,controls,domcfg)
            if not ok: bad+=1
    print(json.dumps({"checked":total,"invalid":bad}))
def cmd_policy_check(a):
    root=resolve_root(a.repo); policy=json.loads((root/'controls/policies/red_flags.json').read_text(encoding='utf-8'))
    pats=[re.compile(p, flags=re.I) for p in policy.get('pii_terms',[])]
    from .util import jsonl_iter
    matches=[]
    for p in (root/'data').glob('*.jsonl'):
        dom=p.stem
        for t in jsonl_iter(p):
            for fld in ('subject','object','evidence'):
                vals=t.get(fld,[]); vals=vals if isinstance(vals,list) else [vals]
                for s in vals:
                    s=s or ""
                    for pat in pats:
                        if pat.search(s): matches.append({'domain':dom,'hash':t.get('hash'),'field':fld,'pattern':pat.pattern})
    print(json.dumps({"matches":matches,"count":len(matches)}))
def cmd_stats(a):
    root=resolve_root(a.repo); store=TripletStore(root); print(json.dumps(store.stats(), indent=2))
def cmd_dedup(a):
    root=resolve_root(a.repo); store=TripletStore(root); print(json.dumps({"removed":store.dedup()}))
def cmd_export(a):
    root=resolve_root(a.repo); from .util import jsonl_iter
    out=[]
    for p in (root/'data').glob('*.jsonl'):
        for t in jsonl_iter(p): out.append(t)
    if a.format=='jsonl':
        if a.out=='-':
            for t in out: print(json.dumps(t, ensure_ascii=False))
        else:
            with open(a.out,'w',encoding='utf-8') as f:
                for t in out: f.write(json.dumps(t, ensure_ascii=False)+'\n')
    else:
        payload=json.dumps(out, ensure_ascii=False, indent=2)
        if a.out=='-': print(payload)
        else: pathlib.Path(a.out).write_text(payload, encoding='utf-8')
def cmd_lint(a):
    root=resolve_root(a.repo); controls=load_global_controls(root)
    preds=json.loads((root/'controls/predicates.json').read_text(encoding='utf-8')).get('predicates',{})
    from .util import jsonl_iter
    seen={}; problems=[]
    for p in (root/'data').glob('*.jsonl'):
        dom=p.stem
        for t in jsonl_iter(p):
            spok=(t.get('subject'), t.get('predicate'), t.get('object'))
            seen.setdefault(spok,set()).add(dom)
            if t.get('predicate') not in controls.get('allowed_predicates',[]) and t.get('predicate') not in preds:
                problems.append({'type':'unknown_predicate','triplet':t})
    by_so={}
    for (s,p,o),D in seen.items():
        by_so.setdefault((s,o), set()).add(p)
    for (s,o),P in by_so.items():
        if 'Supports' in P and 'Contradicts' in P:
            problems.append({'type':'supports_vs_contradicts','subject':s,'object':o})
    print(json.dumps({'problems':problems,'count':len(problems)}))
def cmd_add_domain(a):
    root=resolve_root(a.repo); p=root/f"controls/domains/{a.name}.json"
    if p.exists(): print(json.dumps({'error':'domain exists','domain':a.name})); return
    cfg={'description':a.description or '','allowed_predicates':a.predicates or []}
    p.write_text(json.dumps(cfg, indent=2), encoding='utf-8'); (root/f"data/{a.name}.jsonl").touch()
    print(json.dumps({'created':a.name,'file':str(p)}))
def main(argv=None):
    pa=argparse.ArgumentParser(prog='meshtriplets', description='Triplet seeding toolkit (Codex catalog).')
    pa.add_argument('--repo'); pa.add_argument('-v','--verbose', action='count', default=0)
    sub=pa.add_subparsers(dest='cmd', required=True)
    s=sub.add_parser('list-domains'); s.set_defaults(func=cmd_list_domains)
    s=sub.add_parser('add'); s.add_argument('--domain', required=True); g=s.add_mutually_exclusive_group(required=True); g.add_argument('--file'); g.add_argument('--stdin', action='store_true'); s.set_defaults(func=cmd_add)
    s=sub.add_parser('ingest'); g=s.add_mutually_exclusive_group(required=True); g.add_argument('--file'); g.add_argument('--stdin', action='store_true'); s.set_defaults(func=cmd_ingest)
    s=sub.add_parser('extract'); s.add_argument('--domain', required=True); s.add_argument('--in', dest='infile'); s.add_argument('--source', required=True); s.add_argument('--min-confidence', type=float, default=0.6); s.set_defaults(func=cmd_extract)
    s=sub.add_parser('normalize'); s.set_defaults(func=cmd_normalize)
    s=sub.add_parser('validate'); s.set_defaults(func=cmd_validate)
    s=sub.add_parser('policy-check'); s.set_defaults(func=cmd_policy_check)
    s=sub.add_parser('stats'); s.set_defaults(func=cmd_stats)
    s=sub.add_parser('dedup'); s.set_defaults(func=cmd_dedup)
    s=sub.add_parser('export'); s.add_argument('--format', choices=['jsonl','json'], default='jsonl'); s.add_argument('--out', default='-'); s.set_defaults(func=cmd_export)
    s=sub.add_parser('lint'); s.set_defaults(func=cmd_lint)
    s=sub.add_parser('add-domain'); s.add_argument('--name', required=True); s.add_argument('--description', default=''); s.add_argument('--predicates', nargs='*', default=[]); s.set_defaults(func=cmd_add_domain)
    args=pa.parse_args(argv); logging.basicConfig(level=logging.INFO); return args.func(args)
