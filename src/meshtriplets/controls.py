import json, pathlib
def load_global_controls(root:pathlib.Path)->dict: return json.loads((root/'controls/global.json').read_text(encoding='utf-8'))
def load_domain_controls(root:pathlib.Path, domain:str)->dict: return json.loads((root/f'controls/domains/{domain}.json').read_text(encoding='utf-8'))
def list_domains(root:pathlib.Path)->dict:
    out={}
    for p in sorted((root/'controls/domains').glob('*.json')): out[p.stem]=json.loads(p.read_text(encoding='utf-8'))
    return out
