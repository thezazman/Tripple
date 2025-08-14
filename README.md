# MeshTriplets Codex Catalog (v1.0.0)

Massive, precise category set for Codex-driven triplet generation. Each **category = one file** in `data/` and one control in `controls/domains/`.

## TL;DR
```bash
pip install -e .
meshtriplets --repo . list-domains | head -n 50
bin/codex-loop.sh . prompts/codex_task.md   # requires codex CLI
meshtriplets --repo . normalize && meshtriplets --repo . validate && meshtriplets --repo . lint
meshtriplets --repo . stats
```
