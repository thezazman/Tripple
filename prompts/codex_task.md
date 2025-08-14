# Codex Triplet Builder — Massive Catalog

**Contract**
- Read `controls/domains/*.json` → that’s your category list (hundreds).
- For each category, emit ≥ ${PER_DOMAIN:-10} high-quality triplets.
- Use only predicates allowed by that category’s control file.
- Include `evidence[]` and a `source` (URL or identifier); avoid PII/secret content.
- Output **NDJSON** (one triplet per line). Leave `created_at`/`hash` blank if unsure.

**Postcondition**
Pipe stdout to:
```bash
meshtriplets --repo . ingest --stdin
```
