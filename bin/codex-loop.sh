#!/usr/bin/env bash
set -euo pipefail
REPO="${1:-.}"
PROMPT="${2:-prompts/codex_task.md}"
if ! command -v codex >/dev/null 2>&1; then echo 'codex CLI not found' >&2; exit 127; fi
codex run --repo "$REPO" --prompt "$PROMPT" | meshtriplets --repo "$REPO" ingest --stdin
