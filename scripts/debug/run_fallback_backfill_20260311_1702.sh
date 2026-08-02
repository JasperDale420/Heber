#!/bin/zsh
set -euo pipefail
src=/Users/jacobmcmillan/Empire/Heber/.local_heber_data/data.backfill-20260311-164326
log=/Users/jacobmcmillan/Empire/Heber/logs/fallback-backfill-20260311-1702.log
{
  echo "start $(date -Iseconds) src=$src dst=/Volumes/heber/data"
  (cd "$src" && tar -cf - .) | (cd /Volumes/heber/data && tar -xpf -)
  echo "verify_start $(date -Iseconds)"
  python3 - <<'PY'
from pathlib import Path
src = Path('/Users/jacobmcmillan/Empire/Heber/.local_heber_data/data.backfill-20260311-164326')
dst = Path('/Volumes/heber/data')
missing = []
count = 0
for path in src.rglob('*'):
    if not path.is_file():
        continue
    count += 1
    rel = path.relative_to(src)
    if not (dst / rel).exists():
        missing.append(str(rel))
        if len(missing) >= 20:
            break
print(f'verify_count={count}')
print(f'missing_count={len(missing)}')
for item in missing:
    print(f'missing_sample={item}')
PY
  echo "done $(date -Iseconds)"
} >> "$log" 2>&1
