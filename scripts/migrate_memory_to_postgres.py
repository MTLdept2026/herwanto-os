from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import google_services as gs


def main() -> int:
    result = gs.import_sheet_memory_to_postgres()
    memory = gs.get_memory()
    counts = {category: len(memory.get(category, [])) for category in gs.DEFAULT_MEMORY}
    print(json.dumps({"migration": result, "counts": counts}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
