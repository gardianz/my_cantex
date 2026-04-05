from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

for candidate in (ROOT / "src", ROOT / "cantex_sdk-0.3.0" / "src"):
    candidate_str = str(candidate)
    if candidate.exists() and candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

from autoswap_bot.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
