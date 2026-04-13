from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
for sdk_src in (
    PACKAGE_ROOT / "cantex_sdk-4.0" / "src",
    PACKAGE_ROOT / "cantex_sdk-0.3.0" / "src",
):
    sdk_src_str = str(sdk_src)
    if sdk_src.exists() and sdk_src_str not in sys.path:
        sys.path.insert(0, sdk_src_str)
