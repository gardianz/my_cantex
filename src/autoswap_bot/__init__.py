from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[2]
SDK_SRC = PACKAGE_ROOT / "cantex_sdk-0.3.0" / "src"

sdk_src_str = str(SDK_SRC)
if SDK_SRC.exists() and sdk_src_str not in sys.path:
    sys.path.insert(0, sdk_src_str)
