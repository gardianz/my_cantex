from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .bot import AutoswapBot, configure_logging, summarize_results
from .config import load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cantex autoswap bot")
    parser.add_argument(
        "--config",
        default="config/accounts.toml",
        help="Path ke file konfigurasi TOML",
    )
    return parser


async def _run(config_path: str) -> int:
    config = load_config(config_path)
    configure_logging(
        config.runtime.log_level,
        use_utc=config.runtime.full_24h_mode,
    )
    repo_root = Path(__file__).resolve().parents[2]
    bot = AutoswapBot(config, repo_root=repo_root)
    results = await bot.run()
    print(summarize_results(results))
    return 0 if all(result.ok or result.aborted for result in results) else 1


def main() -> int:
    args = build_parser().parse_args()
    return asyncio.run(_run(args.config))
