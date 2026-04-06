from __future__ import annotations

import argparse
import asyncio
import signal
from pathlib import Path

from .bot import AutoswapBot, configure_logging, summarize_results
from .config import load_config
from .env_loader import load_dotenv_file

class InterruptController:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.bot: AutoswapBot | None = None
        self._prompt_task: asyncio.Task[None] | None = None

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    def attach_bot(self, bot: AutoswapBot) -> None:
        self.bot = bot

    def handle_sigint(self) -> None:
        if self.loop is None or self.loop.is_closed():
            return
        if self._prompt_task is not None and not self._prompt_task.done():
            return
        self._prompt_task = self.loop.create_task(self._confirm_stop())

    async def _confirm_stop(self) -> None:
        answer = await asyncio.to_thread(input, "\nberhenti? (y/n) ")
        if answer.strip().lower() == "y" and self.bot is not None:
            await self.bot.request_stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cantex autoswap bot")
    parser.add_argument(
        "--config",
        default="config/accounts.toml",
        help="Path ke file konfigurasi TOML",
    )
    return parser


async def _run(config_path: str, interrupt_controller: InterruptController) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv_file(repo_root / ".env")
    config = load_config(config_path)
    configure_logging(
        config.runtime.log_level,
        use_utc=config.runtime.full_24h_mode,
    )
    bot = AutoswapBot(config, repo_root=repo_root)
    interrupt_controller.attach_bot(bot)
    results = await bot.run()
    print(summarize_results(results))
    return 0 if all(result.ok or result.aborted for result in results) else 1


def main() -> int:
    args = build_parser().parse_args()
    interrupt_controller = InterruptController()
    loop = asyncio.new_event_loop()
    interrupt_controller.attach_loop(loop)
    previous_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, lambda *_: interrupt_controller.handle_sigint())
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(_run(args.config, interrupt_controller))
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
