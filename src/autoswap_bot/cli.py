from __future__ import annotations

import argparse
import asyncio
import signal
import sys
import threading
from pathlib import Path

from .bot import AutoswapBot, configure_logging, summarize_results
from .config import load_config
from .env_loader import load_dotenv_file


STARTUP_MODE_CHOICES = {
    "1": "free_only",
    "2": "free_then_swap",
    "3": "swap_only",
    "4": "planned_fee",
}


def _default_startup_mode() -> str:
    return "free_then_swap"


def _prompt_startup_mode() -> str:
    if not sys.stdin.isatty():
        return _default_startup_mode()

    print("Pilih mode bot:", flush=True)
    print("1. Mode hanya ambil free swap", flush=True)
    print(
        "2. Mode ambil free swap lalu lanjut swap sesuai batas swap dan fee swap yang ditentukan",
        flush=True,
    )
    print("3. Mode swap sesuai batas swap dan fee swap yang ditentukan", flush=True)
    print("4. Mode swap sesuai jam plan dan batas fee yang ditentukan", flush=True)

    while True:
        print("Masukkan pilihan (1/2/3/4): ", end="", flush=True)
        try:
            answer = input().strip()
        except EOFError:
            print("", flush=True)
            return _default_startup_mode()
        startup_mode = STARTUP_MODE_CHOICES.get(answer)
        if startup_mode is not None:
            return startup_mode
        print("Pilihan tidak valid.", flush=True)

class InterruptController:
    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.bot: AutoswapBot | None = None
        self._prompt_active = False
        self._prompt_lock = threading.Lock()

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop

    def attach_bot(self, bot: AutoswapBot) -> None:
        self.bot = bot

    def handle_sigint(self) -> None:
        if self.loop is None or self.loop.is_closed():
            return
        with self._prompt_lock:
            if self._prompt_active:
                return
            self._prompt_active = True
        prompt_thread = threading.Thread(
            target=self._confirm_stop_blocking,
            name="interrupt-confirmation",
            daemon=True,
        )
        prompt_thread.start()

    def _confirm_stop_blocking(self) -> None:
        try:
            if self.bot is not None:
                self.bot.monitor.set_terminal_dashboard_paused(True)
            print("\nberhenti? (y/n) ", end="", flush=True)
            answer = input().strip().lower()
            if answer == "y" and self.bot is not None and self.loop is not None and not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.loop.create_task, self.bot.request_stop())
            elif answer == "n":
                print("lanjut.\n", end="", flush=True)
        except (EOFError, KeyboardInterrupt):
            return
        finally:
            if self.bot is not None:
                self.bot.monitor.set_terminal_dashboard_paused(False)
            with self._prompt_lock:
                self._prompt_active = False


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
    startup_mode = _prompt_startup_mode()
    configure_logging(
        config.runtime.log_level,
        use_utc=True,
        terminal_dashboard_enabled=config.runtime.terminal_dashboard_enabled,
    )
    bot = AutoswapBot(config, repo_root=repo_root, startup_mode=startup_mode)
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
