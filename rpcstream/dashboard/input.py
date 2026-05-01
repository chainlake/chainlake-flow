from __future__ import annotations

import os
import select
import sys
import termios
import tty


def wait_for_exit_keypress() -> None:
    if not sys.stdin.isatty() or os.name != "posix":
        return

    fd = sys.stdin.fileno()
    original = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        while True:
            ready, _, _ = select.select([fd], [], [], 0.1)
            if not ready:
                continue
            chunk = os.read(fd, 1024)
            if not chunk:
                continue
            if b"\x03" in chunk:
                return
            if chunk == b"\x1b":
                return
            if chunk.startswith(b"\x1b"):
                while True:
                    ready, _, _ = select.select([fd], [], [], 0.02)
                    if not ready:
                        break
                    more = os.read(fd, 1024)
                    if not more:
                        break
    except KeyboardInterrupt:
        return
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, original)
