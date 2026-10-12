"""Wait for a condition the code under test brings about, instead of sleeping and hoping it has.

A unit test that sleeps a fixed 50 ms for a server to register a waiter or release a partition measures
the machine it runs on: fast enough on a development box, too short on a small CI runner (2026-09-28).
`eventually` polls the condition until it holds, so the test waits as long as the work takes and no
longer, and the generous deadline is only reached when the code is broken.
"""
from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable

DEADLINE_SECONDS = 10.0


async def eventually(condition: Callable[[], object | Awaitable[object]], message: str,
                     seconds: float = DEADLINE_SECONDS) -> None:
    """Return once `condition()` is truthy; fail with `message` if it is not within `seconds`."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        assert loop.time() < deadline, message
        await asyncio.sleep(0.01)
