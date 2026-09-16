"""DualStackFabric: broadcast to all radios concurrently."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from .fabric import RFFabric

logger = logging.getLogger(__name__)


class DualStackFabric(RFFabric):
    """RFFabric subclass that broadcasts on every registered radio."""

    async def send(self, data: bytes, *, radio_id: Optional[str] = None) -> Any:
        if radio_id is not None:
            return await super().send(data, radio_id=radio_id)

        if not self._radios:
            return None

        tasks = {}
        for rid, radio in self._radios.items():
            if not hasattr(radio, "send"):
                logger.warning("Radio %r does not support send()", rid)
                continue
            tasks[rid] = asyncio.create_task(radio.send(data))

        if not tasks:
            return None

        results: list[tuple[str, Any]] = []
        successes = []
        for rid, task in tasks.items():
            try:
                result = await task
                successes.append(rid)
                if isinstance(result, dict):
                    result.setdefault("radio_id", rid)
                results.append((rid, result))
            except Exception:
                logger.exception("TX failed on radio %r", rid)
                results.append((rid, None))

        if not successes:
            return None

        return {
            "ok": True,
            "radio_id": ",".join(successes),
            "radio_ids_count": len(successes),
            "results": results,
        }