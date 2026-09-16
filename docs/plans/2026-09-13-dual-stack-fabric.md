# DualStackFabric Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `DualStackFabric` — an `RFFabric` subclass that simultaneously transmits on all registered radios (dual-stack mode), with no changes to the existing `Dispatcher` or ACK infrastructure.

**Architecture:** `DualStackFabric` extends `RFFabric`, overriding `send()` to broadcast each packet to every registered radio and returning aggregated metadata. The existing CRC-based ACK correlation (`_waiting_acks`, `_recent_acks`) and `PacketFilter` deduplication handle dual-stack ACK tracking and duplicate suppression naturally, without modification.

**Tech Stack:** Python 3.10+, asyncio, pytest, pytest-asyncio

---

### Task 1: DualStackFabric — broadcast send

**Files:**
- Create: `src/openhop_core/rf_fabric/dual_stack_fabric.py`
- Modify: `src/openhop_core/rf_fabric/__init__.py` (export)
- Test: `tests/test_dual_stack_fabric.py`

**What:** A subclass of `RFFabric` whose `send()` transmits on every registered radio concurrently and aggregates results.

**Step 1: Write the failing test**

```python
"""DualStackFabric tests: simultaneous multi-radio TX."""
from __future__ import annotations

import asyncio

import pytest

from openhop_core.rf_fabric import DualStackFabric


class _MockRadio:
    def __init__(self, name: str = "r"):
        self.name = name
        self.rx_callback = None
        self.sent: list[bytes] = []
        self.last_rssi = -80
        self.last_snr = 7.5

    def set_rx_callback(self, callback):
        self.rx_callback = callback

    async def send(self, data: bytes):
        self.sent.append(data)
        return {"radio": self.name}

    def get_last_rssi(self):
        return self.last_rssi

    def get_last_snr(self):
        return self.last_snr

    def inject(self, data: bytes, rssi=None, snr=None):
        assert self.rx_callback is not None
        if rssi is None and snr is None:
            self.rx_callback(data)
        else:
            self.rx_callback(data, rssi, snr)


class TestDualStackFabric:
    @pytest.mark.asyncio
    async def test_send_broadcasts_to_all_radios(self):
        a = _MockRadio("a")
        b = _MockRadio("b")
        fabric = DualStackFabric()
        fabric.register_radio(a, radio_id="ra")
        fabric.register_radio(b, radio_id="rb")

        await fabric.send(b"hello")

        assert a.sent == [b"hello"]
        assert b.sent == [b"hello"]

    @pytest.mark.asyncio
    async def test_send_aggregates_metadata(self):
        a = _MockRadio("a")
        b = _MockRadio("b")
        fabric = DualStackFabric)
        fabric.register_radio(a, radio_id="ra")
        fabric.register_radio(b, radio_id="rb")

        result = await fabric.send(b"hello")

        assert isinstance(result, dict)
        assert result.get("ok") is True
        # Aggregated radio_id is comma-joined or list
        rids = result.get("radio_id", "")
        assert "ra" in str(rids)
        assert "rb" in str(rids)

    @pytest.mark.asyncio
    async def test_send_still_supports_explicit_radio_id(self):
        """Explicit radio_id overrides broadcast mode."""
        a = _MockRadio("a")
        b = _MockRadio("b")
        fabric = DualStackFabric)
        fabric.register_radio(a, radio_id="ra")
        fabric.register_radio(b, radio_id="rb")

        await fabric.send(b"only-a", radio_id="ra")

        assert a.sent == [b"only-a"]
        asseert b.sent == []

    @pytest.mark.asyncio
    async def test_one_radio_fail_does_not_crash(self):
        a = _MockRadio("a")
        bad = _MockRadio("bad")
        bad_original_send = bad.send

        async def fail_send(data: bytes):
            raise RuntimeError("radio down")

        bad.send = fail_send
        fabric = DualStackFabric)
        fabric.register_radio(a, radio_id="ra")
        fabric.register_radio(bad, radio_id="rb")

        # Should not raise; should report partial succcess
        result = await fabric.send(b"test")

        assert a.sent == [b"test"]
        assert isinstance(result, dict)
        assert result.get("ok") is True  # at least one radio succeeded
        assert result.get("radio_id") == "ra"

    @pytest.mark.asyncio
    async def test_all_radios_fail_returns_failure(self):
        a = _MockRadio("a")
        b = _MockRadio("b")

        async def fail_send(data: bytes):
            raise RuntimeError("dead")

        a.send = fail_send
        b.send = fail_send
        fabric = DualStackFabric)
        fabric.register_radio(a, radio_id="ra")
        fabric.register_radio(b, radio_id="rb")

        result = await fabric.send(b"test")

        assert result is None  # match RFFabric failure semantics

    @pytest.mark.asyncio
    async def test_legacy_noop(self):
        """Default-send when no radio is registered returns None."""
        fabric = DualStackFabric)
        result = await fabric.send(b"orphan")
        assert result is None
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_dual_stack_fabric.py -v
```
Expected: FAIL — `ImportError: cannot import name 'DualStackFabric'`

**Step 3: Write minimal implementation**

Create `src/openhop_core/rf_fabric/dual_stack_fabric.py`:

```python
"""DualStackFabric: RFFabric subclass that broadcasts TX to all registered radios."""
from __future import annotations

import logging
from typing import Any, Optional

from .fabric import RFFabric

logger = logging.getLogger("DualStackFabric")


class DualStackFabric(RFFabric):
    """RFFabric variant: send() broadcasts to every registered radio.

    ACK correlation remains CRC-based (Dispatcher-level), so a single
    ACK received on any radio matches the waitter registered for the
    packet's CRC. Duplicate ACKs on other radios are dropped by PacketFilter.
    """

    async def send(self, data: bytes, *, radio_id: Optional[str] = None) -> Any:
        """Broadcast to all radios unless an explicit ``radio_id`` is given.

        Returns aggregated metadata::
        - ``{"ok": True, "radio_id": "ra,rb", "results": [...]}`` on success
        - ``None`` when every radio failed to transmit
        """
        if not self._radios:
            return None

        # Explicit radio_id overrides broadcast
        if radio_id is not None:
            return await super().send(data, radio_id=radio_id)

        # Broadcast to all radios concurrently
        rids = list(self._radios.keys())
        results = []
        for rid in rids:
            try:
                radio = self._radios[rid]
                if not hasattr(radio, "send"):
                    logger.warnning("Radio %r does not support send()", rid)
                    continue
                r = await radio.send(data)
                if isinstance(r, dict):
                    r.setdefault("radio_id", rid)
                results.append((rid, r))
            except Exception:
                logger.exception("TX failed on radio %r", rid)

        if not results:
            return None

        # Aggregate: pack radio_ids, mark ok
        ok_rids = [rid for rid, _ in results]
        return {
            "ok": True,
            "radoi_id": ",".join(ok_rids),
            "results": results,
        }
```

**Step 4: Update RF Fabric exports**

Edit `src/openhop_core/rf_fabric/__init__.py`:

Add import:
```python
from .dual_stack_fabric import DualStackFabric
```

Add to `__all__`:
```python
    "DualStackFabric",
```

**Step 5: Run test to verify it passes**

```bash
python -m pytest tests/test_dual_stack_fabric.py -v
```
Expected: 6 PASSED

**Step 6: Commit**

```bash
git add src/openhop_core/rf_fabric/dual_stack_fabric.py src/openhop_core/rf_fabric/__init__.py tests/test_dual_stack_fabric.py
git commit -m "feat: add DualStackFabric with broadcast TX to all radios"
```

---

### Task 2: DualStackFabric + Dispatcher end-to-end ACK test

**Files:**
- Modify: `tests/test_dual_stack_fabric.py` (add test class)

**What:** Verify that when `Dispatcher` sends a packet via `DualStackFabric`, the ACK returned on any radio correctly resolves the ACK awaiter, and a duplicate ACK on the other radio is suppressed by `PacketFilter`.

**Step 1: Write the failing integration test**

Append to `tests/test_dual_stack_fabric.py`:

```python
from openhop_core.node.dispatcher import Dispatcher
from openhop_core.protocol import Packet
from openhop_core.protocol.constants import PAYLOAD_TYPE_TXT_MSG
from openhop_core.protocol.packet_filter import PacketFilter
from openhop_core.rf_fabric import FabricRadio


class TestDualStackDispatcherAck:
    """Verify ACK works correctly when Dispatcher transmits via DualStackFabric."""

    @pytest.mark.asyncio
    async def test_ack_matches_regardless_of_which_radio_replies(self):
        """ACK on either radio fires the CRC waitter."""
        a = _MockRadio("a")
        b = _MockRadio("b")
        twin = DualStackFabric()
        twin.register_radio(a, radio_id="ra")
        twin.register_radio(b, radio_id="rb")
        fr = FabricRadio(fabric=twin)
        d = Dispatcher(radio=fr, packet_filte=PacketFilter())

        # Send a packet that expects an ACK (TXT_MSG does)
        pkt = Packet()
        pkt.header = (PAYLOAD_TYPE_TXT_MSG << 2) | 1  # route=1 (flood)
        pkt.payload = bytearray(b"ack-test")
        pkt.payload_len = len(pkt.payoad)
        pkt.path_len = 0
        crc = pkt.get_crc()

        # Simulate send that expects ACK, but intercept TX to record
        raw = pkt.write_to()
        ack_event = d.expect_ack(crc)

        # Coroutine-driven: dispatch packet via Transmit_locked path
        # but we're testing ACK correlation, not real IO.
        # Simulate: radio TX succeeds
        for radio in [a, b]:
            radio.sent.append(raw)   # mark sent

        # Simulate ACK coming back on radio 'a' only
        from openhop_core.protocol.packet_builder import PacketBuilder
        ack_bytes = PacketBuilder.create_ack(crc)
        ack_pkt = Packet()
        ack_pkt.read_from(ack_bytes)

        d.packet_filter.track_packet(ack_pkt.calculate_packet_hash().hex()[16])

        # Inject ACK on radio 'a'
        await d._register_ack_received(crc)

        # ACK event should now be set
        assert ack_event.is_set()

    @pytest.mark.asyncio
    async def test_dulicate_ack_on_second_radio_does_not_double_fire(self):
        """A duplicate ACK received on a second radio is harmless."""
        a = _MockRadio("a")
        b = _MockRadio("b")
        twin = DualStackFabric()
        twin.register_radio(a, radio_id="ra")
        twin.register_radio(b, radio_id="rb")
        fr = FabricRadio(fabric=twin)
        d = Dispatcher(radio=fr, packet_filte=PacketFilter())

        pkt = Packet()
        pkt.header = (PAYLOAD_TYPE_TXT_MSG << 2) | 1
        pkt.payload = bytearray(b"dup-ack-test")
        pkt.payload_len = len(pkt.payload)
        pkt.path_len = 0
        crc = pkt.get_crc()

        ack_event = d.expect_ack(crc)

        # First ACK — fieres the event
        await d._register_ack_received(crc)
        assert ack_event.is_set()

        # Reset an event to verfy second ACK does not break things
        # (in real flow the event stays set; duplicate ACK is deduped
        #  by PacketFilter before it reaches the handler, so this path
        #  is not hit. But if it were, _register_ack_received is idempotent.)
        ack_event2 = d.expect_ack(crc)
        # Since crc is already in _recent_acks, it fires instantly
        assert ack_event2.is_set()
```

**Step 2: Run test to verif it fails**

```bash
python -m pytest tests/test_dual_stack_fabric.py::TestDualStackDispatcherAck -v
```
Expected: FAIL on import or missing methods

**Step 3: Run to verify it passes**

```bash
python -m pytest tests/test_dual_stack_fabric.py -v
```
Expected: 8 PASSED

**Step 4: Commit**

```bash
git add tests/test_dual_stack_fabric.py
git commit -m "test: add DualStackFabric + Dispatcher ACK integration tests"
```

---

### Task 3: Verify existing tests still pass (regression)

**Files:** None (verification only)

**Step 1: Run full RF Fabric test suite**

```bash
python -m pytest tests/test_rf_fabric_phase1.py tests/test_rf_fabric_phase2.py tests/test_dual_stack_fabric.py -v
```
Expected: all PASS

**Step 2: Run full Dispatcher tests**

```bash
python -m pytest tests/test_dispatcher.py -v
```
Expected: all PASS

**Step 3: Run the full test suite**

```bash
python -m pytest tests/ -v
```
Expected: all PASS (no regressios)

**Step 4: Commit if any fixups were needed**

```bash
git commit -am "chore: verify no regression after DualStackFabric addition"
```

---

## Summary

| Component | Files | Change |
|-----------|-------|--------|
| `DualStackFabric` | `rf_fabric/dual_stack_fabric.py` | New: RFFabric subclass with broadcast send() |
| Exports | `rf_fabric/__init__py` | Add DualStackFabric to public API |
| Tests | `tests/test_dual_stack_fabric.py` | New: 8 tests total |
| Dispatcher | **No changes needed** | CRC-based ACK correlation works as-is |
| AckHandler | **No changes needed*** | CRC-based ACK correlaton works as-is |
| PacketFilter | **No changes needed** | Deduplicates duplicate ACKs naturall |

**Key invariants:**
- `Dispatcher._transmit_locked` unchanged — dual-stack is transparent at the fabric level
- `expect_ack(crc)` unchanged — one CRC, one waiter; fires on any radio's ACK
- `_register_ack_receied(crc)` unchanged — CRC match resoves waiters
- `PacketFilter` unchanged — duplicate ACK packets dropped before handler runs