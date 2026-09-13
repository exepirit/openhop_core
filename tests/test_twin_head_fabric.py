"""TwinHeadFabric — broadcast send to all radios concurrently."""

from __future__ import annotations

import pytest

from openhop_core.node.dispatcher import Dispatcher
from openhop_core.protocol import Packet
from openhop_core.protocol.constants import PAYLOAD_TYPE_TXT_MSG
from openhop_core.protocol.packet_filter import PacketFilter
from openhop_core.rf_fabric import FabricRadio
from openhop_core.rf_fabric.twin_head_fabric import TwinHeadFabric


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


class _FailingRadio:
    def __init__(self, name: str = "fail"):
        self.name = name
        self.rx_callback = None
        self.sent: list[bytes] = []
        self.last_rssi = -80
        self.last_snr = 7.5

    def set_rx_callback(self, callback):
        self.rx_callback = callback

    async def send(self, data: bytes):
        raise RuntimeError(f"radio {self.name} failed")

    def get_last_rssi(self):
        return self.last_rssi

    def get_last_snr(self):
        return self.last_snr


@pytest.mark.asyncio
async def test_send_broadcasts_to_all_radios():
    fabric = TwinHeadFabric()
    ra = _MockRadio("ra")
    rb = _MockRadio("rb")
    fabric.register_radio(ra, radio_id="ra")
    fabric.register_radio(rb, radio_id="rb")

    data = b"broadcast-test"
    result = await fabric.send(data)

    assert result is not None
    assert data in ra.sent
    assert data in rb.sent


@pytest.mark.asyncio
async def test_send_aggregates_metadata():
    fabric = TwinHeadFabric()
    ra = _MockRadio("ra")
    rb = _MockRadio("rb")
    fabric.register_radio(ra, radio_id="ra")
    fabric.register_radio(rb, radio_id="rb")

    result = await fabric.send(b"meta-test")

    assert result["ok"] is True
    assert "ra" in result["radio_id"]
    assert "rb" in result["radio_id"]
    assert result.get("radio_ids_count") == 2
    assert "results" in result
    assert len(result["results"]) == 2
    radio_ids = {rid for rid, _meta in result["results"]}
    assert radio_ids == {"ra", "rb"}


@pytest.mark.asyncio
async def test_send_still_supports_explicit_radio_id():
    fabric = TwinHeadFabric()
    ra = _MockRadio("ra")
    rb = _MockRadio("rb")
    fabric.register_radio(ra, radio_id="ra")
    fabric.register_radio(rb, radio_id="rb")

    result = await fabric.send(b"explicit-test", radio_id="ra")

    assert result["radio_id"] == "ra"
    assert b"explicit-test" in ra.sent
    assert b"explicit-test" not in rb.sent


@pytest.mark.asyncio
async def test_one_radio_fail_does_not_crash():
    fabric = TwinHeadFabric()
    ra = _MockRadio("ra")
    rb = _FailingRadio("rb")
    fabric.register_radio(ra, radio_id="ra")
    fabric.register_radio(rb, radio_id="rb")

    data = b"partial-fail"
    result = await fabric.send(data)

    assert result is not None
    assert result["ok"] is True
    assert result["radio_id"] == "ra"
    assert data in ra.sent


@pytest.mark.asyncio
async def test_all_radios_fail_returns_failure():
    fabric = TwinHeadFabric()
    ra = _FailingRadio("ra")
    rb = _FailingRadio("rb")
    fabric.register_radio(ra, radio_id="ra")
    fabric.register_radio(rb, radio_id="rb")

    result = await fabric.send(b"total-fail")

    assert result is None


@pytest.mark.asyncio
async def test_send_no_radios_returns_none():
    fabric = TwinHeadFabric()

    result = await fabric.send(b"no-radio")

    assert result is None


# ---------------------------------------------------------------------------
# Dispatcher + TwinHeadFabric end-to-end ACK tests
# ---------------------------------------------------------------------------

import asyncio

from openhop_core.protocol.constants import PAYLOAD_TYPE_ACK


class TestTwinHeadAck:
    """ACK correlation with TwinHeadFabric — injects actual ACK packets through
    the radio RX callback path and verifies resolution across both radios."""

    @staticmethod
    def _make_packet(payload_bytes: bytes = b"test-data") -> "Packet":
        pkt = Packet()
        pkt.header = (PAYLOAD_TYPE_TXT_MSG << 2) | 1  # route=1 (flood)
        pkt.payload = bytearray(payload_bytes)
        pkt.payload_len = len(pkt.payload)
        pkt.path_len = 0
        return pkt

    @staticmethod
    def _make_ack_bytes(crc: int) -> bytes:
        ack = Packet()
        ack.header = (PAYLOAD_TYPE_ACK << 2) | 1  # route=1 (flood)
        ack.payload = bytearray(crc.to_bytes(4, "little"))
        ack.payload_len = len(ack.payload)
        ack.path_len = 0
        return ack.write_to()

    @staticmethod
    def _make_dispatcher(*radios: _MockRadio) -> Dispatcher:
        fabric = TwinHeadFabric()
        for i, r in enumerate(radios):
            fabric.register_radio(r, radio_id=f"r{i}")
        d = Dispatcher(FabricRadio(fabric=fabric), packet_filter=PacketFilter())
        d.register_default_handlers()
        return d

    @pytest.mark.asyncio
    async def test_ack_arrives_on_one_radio_resolves_send(self):
        """E2E: send_packet broadcast to both radios; ACK on one radio resolves waiter."""
        ra = _MockRadio("ra")
        rb = _MockRadio("rb")
        d = self._make_dispatcher(ra, rb)

        pkt = self._make_packet()
        raw = pkt.write_to()
        crc = pkt.get_crc()

        # Fire-and-request-ack in background; main coroutine injects ACK while it waits.
        send_task = asyncio.create_task(d.send_packet(pkt, wait_for_ack=True))

        # Let the TX + ACK registration complete.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # Both radios broadcast the same packet.
        assert raw in ra.sent
        assert raw in rb.sent

        # Inject ACK frame on one radio (ra) — the RX pipeline must route it
        # through AckHandler → _register_ack_received → resolve send_task.
        ack_raw = self._make_ack_bytes(crc)
        ra.inject(ack_raw)

        # Allow the RX task to process the ACK.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        result = await send_task
        assert result is True

    @pytest.mark.asyncio
    async def test_ack_dedup_across_both_radios(self):
        """Same ACK frame received on both radios resolves only once."""
        ra = _MockRadio("ra")
        rb = _MockRadio("rb")
        d = self._make_dispatcher(ra, rb)

        pkt = self._make_packet()
        crc = pkt.get_crc()
        ack_raw = self._make_ack_bytes(crc)

        # Register the waiter manually (out-of-band from normal send path).
        ack_waiter = d.expect_ack(crc)
        assert not ack_waiter.is_set()

        # Inject the same ACK frame on both radios.
        ra.inject(ack_raw)
        rb.inject(ack_raw)

        # Allow both RX tasks to process.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        # The waiter has fired (and dedup prevented a second Resolution).
        assert ack_waiter.is_set()
        assert crc not in d._waiting_acks
        assert crc in d._recent_acks
