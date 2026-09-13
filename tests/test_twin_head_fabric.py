"""TwinHeadFabric — broadcast send to all radios concurrently."""

from __future__ import annotations

import pytest

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