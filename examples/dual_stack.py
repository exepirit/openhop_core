#!/usr/bin/env python3
"""
Dual-stack example: broadcast on two radios/frequencies simultaneously.

This example demonstrates DualStackFabric — an RFFabric subclass that
transmits every packet on ALL registered radios concurrently. Each radio
can use different frequencies, LoRa parameters, and transports (SPI, UART,
TCP, etc.).

When a packet is sent via the Dispatcher, it is broadcast on both radios.
Incoming packets from either radio are received and stamped with the
radio_id, allowing the application to know which frequency/interface
the packet arrived on.

Features:
- Two radios on independent frequencies/configurations
- TX broadcast — every outgoing packet transmits on both interfaces
- Per-radio RX tracking — know which radio delivered each incoming packet
- Optional bridging loop — RX on one radio is rebroadcast on the other
  (enable with --bridge)
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common import RADIO_TYPES, create_mesh_node, create_radio

from openhop_core.protocol import LocalIdentity
from openhop_core.protocol.packet_builder import PacketBuilder
from openhop_core.rf_fabric import DualStackFabric, FabricRadio
from openhop_core.node.dispatcher import Dispatcher
from openhop_core.protocol.packet_filter import PacketFilter

logger = logging.getLogger("dual_stack_example")


def _make_identity() -> LocalIdentity:
    return LocalIdentity()


async def dual_stack(
    radio1_type: str = "kiss-tcp",
    radio1_port: str = "localhost:8001",
    radio2_type: str = "kiss-tcp",
    radio2_port: str = "localhost:8002",
    bridge: bool = True,
    timeout: float = 30.0,
):
    print("=" * 60)
    print("  Dual-Stack Mesh Node")
    print("=" * 60)
    print(f"  Radio 0: {radio1_type:12s} {radio1_port}")
    print(f"  Radio 1: {radio2_type:12s} {radio2_port}")
    print(f"  Bridging: {'ON' if bridge else 'OFF'}")
    print("=" * 60)
    print()

    print("[1] Creating radios...")
    serial1, tcp_h1, tcp_p1 = _parse_radio_args(radio1_type, radio1_port)
    serial2, tcp_h2, tcp_p2 = _parse_radio_args(radio2_type, radio2_port)

    radio_a = create_radio(radio1_type, serial_port=serial1, tcp_host=tcp_h1, tcp_port=tcp_p1)
    radio_b = create_radio(radio2_type, serial_port=serial2, tcp_host=tcp_h2, tcp_port=tcp_p2)
    print(f"  ✓ Radio 0 ({radio1_type}) created")
    print(f"  ✓ Radio 1 ({radio2_type}) created")
    print()

    print("[2] Creating dual-stack fabric...")
    fabric = DualStackFabric()
    fabric.register_radio(radio_a, radio_id="stack0")
    fabric.register_radio(radio_b, radio_id="stack1")
    fabric_radio = FabricRadio(fabric=fabric)
    print("  ✓ DualStackFabric ready (stack0 + stack1)")
    print()

    print("[3] Initialising dispatcher...")
    node_name = "DualStackNode"
    identity = _make_identity()
    dispatcher = Dispatcher(
        fabric_radio,
        log_fn=logger.info,
        packet_filter=PacketFilter(),
    )
    dispatcher.register_default_handlers(
        local_identity=identity,
        node_name=node_name,
    )
    dispatcher._rx_enabled = True
    print(f"  ✓ Dispatcher ready (node: {node_name})")
    print()

    print("[3.1] Initialising radios (begin())...")
    fabric_radio.begin()
    print("  ✓ All radios initialised")
    print()

    class _Stats:
        rx_per_stack: dict[str, int] = {"stack0": 0, "stack1": 0}
        tx_broadcasts: int = 0
        bridge_forwards: int = 0

    stats = _Stats()

    async def on_raw_rx(data: bytes, rssi: int, snr: float) -> None:
        rx_rid = getattr(fabric, "last_rx_radio_id", None) or "?"
        stats.rx_per_stack[rx_rid] = stats.rx_per_stack.get(rx_rid, 0) + 1
        tag = "→" if rx_rid == "stack0" else "←"
        print(
            f"  RX [{tag} {rx_rid}]  {len(data)} bytes  "
            f"RSSI={rssi}dBm  SNR={snr:+.1f}dB  "
            f"(total rx: stack0={stats.rx_per_stack['stack0']} stack1={stats.rx_per_stack['stack1']})"
        )

        if bridge:
            other = "stack1" if rx_rid == "stack0" else "stack0"
            try:
                await fabric.send(data, radio_id=other)
                stats.bridge_forwards += 1
                rtag = "→" if other == "stack0" else "←"
                print(f"  BRIDGE [{rtag} {other}] {len(data)} bytes forwarded")
            except Exception:
                pass

    dispatcher.add_raw_rx_subscriber(on_raw_rx)

    async def periodic_broadcast():
        await asyncio.sleep(2)
        while True:
            print()
            print("[4] Broadcasting advert on BOTH stacks...")
            pkt = PacketBuilder.create_advert(
                identity,
                name=node_name,
            )

            result = await dispatcher.send_packet(pkt, wait_for_ack=False)
            if result:
                stats.tx_broadcasts += 1
                print(f"  ✓ Advert broadcast on stack0 + stack1")
            else:
                print("  ✗ TX failed")
            print()

            await asyncio.sleep(30)  # re-broadcast every 30s

    async def periodic_status():
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            await asyncio.sleep(5)
            elapsed = int(time.monotonic() - start)
            remaining = int(timeout - elapsed)
            print(
                f"[{elapsed:3d}s] rx: stack0={stats.rx_per_stack['stack0']} "
                f"stack1={stats.rx_per_stack['stack1']}  "
                f"tx: {stats.tx_broadcasts}  bridge: {stats.bridge_forwards}  "
                f"({remaining}s remaining)"
            )

    tasks = [
        asyncio.create_task(periodic_broadcast()),
        asyncio.create_task(periodic_status()),
    ]

    try:
        done, pending = await asyncio.wait(tasks, timeout=timeout)
        for t in pending:
            t.cancel()
    except KeyboardInterrupt:
        for t in tasks:
            t.cancel()
        print("\nInterrupted.")

    print()
    print("=" * 60)
    print("  Session summary")
    print("=" * 60)
    print(f"  RX stack0:  {stats.rx_per_stack['stack0']}")
    print(f"  RX stack1:  {stats.rx_per_stack['stack1']}")
    print(f"  TX broadcasts: {stats.tx_broadcasts}")
    if bridge:
        print(f"  Bridge forwards: {stats.bridge_forwards}")
    print("=" * 60)


def _parse_radio_args(radio_type: str, port_arg: str):
    """Parse port_arg into (serial_port, tcp_host, tcp_port) tuple."""
    if radio_type in ("kiss-tcp", "modem_tcp", "pymc_tcp"):
        parts = port_arg.rsplit(":", 1)
        host = parts[0] if parts else "localhost"
        port = int(parts[1]) if len(parts) > 1 else 8001
        return ("/dev/null", host, port)
    return (port_arg, "localhost", 8001)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Dual-stack mesh node: broadcast on two radios simultaneously"
    )
    parser.add_argument(
        "--radio1-type",
        choices=RADIO_TYPES,
        default="kiss-tcp",
        help="Radio 0 (stack0) hardware type (default: kiss-tcp)",
    )
    parser.add_argument(
        "--radio1-port",
        default="localhost:8001",
        help="Radio 0 port (serial device, or host:port for TCP; default: localhost:8001)",
    )
    parser.add_argument(
        "--radio2-type",
        choices=RADIO_TYPES,
        default="kiss-tcp",
        help="Radio 1 (stack1) hardware type (default: kiss-tcp)",
    )
    parser.add_argument(
        "--radio2-port",
        default="localhost:8002",
        help="Radio 1 port (serial device, or host:port for TCP; default: localhost:8002)",
    )
    parser.add_argument(
        "--bridge",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable cross-stack bridging (RX on one → TX on other) (default: on)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Runtime in seconds (default: 30)",
    )
    args = parser.parse_args()

    asyncio.run(
        dual_stack(
            radio1_type=args.radio1_type,
            radio1_port=args.radio1_port,
            radio2_type=args.radio2_type,
            radio2_port=args.radio2_port,
            bridge=args.bridge,
            timeout=args.timeout,
        )
    )


if __name__ == "__main__":
    main()