#!/usr/bin/env python3
"""
Minimal example: Send a flood advertisement packet.

This example demonstrates how to create and broadcast an advertisement
packet that will be flooded throughout the mesh network using SX1262 radio hardware.

Usage:
    python send_flood_advert.py [radio_type]

Arguments:
    radio_type: 'waveshare' (default) or 'uconsole'

The flood advert is sent without expecting any acknowledgment or response.
"""

import asyncio

from common import RADIO_TYPES, create_mesh_node, print_packet_info

from openhop_core.protocol.constants import ADVERT_FLAG_IS_CHAT_NODE
from openhop_core.protocol.packet_builder import PacketBuilder


async def send_flood_advert(
    radio_type: str = "waveshare",
    serial_port: str = "/dev/ttyUSB0",
    tcp_host: str = "localhost",
    tcp_port: int = 8001,
):
    # Create a mesh node with SX1262 radio
    mesh_node, identity = create_mesh_node("MyNode", radio_type, serial_port, tcp_host, tcp_port)
    radio = mesh_node.radio if hasattr(mesh_node, 'radio') else None

    try:
        # Create a flood advertisement packet
        # Parameters: identity, node_name, lat, lon, feature1, feature2, flags
        advert_packet = PacketBuilder.create_flood_advert(
            local_identity=identity,
            name="MyNode",
            lat=37.7749,  # San Francisco latitude
            lon=-122.4194,  # San Francisco longitude
            flags=ADVERT_FLAG_IS_CHAT_NODE,
        )

        print_packet_info(advert_packet, "Created flood advert packet")
        print("Sending packet...")

        # Send the packet through the mesh node's dispatcher
        success = await mesh_node.dispatcher.send_packet(advert_packet, wait_for_ack=False)

        if success:
            print("Packet sent successfully!")
        else:
            print("Failed to send packet")

        return advert_packet

    finally:
        if radio is not None and hasattr(radio, 'disconnect'):
            radio.disconnect()


def main():
    """Main function for running the example."""
    import argparse

    parser = argparse.ArgumentParser(description="Send a flood advertisement packet")
    parser.add_argument(
        "--radio-type",
        choices=RADIO_TYPES,
        default="waveshare",
        help="Radio hardware type (default: waveshare)",
    )
    parser.add_argument(
        "--serial-port",
        default="/dev/ttyUSB0",
        help="Serial port for KISS TNC (default: /dev/ttyUSB0)",
    )
    parser.add_argument(
        "--tcp-host",
        default="localhost",
        help="TCP host for kiss-tcp radio type (default: localhost)",
    )
    parser.add_argument(
        "--tcp-port",
        type=int,
        default=8001,
        help="TCP port for kiss-tcp radio type (default: 8001, modem73 default)",
    )

    args = parser.parse_args()

    print(f"Using {args.radio_type} radio configuration")
    if args.radio_type == "kiss-tnc":
        print(f"Serial port: {args.serial_port}")
    elif args.radio_type == "kiss-tcp":
        print(f"TCP: {args.tcp_host}:{args.tcp_port}")

    asyncio.run(
        send_flood_advert(
            args.radio_type,
            args.serial_port,
            args.tcp_host,
            args.tcp_port,
        )
    )


if __name__ == "__main__":
    main()
