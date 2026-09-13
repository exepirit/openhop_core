"""
KISS TCP Protocol Wrapper

Compatible with the KISS TCP interface served by modem73 and similar TNCs.
Connects as a TCP client; the remote TNC is expected to be already running
in KISS mode (no CLI-based auto-configuration over TCP).

Wire format is standard KISS framing — FEND-delimited frames with byte
stuffing — sent directly over a raw TCP stream with no additional headers.
"""

import asyncio
import logging
import socket
import threading
from collections import deque
from typing import Any, Callable, Dict, Optional

from .base import LoRaRadio
from .kiss_protocol import (
    DEFAULT_TIMEOUT,
    KISS_CMD_DATA,
    KISS_CMD_FULLDUP,
    KISS_CMD_PERSIST,
    KISS_CMD_SLOTTIME,
    KISS_CMD_TXDELAY,
    KISS_CMD_TXTAIL,
    KISS_FEND,
    KISS_FESC,
    KISS_MASK_CMD,
    KISS_MASK_PORT,
    KISS_TFEND,
    KISS_TFESC,
    MAX_FRAME_SIZE,
    RX_BUFFER_SIZE,
    TX_BUFFER_SIZE,
    _KISS_FEND_B,
    _KISS_FESC_B,
)

# TCP-specific RX read chunk size
RX_READ_SIZE = 4096

logger = logging.getLogger("KissTcpWrapper")


class KissTcpWrapper(LoRaRadio):
    """
    KISS TCP Protocol Interface

    Provides full-duplex KISS protocol communication over a TCP connection.
    Handles KISS frame encoding/decoding, buffering, and optional KISS-level
    configuration commands. Implements the LoRaRadio interface for openHop
    Core compatibility.
    """

    def __init__(
        self,
        host: str = "localhost",
        tcp_port: int = 8001,
        timeout: float = DEFAULT_TIMEOUT,
        kiss_port: int = 0,
        on_frame_received: Optional[Callable[[bytes], None]] = None,
        radio_config: Optional[Dict[str, Any]] = None,
        auto_configure: bool = False,
        connect_timeout: float = 5.0,
    ):
        """
        Initialize KISS TCP Wrapper

        Args:
            host: TCP hostname or IP address of the KISS TNC (default: localhost)
            tcp_port: TCP port of the KISS TNC (default: 8001, modem73 default)
            timeout: Socket read timeout in seconds (default: 1.0)
            kiss_port: KISS port number (0-15, default: 0)
            on_frame_received: Callback for received data frames
            radio_config: Optional radio configuration dict with keys:
                         frequency, bandwidth, sf, cr, sync_word, power, etc.
                         (Config depends on TNC implementation; modem73 uses
                         its own config file.)
            auto_configure: If True, attempt KISS-level configuration after
                         connecting. Warning: modem73 acknowledges but ignores
                         most KISS config commands. Default: False.
            connect_timeout: TCP connection timeout in seconds (default: 5.0)
        """
        self.host = host
        self.tcp_port = tcp_port
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.kiss_port = kiss_port & 0x0F
        self.auto_configure = auto_configure

        self.radio_config = radio_config or {}
        self.is_configured = False
        self.kiss_mode_active = False

        self.sock: Optional[socket.socket] = None
        self.is_connected = False

        self.rx_buffer = deque(maxlen=RX_BUFFER_SIZE)
        self.tx_buffer = deque(maxlen=TX_BUFFER_SIZE)

        self.rx_frame_buffer = bytearray()
        self.in_frame = False
        self.escaped = False

        self.rx_thread: Optional[threading.Thread] = None
        self.tx_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        self.on_frame_received = on_frame_received

        # Event loop reference for thread-safe RX callback dispatch.
        # Set at connect() time when called from an async context.
        self._event_loop = None

        self.config = {
            "txdelay": 30,
            "persist": 64,
            "slottime": 10,
            "txtail": 1,
            "fulldup": False,
        }

        self.stats = {
            "frames_sent": 0,
            "frames_received": 0,
            "bytes_sent": 0,
            "bytes_received": 0,
            "frame_errors": 0,
            "buffer_overruns": 0,
            "last_rssi": None,
            "last_snr": None,
            "noise_floor": None,
        }

    def connect(self) -> bool:
        """
        Connect to the KISS TNC over TCP and start communication threads.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Capture the event loop for thread-safe RX callback dispatch.
            # If called from outside an async context the loop stays None
            # and callbacks are invoked directly (for simple sync callbacks).
            try:
                self._event_loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            self.sock = socket.create_connection(
                (self.host, self.tcp_port),
                timeout=self.connect_timeout,
            )
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock.settimeout(self.timeout)

            self.is_connected = True
            self.stop_event.clear()

            self.rx_thread = threading.Thread(target=self._rx_worker, daemon=True)
            self.tx_thread = threading.Thread(target=self._tx_worker, daemon=True)

            self.rx_thread.start()
            self.tx_thread.start()

            logger.info(
                f"KISS TCP connected to {self.host}:{self.tcp_port}"
            )

            if self.auto_configure:
                logger.warning(
                    "auto_configure=True for KISS-over-TCP; configuration depends on "
                    "the TNC implementation (modem73 uses its own config file and "
                    "acknowledges but ignores most KISS config commands)."
                )
                if not self.configure_radio_and_enter_kiss():
                    logger.warning(
                        "TCP KISS auto-configuration failed; continuing regardless"
                    )

            self.kiss_mode_active = True
            return True

        except Exception as e:
            logger.error(f"Failed to connect to {self.host}:{self.tcp_port}: {e}")
            self.disconnect()
            return False

    def disconnect(self):
        """Disconnect from the TCP KISS TNC and stop threads."""
        self.is_connected = False

        self._drain_tx_buffer(timeout=2.0)

        self.stop_event.set()

        if self.rx_thread and self.rx_thread.is_alive():
            self.rx_thread.join(timeout=2.0)
        if self.tx_thread and self.tx_thread.is_alive():
            self.tx_thread.join(timeout=2.0)

        if self.sock is not None:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

        self.kiss_mode_active = False
        logger.info(f"KISS TCP disconnected from {self.host}:{self.tcp_port}")

    def _drain_tx_buffer(self, timeout: float = 2.0) -> None:
        """Flush any pending frames in the TX buffer directly to the socket.

        Called during disconnect to ensure queued frames are actually
        transmitted before the socket is closed and the TX thread exits.
        """
        import time as _time

        s = self.sock
        if s is None:
            return

        deadline = _time.monotonic() + timeout
        while self.tx_buffer and _time.monotonic() < deadline:
            try:
                frame = self.tx_buffer.popleft()
                s.sendall(frame)
                self.stats["frames_sent"] += 1
                self.stats["bytes_sent"] += len(frame)
            except Exception as e:
                logger.error(f"Failed to drain TX buffer: {e}")
                break

    def send_frame(self, data: bytes) -> bool:
        """
        Send a data frame via KISS protocol.

        Args:
            data: Raw frame data to send

        Returns:
            True if frame queued successfully, False otherwise
        """
        if not self.is_connected or len(data) > MAX_FRAME_SIZE:
            logger.warning(
                f"Cannot send frame - connected: {self.is_connected}, "
                f"size: {len(data)}/{MAX_FRAME_SIZE}"
            )
            return False

        try:
            kiss_frame = self._encode_kiss_frame(KISS_CMD_DATA, data)

            if len(self.tx_buffer) < TX_BUFFER_SIZE:
                self.tx_buffer.append(kiss_frame)
                return True
            else:
                self.stats["buffer_overruns"] += 1
                logger.warning("TX buffer overrun")
                return False

        except Exception as e:
            logger.error(f"Failed to send frame: {e}")
            return False

    def send_config_command(self, cmd: int, value: int) -> bool:
        """
        Send KISS configuration command.

        Args:
            cmd: KISS command type (KISS_CMD_*)
            value: Command parameter value

        Returns:
            True if command sent successfully, False otherwise
        """
        if not self.is_connected:
            return False

        try:
            kiss_frame = self._encode_kiss_frame(cmd, bytes([value]))

            if len(self.tx_buffer) >= TX_BUFFER_SIZE:
                self.stats["buffer_overruns"] += 1
                return False

            self.tx_buffer.append(kiss_frame)

            if cmd == KISS_CMD_TXDELAY:
                self.config["txdelay"] = value
            elif cmd == KISS_CMD_PERSIST:
                self.config["persist"] = value
            elif cmd == KISS_CMD_SLOTTIME:
                self.config["slottime"] = value
            elif cmd == KISS_CMD_TXTAIL:
                self.config["txtail"] = value
            elif cmd == KISS_CMD_FULLDUP:
                self.config["fulldup"] = bool(value)

            return True

        except Exception as e:
            logger.error(f"Failed to send config command: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Get interface statistics."""
        return self.stats.copy()

    def get_config(self) -> Dict[str, Any]:
        """Get current KISS configuration."""
        return self.config.copy()

    def configure_radio_and_enter_kiss(self) -> bool:
        """
        Attempt KISS-level configuration over TCP.

        Unlike the serial wrapper, TCP-based TNCs (such as modem73) are
        typically pre-configured and start in KISS mode. This method sends
        KISS config commands (TXDELAY, PERSIST, etc.) which may be
        acknowledged but are implementation-dependent.

        Returns:
            True if configuration commands were queued, False otherwise
        """
        if not self.is_connected:
            logger.error("Cannot configure: not connected")
            return False

        try:
            if self.radio_config:
                self._configure_radio()

            self._enter_kiss_mode()

            self.is_configured = True
            self.kiss_mode_active = True
            logger.info("KISS configuration commands sent over TCP")
            return True

        except Exception as e:
            logger.error(f"Configuration failed: {e}")
            return False

    def _configure_radio(self) -> bool:
        """
        Send KISS-level radio configuration commands.

        For TCP-based TNCs, this sends KISS config commands only (TXDELAY,
        PERSIST, SLOTTIME, TXTAIL, FULLDUP). Radio parameters like frequency
        and spreading factor are typically configured via the TNC's own config
        mechanism and are not accessible through standard KISS commands.

        Returns:
            True if configuration commands were queued
        """
        if not self.sock:
            return False

        try:
            # Standard KISS-level parameters — these are the only config
            # commands available in the KISS specification.
            txdelay = self.radio_config.get("txdelay")
            if txdelay is not None:
                self.send_config_command(KISS_CMD_TXDELAY, int(txdelay))

            persist = self.radio_config.get("persist")
            if persist is not None:
                self.send_config_command(KISS_CMD_PERSIST, int(persist))

            slottime = self.radio_config.get("slottime")
            if slottime is not None:
                self.send_config_command(KISS_CMD_SLOTTIME, int(slottime))

            txtail = self.radio_config.get("txtail")
            if txtail is not None:
                self.send_config_command(KISS_CMD_TXTAIL, int(txtail))

            fulldup = self.radio_config.get("fulldup")
            if fulldup is not None:
                self.send_config_command(KISS_CMD_FULLDUP, int(fulldup))

            logger.info(
                "Radio config sent via KISS commands (TXDELAY/PERSIST/SLOTTIME/"
                "TXTAIL/FULLDUP). Frequency/BW/SF/CR must be configured on the "
                "TNC side."
            )
            return True

        except Exception as e:
            logger.error(f"Radio configuration error: {e}")
            return False

    def _enter_kiss_mode(self) -> bool:
        """
        Mark KISS mode as active over TCP.

        TCP TNCs typically start in KISS mode automatically. This method
        does not send any command — it simply records the mode as active.
        """
        self.kiss_mode_active = True
        logger.info("Marked KISS mode as active over TCP")
        return True

    def set_rx_callback(self, callback: Callable[[bytes], None]):
        """
        Set the RX callback function.

        Args:
            callback: Function to call when a frame is received
        """
        self.on_frame_received = callback
        logger.debug("RX callback set")

    def begin(self):
        """
        Initialize the radio.

        Raises:
            Exception: If connection fails
        """
        success = self.connect()
        if not success:
            raise Exception("Failed to initialize KISS TCP radio")

    async def send(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Send data via KISS TCP TNC.

        Returns:
            Empty metadata dict on successful queue (no hardware TX metadata).

        Raises:
            Exception: If send fails
        """
        success = self.send_frame(data)
        if not success:
            raise Exception("Failed to send frame via KISS TCP TNC")
        return {}

    async def wait_for_rx(self) -> bytes:
        """
        Wait for a packet to be received asynchronously.

        Returns:
            Received packet data
        """
        loop = asyncio.get_running_loop()
        future = asyncio.Future()

        original_callback = self.on_frame_received

        def temp_callback(data: bytes):
            if not future.done():
                try:
                    loop.call_soon_threadsafe(future.set_result, data)
                except RuntimeError as e:
                    logger.error(f"Failed to complete wait_for_rx future: {e}")
            if original_callback:
                try:
                    original_callback(data)
                except Exception as e:
                    logger.error(f"Error in original callback: {e}")

        self.on_frame_received = temp_callback

        try:
            data = await future
            return data
        finally:
            self.on_frame_received = original_callback

    def sleep(self):
        """
        Put the radio into low-power mode.

        Note: KISS TCP TNCs typically don't support software sleep control.
        """
        logger.debug("Sleep mode not supported for KISS TCP TNC")
        pass

    def get_last_rssi(self) -> int:
        """
        Return last received RSSI in dBm.

        Returns:
            Last RSSI value or -999 if not available
        """
        val = self.stats.get("last_rssi", -999)
        return -999 if val is None else val

    def get_last_snr(self) -> float:
        """
        Return last received SNR in dB.

        Returns:
            Last SNR value or -999.0 if not available
        """
        val = self.stats.get("last_snr", -999.0)
        return -999.0 if val is None else val

    def _encode_kiss_frame(self, cmd: int, data: bytes) -> bytes:
        """
        Encode data into KISS frame format.

        Args:
            cmd: KISS command byte
            data: Raw data to encode

        Returns:
            Encoded KISS frame
        """
        cmd_byte = ((self.kiss_port << 4) & KISS_MASK_PORT) | (cmd & KISS_MASK_CMD)

        frame = bytearray([KISS_FEND, cmd_byte])

        for byte in data:
            if byte == KISS_FEND:
                frame.extend([KISS_FESC, KISS_TFEND])
            elif byte == KISS_FESC:
                frame.extend([KISS_FESC, KISS_TFESC])
            else:
                frame.append(byte)

        frame.append(KISS_FEND)

        return bytes(frame)

    def _decode_kiss_byte(self, byte: int):
        """
        Process received byte for KISS frame decoding.

        Args:
            byte: Received byte
        """
        if byte == KISS_FEND:
            if self.in_frame and len(self.rx_frame_buffer) > 1:
                self._process_received_frame()
            self.rx_frame_buffer.clear()
            self.in_frame = True
            self.escaped = False

        elif byte == KISS_FESC:
            if self.in_frame:
                self.escaped = True

        elif self.escaped:
            if byte == KISS_TFEND or byte == KISS_TFESC:
                decoded = KISS_FEND if byte == KISS_TFEND else KISS_FESC
                if len(self.rx_frame_buffer) >= MAX_FRAME_SIZE:
                    self.stats["frame_errors"] += 1
                    logger.warning(
                        "KISS frame exceeded max size (%d), resyncing",
                        MAX_FRAME_SIZE,
                    )
                    self.rx_frame_buffer.clear()
                    self.in_frame = False
                else:
                    self.rx_frame_buffer.append(decoded)
            else:
                self.stats["frame_errors"] += 1
                logger.warning(f"Invalid KISS escape sequence: 0x{byte:02X}")
                self.rx_frame_buffer.clear()
                self.in_frame = False
            self.escaped = False

        else:
            if self.in_frame:
                if len(self.rx_frame_buffer) >= MAX_FRAME_SIZE:
                    self.stats["frame_errors"] += 1
                    logger.warning(
                        "KISS frame exceeded max size (%d), resyncing",
                        MAX_FRAME_SIZE,
                    )
                    self.rx_frame_buffer.clear()
                    self.in_frame = False
                else:
                    self.rx_frame_buffer.append(byte)

    def _decode_kiss(self, data: bytes) -> None:
        """Bulk KISS decoder used by the RX worker.

        Behaviorally identical to feeding each byte through ``_decode_kiss_byte``,
        but copies runs of plain bytes with C-level ``bytes.find``/slicing and
        only does per-byte work at FEND/FESC.
        """
        n = len(data)
        if n == 0:
            return

        buf = self.rx_frame_buffer
        in_frame = self.in_frame
        escaped = self.escaped
        i = 0

        while i < n:
            if escaped:
                b = data[i]
                i += 1
                escaped = False
                if b == KISS_TFEND or b == KISS_TFESC:
                    decoded = KISS_FEND if b == KISS_TFEND else KISS_FESC
                    if len(buf) >= MAX_FRAME_SIZE:
                        self.stats["frame_errors"] += 1
                        logger.warning(
                            "KISS frame exceeded max size (%d), resyncing",
                            MAX_FRAME_SIZE,
                        )
                        buf.clear()
                        in_frame = False
                    else:
                        buf.append(decoded)
                else:
                    self.stats["frame_errors"] += 1
                    logger.warning(
                        f"Invalid KISS escape sequence: 0x{b:02X}"
                    )
                    buf.clear()
                    in_frame = False
                continue

            fend = data.find(_KISS_FEND_B, i)
            fesc = data.find(_KISS_FESC_B, i)
            if fend == -1:
                nxt = fesc
            elif fesc == -1:
                nxt = fend
            else:
                nxt = fend if fend < fesc else fesc

            run_end = n if nxt == -1 else nxt
            if run_end > i and in_frame:
                run = data[i:run_end]
                space = MAX_FRAME_SIZE - len(buf)
                if len(run) <= space:
                    buf += run
                else:
                    if space > 0:
                        buf += run[:space]
                    self.stats["frame_errors"] += 1
                    logger.warning(
                        "KISS frame exceeded max size (%d), resyncing",
                        MAX_FRAME_SIZE,
                    )
                    buf.clear()
                    in_frame = False

            if nxt == -1:
                break

            i = run_end
            b = data[i]
            i += 1
            if b == KISS_FEND:
                if in_frame and len(buf) > 1:
                    self._process_received_frame()
                buf.clear()
                in_frame = True
                escaped = False
            else:  # KISS_FESC
                if in_frame:
                    escaped = True

        self.in_frame = in_frame
        self.escaped = escaped

    def _process_received_frame(self):
        """Process a complete received KISS frame."""
        if len(self.rx_frame_buffer) < 1:
            return

        cmd_byte = self.rx_frame_buffer[0]
        port = (cmd_byte & KISS_MASK_PORT) >> 4
        cmd = cmd_byte & KISS_MASK_CMD

        if port != self.kiss_port:
            return

        data = bytes(self.rx_frame_buffer[1:])

        if cmd == KISS_CMD_DATA:
            callback = self.on_frame_received
            if callback and len(data) > 0:
                self.stats["frames_received"] += 1
                self.stats["bytes_received"] += len(data)
                try:
                    loop = self._event_loop
                    if loop is not None:
                        loop.call_soon_threadsafe(callback, data)
                    else:
                        callback(data)
                except Exception as e:
                    logger.error(f"Error in frame received callback: {e}")
        else:
            logger.debug(
                f"Received KISS config command: cmd=0x{cmd:02X}, data={data.hex()}"
            )

    def _rx_worker(self):
        """Background thread for receiving data from TCP socket."""
        while not self.stop_event.is_set() and self.is_connected:
            try:
                s = self.sock
                if s is None:
                    break
                chunk = s.recv(RX_READ_SIZE)
                if not chunk:
                    logger.info("TCP KISS connection closed by remote")
                    self._fail_transport()
                    break
                self._decode_kiss(chunk)

            except socket.timeout:
                continue
            except Exception as e:
                if self.is_connected:
                    logger.error(f"RX worker error: {e}")
                self._fail_transport()
                break

    def _tx_worker(self):
        """Background thread for sending data over TCP socket."""
        while not self.stop_event.is_set() and self.is_connected:
            try:
                if self.tx_buffer:
                    frame = self.tx_buffer.popleft()

                    if self.sock:
                        self.sock.sendall(frame)

                        self.stats["frames_sent"] += 1
                        self.stats["bytes_sent"] += len(frame)
                    else:
                        logger.warning("TCP socket not available")
                else:
                    threading.Event().wait(0.01)

            except Exception as e:
                if self.is_connected:
                    logger.error(f"TX worker error: {e}")
                self._fail_transport()
                break

    def _fail_transport(self) -> None:
        """Mark the link unhealthy and close the TCP socket from a worker thread.

        Does not join threads (the caller is one of them). Closing the socket
        wakes the peer worker out of a blocking recv so both sides exit.
        """
        self.is_connected = False
        self.stop_event.set()
        s = self.sock
        if s is None:
            return
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            s.close()
        except OSError:
            pass
        self.sock = None

    def __enter__(self):
        """Context manager entry."""
        if not self.connect():
            raise RuntimeError(
                f"Failed to connect to KISS TCP TNC {self.host}:{self.tcp_port}"
            )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            self.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    import time

    def on_frame_received(data):
        print(f"Received frame: {data.hex()}")

    kiss = KissTcpWrapper(
        host="localhost",
        tcp_port=8001,
        on_frame_received=on_frame_received,
    )

    try:
        if kiss.connect():
            print("Connected successfully")
            print(f"Configuration: {kiss.get_config()}")
            print(f"Statistics: {kiss.get_stats()}")

            kiss.send_frame(b"Hello KISS over TCP!")

            time.sleep(5)
        else:
            print("Failed to connect")

    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        kiss.disconnect()
