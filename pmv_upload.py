#!/usr/bin/env python3
"""
pmv_upload.py - Upload a PMV .dat database to a real device or the emulator

Usage:
  # Upload to emulator (start pmv_emulator.py first)
  python3 pmv_upload.py "Invest 1.15.dat"

  # Upload to emulator on custom host/port
  python3 pmv_upload.py "Invest 1.15.dat" --host 127.0.0.1 --port 54321

  # Upload to real USB HID device (requires hidapi: pip install hidapi)
  python3 pmv_upload.py "Invest 1.15.dat" --hid --vid 0x1234 --pid 0x5678

Packet format (reversed from InvestorDatabaseDownloader.exe IL):

  BEGIN (cmd=0x0a, 64 bytes):
    [0]      = 0x0a
    [1..16]  = description PadRight(16), one ASCII byte per char
    [17..63] = 0x00

  RECORD (cmd=0x0b, 64 bytes):
    [0]      = 0x0b
    [1]      = record index (0-based)
    [2..25]  = name PadRight(24), one ASCII byte per char
    [26]     = category_id (0=Gold 1=Silver 2=Other 3=Coins/Bullion)
    [27..30] = values[0] as float32 LE  (ResGreenLeft)
    [31..34] = values[1] as float32 LE  (ResYellowLeft)
    [35..38] = values[2] as float32 LE  (ResGreenRight)
    [39..42] = values[3] as float32 LE  (ResYellowRight)
    [43..46] = values[4] as float32 LE  (Field5 / SpecificGravity)
    [47..50] = values[5] as float32 LE  (SpecificGravity label / TempCoefficient)
    [51..54] = values[6] as float32 LE  (DimensionModePlusTolerance)
    [55..58] = values[7] as float32 LE  (DimensionModeMinusTolerance)
    [59..62] = values[8] as float32 LE  (TotalWeightMultiplier)
    [63]     = 0x00

  END (cmd=0x0c, 64 bytes):
    [0]      = 0x0c
    [1..63]  = 0x00

Encryption (bidirectional):
  ALL commands are AES-128-CBC encrypted in BOTH directions.
  Outgoing: plaintext 64 bytes → encrypt → send (+ report ID for HID).
  Incoming: device sends [0x00 (HID report ID)] + 64 encrypted bytes.
  Decrypt with AES-128-CBC, key/IV from FieldRVA in exe.
  Decrypted[0] = cmd echo, Decrypted[1] = 0x01 (success).

Handshake (real device):
  Before uploading, the .exe sends FirmwareVersionRequest (0x04) once,
  then StatusRequest (0x05).  We replicate this to be safe.

Note: This tool bypasses the .NET software's 49-record limit.
"""

import argparse
import glob
import select
import socket
import struct
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pmv_editor import Database

from Crypto.Cipher import AES

AES_KEY = bytes.fromhex('32392013ded44052ae296db75bf03377')
AES_IV  = bytes.fromhex('774abcf022b64aa193d519726f0144bd')

CMD_FIRMWARE = 0x04
CMD_STATUS   = 0x05
CMD_BEGIN    = 0x0a
CMD_RECORD   = 0x0b
CMD_END      = 0x0c


# ---------------------------------------------------------------------------
# Packet builders
# ---------------------------------------------------------------------------

def build_begin_packet(description: str) -> bytes:
    pkt = bytearray(64)
    pkt[0] = CMD_BEGIN
    padded = description.ljust(16)[:16]
    for i, ch in enumerate(padded):
        pkt[1 + i] = ord(ch) & 0xFF
    return bytes(pkt)


def build_record_packet(index: int, record) -> bytes:
    pkt = bytearray(64)
    pkt[0] = CMD_RECORD
    pkt[1] = index & 0xFF
    padded_name = record.name.ljust(24)[:24]
    for i, ch in enumerate(padded_name):
        pkt[2 + i] = ord(ch) & 0xFF
    pkt[26] = record.category_id & 0xFF
    for i, val in enumerate(record.values):
        struct.pack_into('<f', pkt, 27 + i * 4, val)
    return bytes(pkt)


def build_end_packet() -> bytes:
    pkt = bytearray(64)
    pkt[0] = CMD_END
    return bytes(pkt)


def build_firmware_request() -> bytes:
    pkt = bytearray(64)
    pkt[0] = CMD_FIRMWARE
    return bytes(pkt)


def build_status_request() -> bytes:
    pkt = bytearray(64)
    pkt[0] = CMD_STATUS
    return bytes(pkt)


# ---------------------------------------------------------------------------
# Encryption / decryption
# ---------------------------------------------------------------------------

def encrypt_cmd(pkt64: bytes) -> bytes:
    """Encrypt a 64-byte plaintext command → 64 bytes ciphertext."""
    assert len(pkt64) == 64
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    return cipher.encrypt(pkt64)


def decrypt_ack(resp65: bytes) -> bytes:
    """Decrypt 65-byte device response; return 64-byte plaintext."""
    if len(resp65) < 65:
        raise ValueError(f'Response too short: {len(resp65)} bytes (expected 65)')
    cipher = AES.new(AES_KEY, AES.MODE_CBC, AES_IV)
    return cipher.decrypt(resp65[1:65])


def check_ack(resp65: bytes, expected_cmd: int, label: str):
    plain = decrypt_ack(resp65)
    if plain[0] != expected_cmd or plain[1] != 1:
        raise RuntimeError(
            f'{label}: expected cmd=0x{expected_cmd:02x} status=1, '
            f'got cmd=0x{plain[0]:02x} status={plain[1]}'
        )


def check_firmware_ack(resp65: bytes):
    """Check firmware version response — byte[0]=0x04, rest is version string."""
    plain = decrypt_ack(resp65)
    if plain[0] != CMD_FIRMWARE:
        raise RuntimeError(
            f'FIRMWARE: expected cmd=0x04, got cmd=0x{plain[0]:02x}'
        )
    # Extract version string (bytes after cmd until null)
    version_bytes = plain[1:]
    null_pos = version_bytes.find(0)
    if null_pos > 0:
        version = version_bytes[:null_pos].decode('ascii', errors='replace')
    else:
        version = '(unknown)'
    return version


# ---------------------------------------------------------------------------
# Transport: TCP (emulator)
# ---------------------------------------------------------------------------

class TCPTransport:
    def __init__(self, host='127.0.0.1', port=54321):
        self.host = host
        self.port = port
        self._sock = None

    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((self.host, self.port))
        print(f'Connected to emulator at {self.host}:{self.port}')

    def close(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def send_recv(self, pkt64: bytes) -> bytes:
        assert len(pkt64) == 64
        self._sock.sendall(pkt64)
        buf = b''
        while len(buf) < 65:
            chunk = self._sock.recv(65 - len(buf))
            if not chunk:
                raise ConnectionError('Device closed connection unexpectedly')
            buf += chunk
        return buf


# ---------------------------------------------------------------------------
# Transport: USB HID (real device)
# ---------------------------------------------------------------------------

class HIDTransport:
    def __init__(self, vendor_id=0x04D8, product_id=0x0020):
        self.vendor_id = vendor_id
        self.product_id = product_id
        self._dev = None

    def connect(self):
        try:
            import hid
        except ImportError:
            raise ImportError('hidapi not installed. Run: pip install hidapi')

        if not self.vendor_id or not self.product_id:
            import hid
            print('Available HID devices:')
            for d in hid.enumerate():
                print(f"  VID=0x{d['vendor_id']:04x} PID=0x{d['product_id']:04x}  "
                      f"{d.get('manufacturer_string', '')} {d.get('product_string', '')}")
            raise SystemExit('Specify --vid and --pid to select a device.')

        import hid
        self._dev = hid.device()
        self._dev.open(self.vendor_id, self.product_id)
        mfr = self._dev.get_manufacturer_string()
        prd = self._dev.get_product_string()
        print(f'Opened HID device: {mfr} {prd}')

    def close(self):
        if self._dev:
            self._dev.close()
            self._dev = None

    def send_recv(self, pkt64: bytes) -> bytes:
        assert len(pkt64) == 64
        encrypted = encrypt_cmd(pkt64)
        self._dev.write(b'\x00' + encrypted)      # report ID + encrypted payload
        resp = bytes(self._dev.read(65, timeout_ms=5000))
        if len(resp) < 65:
            raise TimeoutError(f'HID read timeout (got {len(resp)} bytes)')
        return resp


# ---------------------------------------------------------------------------
# Transport: Linux /dev/hidraw (no pip install needed)
# ---------------------------------------------------------------------------

def find_pmv_hidraw(vid=0x04D8, pid=0x0020):
    """Find /dev/hidrawN for a given VID/PID. Returns path or None."""
    for uevent_path in sorted(glob.glob('/sys/class/hidraw/hidraw*/device/uevent')):
        try:
            with open(uevent_path) as f:
                for line in f:
                    if line.startswith('HID_ID='):
                        # Format: bus_type:VVVVVVVV:PPPPPPPP (8-digit hex)
                        parts = line.strip().split('=')[1].split(':')
                        if len(parts) == 3:
                            dev_vid = int(parts[1], 16)
                            dev_pid = int(parts[2], 16)
                            if dev_vid == vid and dev_pid == pid:
                                hidraw = uevent_path.split('/')[4]
                                return f'/dev/{hidraw}'
        except (OSError, ValueError):
            continue
    return None


class LinuxHIDTransport:
    """Direct /dev/hidraw access — zero dependencies beyond the kernel."""

    def __init__(self, vendor_id=0x04D8, product_id=0x0020):
        self.vendor_id = vendor_id
        self.product_id = product_id
        self._fd = None

    def connect(self):
        dev_path = find_pmv_hidraw(self.vendor_id, self.product_id)
        if not dev_path:
            raise RuntimeError(
                f'No HID device found with VID=0x{self.vendor_id:04X} '
                f'PID=0x{self.product_id:04X}.\n'
                f'Check: device plugged in? udev rule installed?')
        try:
            self._fd = os.open(dev_path, os.O_RDWR)
        except PermissionError:
            raise PermissionError(
                f'Cannot open {dev_path} — permission denied.\n'
                f'Install the udev rule or run as root.')
        print(f'Opened {dev_path} (VID=0x{self.vendor_id:04X} PID=0x{self.product_id:04X})')

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    def send_recv(self, pkt64: bytes) -> bytes:
        assert len(pkt64) == 64
        encrypted = encrypt_cmd(pkt64)
        os.write(self._fd, b'\x00' + encrypted)
        # Wait for response with 5-second timeout
        ready, _, _ = select.select([self._fd], [], [], 5.0)
        if not ready:
            raise TimeoutError('HID read timeout (5s)')
        resp = os.read(self._fd, 65)
        # hidraw may return 64 (no report ID) or 65 bytes
        if len(resp) == 64:
            resp = b'\x00' + resp
        if len(resp) < 65:
            raise RuntimeError(f'Short HID read: {len(resp)} bytes')
        return resp


# ---------------------------------------------------------------------------
# Upload logic
# ---------------------------------------------------------------------------

def handshake(transport, verbose=True):
    """Send FirmwareVersionRequest + StatusRequest, as the .exe does."""
    def log(msg):
        if verbose:
            print(msg)

    # FirmwareVersionRequest
    log('--> FIRMWARE VERSION REQUEST (0x04)')
    resp = transport.send_recv(build_firmware_request())
    version = check_firmware_ack(resp)
    log(f'<-- Firmware version: {version}')

    # StatusRequest
    log('--> STATUS REQUEST (0x05)')
    resp = transport.send_recv(build_status_request())
    check_ack(resp, CMD_STATUS, 'STATUS')
    log(f'<-- ACK STATUS ok')

    return version


def upload(db: Database, transport, verbose=True):
    def log(msg):
        if verbose:
            print(msg)

    print(f'Uploading "{db.description.rstrip()}" — {len(db.records)} records')

    # Handshake (mimics .exe behavior)
    handshake(transport, verbose=verbose)
    print()

    # BEGIN
    pkt = build_begin_packet(db.description)
    log(f'--> BEGIN  desc={db.description.rstrip()!r}')
    resp = transport.send_recv(pkt)
    check_ack(resp, CMD_BEGIN, 'BEGIN')
    log(f'<-- ACK BEGIN ok')

    # RECORDs
    for i, rec in enumerate(db.records):
        pkt = build_record_packet(i, rec)
        log(f'--> RECORD {i:3d}  {rec.name!r}  cat={rec.category_id}')
        resp = transport.send_recv(pkt)
        check_ack(resp, CMD_RECORD, f'RECORD {i}')
        log(f'<-- ACK RECORD {i} ok')

    # END
    pkt = build_end_packet()
    log(f'--> END')
    resp = transport.send_recv(pkt)
    check_ack(resp, CMD_END, 'END')
    log(f'<-- ACK END ok')

    print(f'Upload complete — {len(db.records)} records sent.')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Upload PMV .dat database to device (bypasses 49-record limit)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('datfile', help='Path to .dat file')
    parser.add_argument('--host',  default='127.0.0.1',
                        help='Emulator host (default: 127.0.0.1)')
    parser.add_argument('--port',  type=int, default=54321,
                        help='Emulator port (default: 54321)')
    parser.add_argument('--hid',   action='store_true',
                        help='Use real USB HID device instead of emulator')
    parser.add_argument('--vid',   type=lambda x: int(x, 0), default=0x04D8,
                        help='HID vendor ID (default: 0x04D8 = Microchip/PMV)')
    parser.add_argument('--pid',   type=lambda x: int(x, 0), default=0x0020,
                        help='HID product ID (default: 0x0020 = PMV)')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress per-packet output')
    args = parser.parse_args()

    print(f'Loading {args.datfile!r}...')
    db = Database.load(args.datfile)
    print(f'  {len(db.records)} records, description={db.description.rstrip()!r}')
    print()

    if args.hid:
        # On Linux, prefer /dev/hidraw (no pip dependency)
        if sys.platform == 'linux':
            transport = LinuxHIDTransport(vendor_id=args.vid, product_id=args.pid)
        else:
            transport = HIDTransport(vendor_id=args.vid, product_id=args.pid)
    else:
        transport = TCPTransport(host=args.host, port=args.port)

    transport.connect()
    try:
        upload(db, transport, verbose=not args.quiet)
    finally:
        transport.close()


if __name__ == '__main__':
    main()
