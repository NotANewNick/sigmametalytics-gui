#!/usr/bin/env python3
"""
pmv_gui.py - Sigma Metalytics PMV Device Interface

Tkinter GUI for the PMV (Precious Metal Verifier) USB HID device.
Provides live measurement display, database management, metal learning,
and wand probe integration.

Features:
  - Auto-detects PMV device via USB HID (VID=0x04D8, PID=0x0020)
  - Live polling of conductivity (%IACS), resistivity (µΩ·cm), thickness, temperature
  - Wand eddy current probe support (SYSTEM_STATUS byte 10)
  - Auto-matches readings against device DB, syncs device screen to detected metal
  - Editable database table with save/load/flash/restore
  - Bar meter with 5-zone display (red/yellow/green/yellow/red)
  - Learn mode: sample a new metal, set thresholds via draggable handles

Threading model:
  All USB communication runs in background threads. A threading.Lock serializes
  transport access. Live polling runs every 500ms. Flash operations stop polling
  first and wait for in-flight polls via a threading.Event (_poll_busy).
  SET_CURRENT_METAL is queued and sent by the poll worker to avoid contention.

Usage:
    python3 pmv_gui.py
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import subprocess
import select
import struct
import math
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pmv_upload import (
    TCPTransport, HIDTransport, LinuxHIDTransport, encrypt_cmd, decrypt_ack,
    build_begin_packet, build_record_packet, build_end_packet,
    build_firmware_request, build_status_request,
    check_ack, check_firmware_ack,
    find_pmv_hidraw,
    AES_KEY, AES_IV,
)
from pmv_editor import Database, Record, FIELD_NAMES, CATEGORIES

# udev rule for Linux USB HID access without root
UDEV_RULE_PATH = '/etc/udev/rules.d/99-pmv.rules'
UDEV_RULE = 'SUBSYSTEM=="hidraw", ATTRS{idVendor}=="04d8", ATTRS{idProduct}=="0020", MODE="0666"'

# ─── Command code lookup ─────────────────────────────────────────────────────
# Maps firmware command bytes to human-readable names for logging

CMD_NAMES = {
    0x01: "VALID_RESPONSE", 0x02: "INVALID_CMD", 0x03: "INVALID_PARAM",
    0x04: "FIRMWARE_VER", 0x05: "STATUS", 0x06: "START_CAL",
    0x07: "CAL_STATUS", 0x08: "CAL_RESULTS", 0x09: "TEMPERATURE",
    0x0a: "BEGIN_DB_DL", 0x0b: "DB_RECORD", 0x0c: "END_DB_DL",
    0x0d: "SET_SENSOR_CFG", 0x0e: "GET_SENSOR_CFG", 0x0f: "STORE_SENSOR_CFG",
    0x10: "FACTORY_CAL", 0x11: "SET_METER_CFG", 0x12: "GET_METER_CFG",
    0x13: "SET_DUMMY", 0x14: "THICKNESS_DATA", 0x15: "LIFTOFF_CAL",
    0x16: "GET_WAND_CFG", 0x17: "ERASE_WAND", 0x18: "SENSOR_DIST_CAL",
    0x19: "SENSOR_DIST_RES", 0x1a: "GET_DEBUG", 0x1b: "THICKNESS_CAL",
    0x1c: "SET_DEBUG", 0x1d: "SYSTEM_INFO", 0x1e: "SYSTEM_STATUS",
    0x1f: "SYSTEM_SETTINGS", 0x20: "TARGET_BULK_RES", 0x21: "WAND_STATUS",
    0x22: "BEGIN_DB_UL", 0x23: "DB_RECORD_UL", 0x24: "SET_METAL",
    0x25: "SET_WEIGHT", 0x26: "DEBUG_LEGEND", 0x27: "ANALOG_VALUES",
    0x28: "DIAG_SYS_STATUS", 0x29: "SET_COLOR",
}


def build_generic_packet(cmd_byte, payload=b''):
    """Build a 64-byte command packet with cmd in byte[0] and optional payload."""
    pkt = bytearray(64)
    pkt[0] = cmd_byte & 0xFF
    pkt[1:1 + min(len(payload), 63)] = payload[:63]
    return bytes(pkt)


def hex_dump(data, width=16):
    """Format binary data as a hex dump with offset, hex bytes, and ASCII."""
    lines = []
    for i in range(0, len(data), width):
        chunk = data[i:i + width]
        hx = ' '.join(f'{b:02x}' for b in chunk)
        asc = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
        lines.append(f'{i:04x}  {hx:<{width * 3}}  {asc}')
    return '\n'.join(lines)


# ─── Main Application ────────────────────────────────────────────────────────

class PMVApp:
    """Main application class — manages connection, tabs, live polling, and flash operations."""

    def __init__(self, root):
        self.root = root
        self.root.title('PMV Device Interface')
        self.root.geometry('960x720')
        self.root.minsize(800, 600)

        # ── Connection state ─────────────────────────────────────────────
        self.transport = None              # Active transport (TCP/HID/LinuxHID)
        self.connected = False             # True when transport is open
        self.loaded_db = None              # Database loaded from file or device
        self._cmd_buttons = []             # Buttons disabled when not connected
        self._lock = threading.Lock()      # Serializes all transport access
        self._emu_proc = None              # Subprocess for TCP emulator
        self._known_hid_paths = set()      # HID paths already probed (avoid re-scan)
        self._is_linux = sys.platform == 'linux'

        # ── Live polling state ───────────────────────────────────────────
        self._live_poll_id = None          # Tkinter after() ID for next poll tick
        self._poll_busy = threading.Event()  # Set while poll worker thread is active
        self._device_db = None             # DB read from device (used for metal matching)
        self._device_current_metal_idx = -1  # Last SET_CURRENT_METAL index sent
        self._pending_metal_idx = None     # Queued index for next poll cycle (avoids contention)

        self._build_connection_panel()
        self._build_notebook()
        self._build_log_panel()

        self._set_buttons_state('disabled')
        self.root.protocol('WM_DELETE_WINDOW', self._on_close)

        # Start device auto-detection: initial scan at 500ms, then every 3s
        self.root.after(500, self._scan_for_device)
        self.root.after(3000, self._device_monitor_tick)

    # ── Connection panel ──────────────────────────────────────────────────

    def _build_connection_panel(self):
        fr = ttk.LabelFrame(self.root, text='Connection', padding=6)
        fr.pack(fill='x', padx=6, pady=(6, 0))

        ttk.Label(fr, text='Mode:').grid(row=0, column=0, padx=(0, 4))
        self._mode_var = tk.StringVar(value='TCP')
        mode_cb = ttk.Combobox(fr, textvariable=self._mode_var,
                               values=['TCP', 'HID'], state='readonly', width=5)
        mode_cb.grid(row=0, column=1, padx=(0, 12))
        mode_cb.bind('<<ComboboxSelected>>', self._on_mode_change)

        # TCP fields
        self._tcp_frame = ttk.Frame(fr)
        self._tcp_frame.grid(row=0, column=2)
        ttk.Label(self._tcp_frame, text='Host:').pack(side='left')
        self._host_var = tk.StringVar(value='127.0.0.1')
        ttk.Entry(self._tcp_frame, textvariable=self._host_var, width=14).pack(side='left', padx=(2, 8))
        ttk.Label(self._tcp_frame, text='Port:').pack(side='left')
        self._port_var = tk.StringVar(value='54321')
        ttk.Entry(self._tcp_frame, textvariable=self._port_var, width=6).pack(side='left', padx=(2, 0))
        self._emu_btn = ttk.Button(self._tcp_frame, text='Start Emulator',
                                   command=self._toggle_emulator)
        self._emu_btn.pack(side='left', padx=(10, 0))

        # HID fields (hidden by default)
        self._hid_frame = ttk.Frame(fr)
        ttk.Label(self._hid_frame, text='VID:').pack(side='left')
        self._vid_var = tk.StringVar(value='0x04D8')
        ttk.Entry(self._hid_frame, textvariable=self._vid_var, width=8).pack(side='left', padx=(2, 8))
        ttk.Label(self._hid_frame, text='PID:').pack(side='left')
        self._pid_var = tk.StringVar(value='0x0020')
        ttk.Entry(self._hid_frame, textvariable=self._pid_var, width=8).pack(side='left', padx=(2, 0))

        self._conn_btn = ttk.Button(fr, text='Connect', command=self._on_connect)
        self._conn_btn.grid(row=0, column=3, padx=(16, 4))
        self._disc_btn = ttk.Button(fr, text='Disconnect', command=self._on_disconnect,
                                    state='disabled')
        self._disc_btn.grid(row=0, column=4, padx=(0, 12))

        self._status_var = tk.StringVar(value='Disconnected')
        self._status_lbl = ttk.Label(fr, textvariable=self._status_var, foreground='red')
        self._status_lbl.grid(row=0, column=5, padx=(8, 0))

    def _toggle_emulator(self):
        if self._emu_proc and self._emu_proc.poll() is None:
            # Disconnect first if connected via TCP
            if self.connected:
                self._on_disconnect()
            self._emu_proc.terminate()
            self._emu_proc = None
            self._emu_btn.config(text='Start Emulator')
            self._log_msg('Emulator stopped')
            return
        # Start it
        emu_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pmv_emulator.py')
        self._emu_proc = subprocess.Popen(
            [sys.executable, emu_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self._emu_btn.config(text='Stop Emulator')
        self._log_msg(f'Emulator started (PID {self._emu_proc.pid})')
        # Auto-connect after a short delay to let the server bind
        self._mode_var.set('TCP')
        self._on_mode_change()
        self.root.after(800, self._on_connect)

    def _on_mode_change(self, _event=None):
        if self._mode_var.get() == 'TCP':
            self._hid_frame.grid_remove()
            self._tcp_frame.grid(row=0, column=2)
        else:
            self._tcp_frame.grid_remove()
            self._hid_frame.grid(row=0, column=2)

    def _on_connect(self):
        """Open a transport connection in a background thread to avoid blocking the UI."""
        self._conn_btn.config(state='disabled')
        mode = self._mode_var.get()

        def worker():
            try:
                if mode == 'TCP':
                    t = TCPTransport(self._host_var.get(), int(self._port_var.get()))
                elif self._is_linux:
                    t = LinuxHIDTransport(int(self._vid_var.get(), 0), int(self._pid_var.get(), 0))
                else:
                    t = HIDTransport(int(self._vid_var.get(), 0), int(self._pid_var.get(), 0))
                t.connect()
                self.root.after(0, self._connect_done, t, None)
            except Exception as e:
                self.root.after(0, self._connect_done, None, e)

        threading.Thread(target=worker, daemon=True).start()

    def _connect_done(self, transport, err):
        """Connection callback. On success, auto-reads device DB and starts live polling."""
        if err:
            self._conn_btn.config(state='normal')
            self._status_var.set(f'Error')
            self._status_lbl.config(foreground='red')
            self._log_msg(f'Connection failed: {err}')
            # Friendly message for missing hidapi on Windows
            if isinstance(err, ImportError) and 'hidapi' in str(err):
                messagebox.showerror('Missing Dependency',
                    'The "hidapi" package is required for USB HID on Windows.\n\n'
                    'Open a command prompt and run:\n'
                    '    pip install hidapi\n\n'
                    'Then restart this application.')
            return
        self.transport = transport
        self.connected = True
        self._conn_btn.config(state='disabled')
        self._disc_btn.config(state='normal')
        self._status_var.set('Connected')
        self._status_lbl.config(foreground='green')
        self._set_buttons_state('normal')
        self._log_msg(f'Connected ({self._mode_var.get()})')
        self._auto_read_device_db()  # live poll starts after DB read completes

    def _on_disconnect(self):
        self._live_poll_stop()
        if self.transport:
            try:
                self.transport.close()
            except Exception:
                pass
        self.transport = None
        self.connected = False
        self._conn_btn.config(state='normal')
        self._disc_btn.config(state='disabled')
        self._status_var.set('Disconnected')
        self._status_lbl.config(foreground='red')
        self._set_buttons_state('disabled')
        self._dev_info_var.set('Not connected')
        self._device_current_metal_idx = -1
        self._log_msg('Disconnected')

    # ── Auto-read device database on connect ────────────────────────

    def _auto_read_device_db(self):
        """Read system info + database from the device in background on connect."""
        self._device_db = None
        self._log_msg('Reading device info...')

        def worker():
            records = []
            try:
                # ── Query system info ────────────────────────────────
                info_parts = {}

                # Firmware version (0x04)
                try:
                    pkt = build_firmware_request()
                    with self._lock:
                        resp = self.transport.send_recv(pkt)
                    ver = check_firmware_ack(resp)
                    info_parts['Firmware'] = ver
                except Exception:
                    pass

                # System Info (0x1d) — typically contains serial, FPGA, DB name
                try:
                    pkt = build_generic_packet(0x1d)
                    with self._lock:
                        resp = self.transport.send_recv(pkt)
                    plain = decrypt_ack(resp)
                    if plain[0] not in (0x02, 0x03):
                        # Header: [0]=cmd, [1..8]=binary fields, [9..]=DB name
                        db_name = plain[9:].decode('latin-1').rstrip('\x00').strip()
                        if db_name:
                            info_parts['System'] = db_name
                except Exception:
                    pass

                # System Info is the DB name — label it accordingly
                if 'System' in info_parts:
                    info_parts['Database'] = info_parts.pop('System')

                info_str = '  |  '.join(f'{k}: {v}' for k, v in info_parts.items())
                db_label = info_parts.get('Database', '')
                self.root.after(0, lambda s=info_str: self._dev_info_var.set(s or 'Connected'))
                self.root.after(0, lambda s=db_label: self._read_dbname_var.set(s))

                # ── Read database ────────────────────────────────────
                self._safe_log('Reading database from device...')

                # BEGIN_DB_UPLOAD — byte[1] is the record count
                pkt = build_generic_packet(0x22)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                if plain[0] != 0x22:
                    self.root.after(0, self._auto_read_done, [],
                                    f'Unexpected cmd 0x{plain[0]:02x}')
                    return

                record_count = plain[1]
                self._safe_log(f'Device reports {record_count} records')

                for idx in range(record_count):
                    pkt = build_generic_packet(0x23, bytes([idx]))
                    with self._lock:
                        resp = self.transport.send_recv(pkt)
                    plain = decrypt_ack(resp)

                    if plain[0] in (0x02, 0x03):
                        self._safe_log(f'Record {idx}: error 0x{plain[0]:02x}, stopping')
                        break

                    name = plain[2:26].decode('latin-1').rstrip('\x00').rstrip()
                    cat_id = plain[26]
                    vals = [struct.unpack_from('<f', plain, 27 + i * 4)[0]
                            for i in range(9)]
                    records.append(Record(name, cat_id, vals))

                self.root.after(0, self._auto_read_done, records, None)
            except Exception as e:
                self._safe_log(f'DB read error: {e}')
                self.root.after(0, self._auto_read_done, records, str(e))

        threading.Thread(target=worker, daemon=True).start()

    def _auto_read_done(self, records, err):
        """Callback after auto-reading device DB on connect. Populates the Database Read
        tab, creates a backup for restore, and starts live polling."""
        if err:
            self._log_msg(f'DB read failed: {err}')
            if self.loaded_db:
                self._device_db = self.loaded_db
                self._log_msg(f'Using loaded .dat file ({len(self.loaded_db.records)} records)')
        elif records:
            self._device_db = Database(
                description='Device DB',
                timestamp=datetime.now().strftime('%m/%d/%Y %I:%M:%S %p'),
                records=records)
            self._log_msg(f'Read {len(records)} records from device')
            self.loaded_db = self._device_db
            self._learn_populate_f0()
            # Populate the Database Read tab table
            self._read_tree.delete(*self._read_tree.get_children())
            for i, rec in enumerate(records):
                self._read_add_row(i, rec)
            self._read_status_var.set(f'{len(records)} records read from device')
            self._read_db = self._device_db
            self._read_db_backup = Database(
                description=self._device_db.description,
                timestamp=self._device_db.timestamp,
                records=[rec.clone() for rec in records])
            self._read_dirty = False
            self._read_restore_btn.pack_forget()
            self._read_save_btn.config(state='normal')
            self._read_flash_btn.config(state='normal' if self.connected else 'disabled')
        else:
            self._log_msg('No records read from device')
            if self.loaded_db:
                self._device_db = self.loaded_db
        # Start live polling now that DB is available
        self._live_poll_start()

    # ── Metal matching ───────────────────────────────────────────────

    def _match_metal(self, resistivity):
        """Match a resistivity value against the Database Read tab's current records.
        Returns list of (display_name, zone, record, index) for all matches, best first."""
        db = getattr(self, '_read_db', None) or self._device_db
        if not db:
            return []

        greens = []
        yellows = []

        for idx, rec in enumerate(db.records):
            f1 = rec.values[1]  # ResYellowLeft  (lowest resistivity boundary)
            f2 = rec.values[2]  # ResGreenLeft   (green zone low boundary)
            f3 = rec.values[3]  # ResGreenRight  (green zone high boundary)
            f4 = rec.values[4]  # ResYellowRight (highest resistivity boundary)

            cat = CATEGORIES.get(rec.category_id, '?')
            name = rec.name.strip()
            display = f'{name} ({cat})'

            if f2 <= resistivity <= f3:
                greens.append((display, 'GREEN', rec, idx))
            elif f1 <= resistivity < f2 or f3 < resistivity <= f4:
                yellows.append((display, 'YELLOW', rec, idx))

        return greens + yellows

    def _set_device_metal(self, idx):
        """Queue SET_CURRENT_METAL (0x24) — sent by next poll cycle."""
        self._pending_metal_idx = idx

    # ── Continuous live polling (runs whenever connected) ────────────

    def _live_poll_start(self):
        """Begin the 500ms polling loop for live readings."""
        self._live_poll_id = None
        self._live_poll_tick()

    def _live_poll_stop(self):
        """Cancel the next scheduled poll tick. Does NOT wait for an in-flight poll —
        callers that need to use the transport should also wait on _poll_busy."""
        if hasattr(self, '_live_poll_id') and self._live_poll_id:
            self.root.after_cancel(self._live_poll_id)
            self._live_poll_id = None

    def _live_poll_tick(self):
        """Schedule a background worker to read THICKNESS_DATA, SYSTEM_STATUS (wand),
        TEMPERATURE, and send any queued SET_CURRENT_METAL."""
        if not self.connected:
            return
        # Skip if the learn tab is actively sampling (it has its own poll loop)
        if self._learn_collecting:
            self._live_poll_id = self.root.after(500, self._live_poll_tick)
            return

        def worker():
            self._poll_busy.set()
            try:
                readings = {}

                # THICKNESS_DATA (0x14) — main sensor
                pkt = build_generic_packet(0x14)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                fields = [struct.unpack_from('<f', plain, 1 + i * 4)[0] for i in range(9)]
                resistivity = fields[5]
                thickness = fields[3]
                if resistivity > 0.01:
                    readings['iacs'] = 100.0 / resistivity
                    readings['resistivity'] = resistivity
                if thickness > 0.01:
                    readings['thickness'] = thickness

                # SYSTEM_STATUS (0x1e) — wand resistivity at byte offset 10
                pkt = build_generic_packet(0x1e)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                if plain[0] not in (0x02, 0x03) and len(plain) >= 14:
                    wand_res = struct.unpack_from('<f', plain, 10)[0]
                    if 0.1 < wand_res < 500:  # valid resistivity range
                        readings['wand_resistivity'] = wand_res
                        # Prefer wand over main sensor
                        readings['iacs'] = 100.0 / wand_res
                        readings['resistivity'] = wand_res
                        readings['source'] = 'wand'

                # TEMPERATURE (0x09)
                pkt = build_generic_packet(0x09)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                temp = struct.unpack_from('<f', plain, 1)[0]
                if 0 < temp < 100:
                    readings['temperature'] = temp

                # SET_CURRENT_METAL (0x24) if queued
                pending = self._pending_metal_idx
                if pending is not None:
                    self._pending_metal_idx = None
                    pkt = build_generic_packet(0x24, bytes([pending & 0xFF]))
                    with self._lock:
                        self.transport.send_recv(pkt)

                self.root.after(0, self._live_poll_update, readings)
            except Exception:
                self.root.after(0, self._live_poll_update, {})
            finally:
                self._poll_busy.clear()

        threading.Thread(target=worker, daemon=True).start()
        self._live_poll_id = self.root.after(500, self._live_poll_tick)

    def _live_poll_update(self, readings):
        """Update the GUI with fresh readings from the poll worker. Runs on GUI thread.
        Also performs metal matching and syncs device screen when a match changes."""
        src = ' (wand)' if readings.get('source') == 'wand' else ''
        if 'iacs' in readings:
            self._learn_iacs_var.set(f'{readings["iacs"]:.2f} %IACS{src}')
            self._learn_live_value = readings['iacs']
        else:
            self._learn_iacs_var.set('-- %IACS')
            self._learn_live_value = None
        if 'resistivity' in readings:
            self._learn_res_var.set(f'{readings["resistivity"]:.4f} µΩ·cm{src}')
        else:
            self._learn_res_var.set('-- µΩ·cm')
        if 'thickness' in readings:
            self._learn_thick_var.set(f'{readings["thickness"]:.3f} mm')
        else:
            self._learn_thick_var.set('-- mm')
        if 'temperature' in readings:
            self._learn_temp_var.set(f'{readings["temperature"]:.1f} °C')
        else:
            self._learn_temp_var.set('-- °C')

        # Metal matching
        if 'resistivity' in readings:
            matches = self._match_metal(readings['resistivity'])
            if matches:
                top_name, top_zone, top_rec, top_idx = matches[0]
                count = len(matches)
                if top_zone == 'GREEN':
                    if count > 1:
                        self._learn_match_var.set(f'{top_name} (+{count-1} more)')
                    else:
                        self._learn_match_var.set(top_name)
                    self._learn_match_lbl.config(fg='#00cc00')
                else:
                    self._learn_match_var.set(f'{top_name} - YELLOW')
                    self._learn_match_lbl.config(fg='#ccaa00')
                # Set bar zones from matched record
                self._learn_scan_rec = top_rec
                # Sync device screen to matched metal (only on change, and only
                # if DB hasn't been edited — edited DB may not match device)
                if top_idx != self._device_current_metal_idx:
                    self._device_current_metal_idx = top_idx
                    if not getattr(self, '_read_dirty', False):
                        self._set_device_metal(top_idx)
            else:
                self._learn_match_var.set('No match')
                self._learn_match_lbl.config(fg='#888888')
                self._learn_scan_rec = None
        else:
            self._learn_match_var.set('--')
            self._learn_match_lbl.config(fg='#888888')
            self._learn_scan_rec = None
            # Reset so next reading triggers a fresh SET_CURRENT_METAL
            self._device_current_metal_idx = -1

        # Update the bar needle if on the learn tab and not sampling
        if not self._learn_collecting and self._learn_live_value is not None:
            self._learn_redraw()

    def _on_close(self):
        self._live_poll_stop()
        if self._emu_proc and self._emu_proc.poll() is None:
            self._emu_proc.terminate()
        if self.transport:
            try:
                self.transport.close()
            except Exception:
                pass
        self.root.destroy()

    def _set_buttons_state(self, state):
        for btn in self._cmd_buttons:
            btn.config(state=state)

    # ── HID device auto-detection ─────────────────────────────────────────

    def _probe_hidraw(self, dev_path):
        """Probe a /dev/hidrawN device with firmware request. Returns version or None."""
        try:
            fd = os.open(dev_path, os.O_RDWR | os.O_NONBLOCK)
            try:
                pkt = build_firmware_request()
                encrypted = encrypt_cmd(pkt)
                os.write(fd, b'\x00' + encrypted)
                ready, _, _ = select.select([fd], [], [], 2.0)
                if not ready:
                    return None
                resp = os.read(fd, 65)
                if len(resp) == 64:
                    resp = b'\x00' + resp
                if len(resp) < 65:
                    return None
                plain = decrypt_ack(resp)
                if plain[0] != 0x04:
                    return None
                null = plain[1:].find(0)
                return plain[1:1 + null].decode('ascii', errors='replace') if null > 0 else 'unknown'
            finally:
                os.close(fd)
        except Exception:
            return None

    def _probe_hidapi(self, vid, pid):
        """Probe via hidapi (Windows / fallback). Returns version or None."""
        try:
            import hid
            dev = hid.device()
            dev.open(vid, pid)
            pkt = build_firmware_request()
            encrypted = encrypt_cmd(pkt)
            dev.write(b'\x00' + encrypted)
            resp = bytes(dev.read(65, timeout_ms=2000))
            dev.close()
            if not resp:
                return None
            if len(resp) == 64:
                resp = b'\x00' + resp
            if len(resp) < 65:
                return None
            plain = decrypt_ack(resp)
            if plain[0] != 0x04:
                return None
            null = plain[1:].find(0)
            return plain[1:1 + null].decode('ascii', errors='replace') if null > 0 else 'unknown'
        except Exception:
            return None

    def _check_udev_rule(self):
        """Check if the PMV udev rule is installed. Offer to install if missing."""
        if not self._is_linux:
            return
        if os.path.exists(UDEV_RULE_PATH):
            return
        # Found a device but no udev rule — offer to install
        if not messagebox.askyesno(
                'USB Permission Setup',
                'PMV device detected but a udev rule is needed for USB access '
                'without root.\n\n'
                'Install it now? (will ask for your password)\n\n'
                f'Rule: {UDEV_RULE_PATH}'):
            return
        try:
            # Use pkexec for graphical sudo, fall back to sudo
            import shutil
            installer = 'pkexec' if shutil.which('pkexec') else 'sudo'
            subprocess.run(
                [installer, 'bash', '-c',
                 f'echo \'{UDEV_RULE}\' > {UDEV_RULE_PATH} && '
                 f'udevadm control --reload-rules && udevadm trigger'],
                check=True)
            self._log_msg('udev rule installed — unplug and replug the device')
            messagebox.showinfo('Done', 'udev rule installed.\n\n'
                                        'Unplug and replug the PMV device.')
        except Exception as e:
            self._log_msg(f'udev install failed: {e}')
            messagebox.showerror('Error', f'Failed to install udev rule:\n{e}')

    def _scan_for_device(self):
        """Background scan for PMV HID devices. If found, switch to HID mode."""
        if self.connected:
            return

        def worker():
            found_ver = None

            if self._is_linux:
                # Scan /sys/class/hidraw — no pip dependency
                dev_path = find_pmv_hidraw(0x04D8, 0x0020)
                if dev_path and dev_path not in self._known_hid_paths:
                    found_ver = self._probe_hidraw(dev_path)
                    if found_ver:
                        self._known_hid_paths.add(dev_path)
                    elif dev_path not in self._known_hid_paths:
                        # Device present but probe failed — likely permissions
                        self._known_hid_paths.add(dev_path)
                        self.root.after(0, self._check_udev_rule)
                        return
            else:
                # Windows/macOS — use hidapi
                try:
                    import hid
                    for dev_info in hid.enumerate():
                        vid = dev_info['vendor_id']
                        pid = dev_info['product_id']
                        path = dev_info['path']
                        if path in self._known_hid_paths:
                            continue
                        self._known_hid_paths.add(path)
                        if vid == 0x04D8 and pid == 0x0020:
                            found_ver = self._probe_hidapi(vid, pid)
                            if found_ver:
                                break
                except ImportError:
                    pass

            if found_ver:
                self.root.after(0, self._device_found, 0x04D8, 0x0020, found_ver)

        threading.Thread(target=worker, daemon=True).start()

    def _device_found(self, vid, pid, version):
        """Called on GUI thread when a PMV device is detected."""
        if self.connected:
            return
        self._mode_var.set('HID')
        self._on_mode_change()
        self._vid_var.set(f'0x{vid:04X}')
        self._pid_var.set(f'0x{pid:04X}')
        self._status_var.set(f'PMV found (v{version}) — click Connect')
        self._status_lbl.config(foreground='#CC8800')
        self._log_msg(f'PMV device detected: VID=0x{vid:04X} PID=0x{pid:04X} firmware v{version}')

    def _device_monitor_tick(self):
        """Periodic check for newly plugged-in HID devices (every 3 seconds)."""
        if not self.connected:
            self._scan_for_device()
        self.root.after(3000, self._device_monitor_tick)

    # ── Communication core ────────────────────────────────────────────────

    def _run_command(self, pkt64, callback):
        """Send 64-byte packet in background, decrypt response, call callback(plain, err) on GUI thread."""
        if not self.connected:
            self.root.after(0, callback, None, RuntimeError('Not connected'))
            return

        cmd = pkt64[0]
        name = CMD_NAMES.get(cmd, f'0x{cmd:02x}')
        self._log_msg(f'--> {name}  {pkt64[:16].hex(" ")} ...')

        def worker():
            try:
                with self._lock:
                    resp65 = self.transport.send_recv(pkt64)
                plain = decrypt_ack(resp65)
                self._safe_log(f'<-- {name}  {plain[:16].hex(" ")} ...')
                self.root.after(0, callback, plain, None)
            except Exception as e:
                self._safe_log(f'<-- {name}  ERROR: {e}')
                self.root.after(0, callback, None, e)

        threading.Thread(target=worker, daemon=True).start()

    def _safe_log(self, msg):
        self.root.after(0, self._log_msg, msg)

    # ── Log panel ─────────────────────────────────────────────────────────

    def _build_log_panel(self):
        fr = ttk.LabelFrame(self.root, text='Log', padding=4)
        fr.pack(fill='both', expand=False, padx=6, pady=(4, 6))
        fr.configure(height=140)
        fr.pack_propagate(False)

        top = ttk.Frame(fr)
        top.pack(fill='x')
        ttk.Button(top, text='Clear', command=self._log_clear).pack(side='right')

        self._log_text = tk.Text(fr, height=6, font=('Consolas', 9), state='disabled',
                                 wrap='none', bg='#1e1e1e', fg='#cccccc',
                                 insertbackground='white')
        sb = ttk.Scrollbar(fr, command=self._log_text.yview)
        self._log_text.config(yscrollcommand=sb.set)
        sb.pack(side='right', fill='y')
        self._log_text.pack(fill='both', expand=True)

    def _log_msg(self, msg):
        ts = datetime.now().strftime('%H:%M:%S')
        self._log_text.config(state='normal')
        self._log_text.insert('end', f'{ts}  {msg}\n')
        self._log_text.see('end')
        self._log_text.config(state='disabled')

    def _log_clear(self):
        self._log_text.config(state='normal')
        self._log_text.delete('1.0', 'end')
        self._log_text.config(state='disabled')

    # ── Notebook ──────────────────────────────────────────────────────────

    def _build_notebook(self):
        self._nb = ttk.Notebook(self.root)
        self._nb.pack(fill='both', expand=True, padx=6, pady=4)

        self._build_learn_tab()
        self._build_read_tab()
        # self._build_wand_tab()  # hidden — wand readings integrated into Learn tab
        self._nb.select(0)  # Learn Metal tab first

    # ── Helper: add a command row to a tab ────────────────────────────────

    def _add_cmd_row(self, parent, row, label, cmd_byte, has_payload=False,
                     section_label=None):
        """Add [Button] [optional Payload entry] [Response label] to a grid row.
        Returns (resp_var, payload_var_or_None)."""
        if section_label:
            sep = ttk.Label(parent, text=section_label, font=('TkDefaultFont', 10, 'bold'))
            sep.grid(row=row, column=0, columnspan=4, sticky='w', pady=(10, 2), padx=4)
            return None, None

        payload_var = tk.StringVar() if has_payload else None

        def on_click():
            payload = b''
            if payload_var and payload_var.get().strip():
                try:
                    payload = bytes.fromhex(payload_var.get().replace(' ', ''))
                except ValueError:
                    resp_var.set('ERROR: invalid hex payload')
                    return
            pkt = build_generic_packet(cmd_byte, payload)

            def cb(plain, err):
                if err:
                    resp_var.set(f'ERROR: {err}')
                else:
                    resp_var.set(plain.hex(' '))
            self._run_command(pkt, cb)

        btn = ttk.Button(parent, text=label, command=on_click, width=22)
        btn.grid(row=row, column=0, sticky='w', padx=4, pady=2)
        self._cmd_buttons.append(btn)

        col = 1
        if has_payload:
            ttk.Label(parent, text='Payload:').grid(row=row, column=col, padx=(4, 2))
            col += 1
            e = ttk.Entry(parent, textvariable=payload_var, width=30)
            e.grid(row=row, column=col, padx=(0, 4))
            col += 1

        resp_var = tk.StringVar(value='--')
        ttk.Label(parent, textvariable=resp_var, anchor='w',
                  wraplength=400).grid(row=row, column=col, columnspan=4-col,
                                       sticky='w', padx=4)
        return resp_var, payload_var

    # ══════════════════════════════════════════════════════════════════════
    # TAB 1: Database Flash
    # ══════════════════════════════════════════════════════════════════════

    def _build_flash_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Database Flash')

        # Top bar
        top = ttk.Frame(tab)
        top.pack(fill='x')
        load_btn = ttk.Button(top, text='Load .dat File', command=self._flash_load)
        load_btn.pack(side='left')
        self._flash_file_lbl = ttk.Label(top, text='  No file loaded')
        self._flash_file_lbl.pack(side='left', padx=8)

        # Treeview
        cols = ('#', 'Name', 'Category', 'ResGrL', 'ResYelL', 'ResGrR', 'ResYelR')
        self._flash_tree = ttk.Treeview(tab, columns=cols, show='headings', height=14)
        for c in cols:
            self._flash_tree.heading(c, text=c)
            w = 40 if c == '#' else 80 if c == 'Category' else 140 if c == 'Name' else 70
            self._flash_tree.column(c, width=w, minwidth=40)
        tree_sb = ttk.Scrollbar(tab, orient='vertical', command=self._flash_tree.yview)
        self._flash_tree.config(yscrollcommand=tree_sb.set)
        self._flash_tree.pack(fill='both', expand=True, pady=(6, 0))
        tree_sb.place(relx=1.0, rely=0.0, relheight=1.0, anchor='ne',
                      in_=self._flash_tree)

        # Bottom bar
        bot = ttk.Frame(tab)
        bot.pack(fill='x', pady=(6, 0))
        self._flash_btn = ttk.Button(bot, text='Flash to Device', command=self._flash_upload)
        self._flash_btn.pack(side='left')
        self._cmd_buttons.append(self._flash_btn)
        self._flash_prog = ttk.Progressbar(bot, mode='determinate', length=300)
        self._flash_prog.pack(side='left', padx=12)
        self._flash_status = ttk.Label(bot, text='')
        self._flash_status.pack(side='left')

    def _flash_load(self):
        path = filedialog.askopenfilename(
            title='Open PMV database',
            filetypes=[('DAT files', '*.dat'), ('All files', '*.*')])
        if not path:
            return
        try:
            db = Database.load(path)
        except Exception as e:
            messagebox.showerror('Load Error', str(e))
            return
        self.loaded_db = db
        self._flash_file_lbl.config(
            text=f'  {os.path.basename(path)}  —  {len(db.records)} records, '
                 f'desc={db.description.rstrip()!r}')
        # Populate tree
        self._flash_load_db(db)
        # Update the Learn tab's ResGreenLeft dropdown
        self._learn_populate_f0()

    def _flash_upload(self):
        if not self.connected or not self.loaded_db:
            return
        db = self.loaded_db
        total = len(db.records)
        if total > 49:
            messagebox.showerror('Too Many Records',
                                 f'Database has {total} records but the device limit is 49.\n\n'
                                 f'Remove {total - 49} record(s) in the Database Read tab before flashing.')
            return
        self._flash_prog['maximum'] = total + 2  # handshake + begin + records + end
        self._flash_prog['value'] = 0
        self._flash_btn.config(state='disabled')
        self._flash_status.config(text='Starting...')

        def worker():
            try:
                t = self.transport
                lock = self._lock

                # Handshake
                with lock:
                    resp = t.send_recv(build_firmware_request())
                ver = check_firmware_ack(resp)
                self._safe_log(f'<-- Firmware: {ver}')
                self.root.after(0, self._flash_progress, 0, total, 'Handshake...')

                with lock:
                    resp = t.send_recv(build_status_request())
                check_ack(resp, 0x05, 'STATUS')

                # BEGIN
                with lock:
                    resp = t.send_recv(build_begin_packet(db.description))
                check_ack(resp, 0x0a, 'BEGIN')
                self._safe_log(f'--> BEGIN desc={db.description.rstrip()!r}')

                # RECORDS
                for i, rec in enumerate(db.records):
                    with lock:
                        resp = t.send_recv(build_record_packet(i, rec))
                    check_ack(resp, 0x0b, f'RECORD {i}')
                    self.root.after(0, self._flash_progress, i + 1, total,
                                    f'Record {i+1}/{total}  {rec.name.strip()}')

                # END
                with lock:
                    resp = t.send_recv(build_end_packet())
                check_ack(resp, 0x0c, 'END')
                self._safe_log(f'--> END')
                self.root.after(0, self._flash_done, None)
            except Exception as e:
                self.root.after(0, self._flash_done, e)

        threading.Thread(target=worker, daemon=True).start()

    def _flash_progress(self, current, total, text):
        self._flash_prog['value'] = current
        self._flash_status.config(text=text)

    def _flash_done(self, err):
        self._flash_btn.config(state='normal' if self.connected else 'disabled')
        if err:
            self._flash_status.config(text=f'FAILED: {err}')
            self._log_msg(f'Flash failed: {err}')
            messagebox.showerror('Flash Error', str(err))
        else:
            n = len(self.loaded_db.records)
            self._flash_prog['value'] = self._flash_prog['maximum']
            self._flash_status.config(text=f'Done! {n} records flashed.')
            self._log_msg(f'Flash complete: {n} records')

    # ══════════════════════════════════════════════════════════════════════
    # TAB 2: Database Read
    # ══════════════════════════════════════════════════════════════════════

    def _build_read_tab(self):
        """Build the Database Read tab with editable treeview and toolbar buttons.
        Toolbar: DB Name field, Save as .dat, Load from File, Save to Device, Restore DB.
        The Restore DB button is hidden until the user edits, loads, or flashes."""
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Database Read')

        top = ttk.Frame(tab)
        top.pack(fill='x')

        ttk.Label(top, text='DB Name:').pack(side='left')
        self._read_dbname_var = tk.StringVar(value='')
        self._read_dbname_entry = ttk.Entry(top, textvariable=self._read_dbname_var, width=25)
        self._read_dbname_entry.pack(side='left', padx=(4, 12))

        self._read_save_btn = ttk.Button(top, text='Save as .dat', command=self._read_save,
                                         state='disabled')
        self._read_save_btn.pack(side='left', padx=(0, 4))

        self._read_load_btn = ttk.Button(top, text='Load from File', command=self._read_load_file)
        self._read_load_btn.pack(side='left', padx=(0, 4))

        self._read_flash_btn = ttk.Button(top, text='Save to Device', command=self._read_flash,
                                          state='disabled')
        self._read_flash_btn.pack(side='left', padx=(0, 4))
        self._cmd_buttons.append(self._read_flash_btn)

        self._read_restore_btn = ttk.Button(top, text='Restore DB', command=self._read_restore)
        # Not packed yet — shown only after an edit

        self._read_status_var = tk.StringVar(value='')
        ttk.Label(top, textvariable=self._read_status_var).pack(side='left', padx=8)

        # Treeview with all fields
        self._read_cols = ('#', 'Name', 'Category',
                           'ResGrL', 'ResYelL', 'ResGrR', 'ResYelR', 'Field5',
                           'SG', 'Dim+', 'Dim-', 'WtMul')
        tree_fr = ttk.Frame(tab)
        tree_fr.pack(fill='both', expand=True, pady=(6, 0))

        self._read_tree = ttk.Treeview(tree_fr, columns=self._read_cols,
                                       show='headings', height=14)
        vsb = ttk.Scrollbar(tree_fr, orient='vertical', command=self._read_tree.yview)
        hsb = ttk.Scrollbar(tree_fr, orient='horizontal', command=self._read_tree.xview)
        self._read_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self._read_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        tree_fr.rowconfigure(0, weight=1)
        tree_fr.columnconfigure(0, weight=1)

        for c in self._read_cols:
            self._read_tree.heading(c, text=c)
            w = 35 if c == '#' else 130 if c == 'Name' else 70 if c == 'Category' else 62
            self._read_tree.column(c, width=w, minwidth=35)

        # Double-click to edit a cell
        self._read_tree.bind('<Double-1>', self._read_on_dblclick)

        # Inline edit widget (created on demand)
        self._read_edit_entry = None
        self._read_edit_item = None
        self._read_edit_col = None

        self._read_db = None
        self._read_db_backup = None  # deep copy stored on first read
        self._read_dirty = False
        self._read_max_records = 49

    def _read_from_device(self):
        if not self.connected:
            return
        self._read_btn.config(state='disabled')
        self._read_tree.delete(*self._read_tree.get_children())
        self._read_status_var.set('Reading...')
        self._read_db = None

        def worker():
            records = []
            try:
                # BEGIN_DATABASE_UPLOAD
                pkt = build_generic_packet(0x22)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                self._safe_log(f'<-- BEGIN_DB_UPLOAD: {plain.hex(" ")}')

                if plain[0] != 0x22:
                    raise RuntimeError(f'Unexpected response cmd 0x{plain[0]:02x}')

                # Try reading records via cmd 0x23 with index in byte[1]
                for idx in range(self._read_max_records):
                    pkt = build_generic_packet(0x23, bytes([idx]))
                    with self._lock:
                        resp = self.transport.send_recv(pkt)
                    plain = decrypt_ack(resp)

                    # Stop on error response
                    if plain[0] in (0x02, 0x03) or (plain[0] == 0x23 and plain[1] == 0x00
                                                     and all(b == 0 for b in plain[2:])):
                        break

                    # Attempt to parse like a 0x0b record format:
                    # [0]=cmd, [1]=index, [2:26]=name, [26]=category, [27:63]=9×float32
                    try:
                        name = plain[2:26].decode('latin-1').rstrip('\x00').rstrip()
                        cat_id = plain[26]
                        vals = [struct.unpack_from('<f', plain, 27 + i * 4)[0]
                                for i in range(9)]
                        rec = Record(name, cat_id, vals)
                        records.append(rec)
                        self.root.after(0, self._read_add_row, idx, rec)
                        self.root.after(0, self._read_status_var.set,
                                        f'Read {idx + 1} records...')
                    except Exception:
                        self._safe_log(f'<-- DB_RECORD_UPLOAD[{idx}]: {plain.hex(" ")}')
                        break

                self.root.after(0, self._read_done, records, None)
            except Exception as e:
                self.root.after(0, self._read_done, records, e)

        threading.Thread(target=worker, daemon=True).start()

    def _read_add_row(self, idx, rec):
        cat = CATEGORIES.get(rec.category_id, '?')
        v = rec.values
        self._read_tree.insert('', 'end', values=(
            idx, rec.name.strip(), cat,
            f'{v[0]:.4g}', f'{v[1]:.4g}', f'{v[2]:.4g}', f'{v[3]:.4g}', f'{v[4]:.4g}',
            f'{v[5]:.4g}', f'{v[6]:.4g}', f'{v[7]:.4g}', f'{v[8]:.4g}'))

    def _read_done(self, records, err):
        self._read_btn.config(state='normal' if self.connected else 'disabled')
        if err:
            self._read_status_var.set(f'Error: {err}')
            self._log_msg(f'Read failed: {err}')
        else:
            self._read_status_var.set(f'Done — {len(records)} records read')
            if records:
                self._read_db = Database(description='Read from device',
                                         timestamp=datetime.now().strftime('%m/%d/%Y %I:%M:%S %p'),
                                         records=records)
                # Store deep copy as backup for restore
                self._read_db_backup = Database(
                    description=self._read_db.description,
                    timestamp=self._read_db.timestamp,
                    records=[rec.clone() for rec in records])
                self._read_dirty = False
                self._read_restore_btn.pack_forget()
                self._read_save_btn.config(state='normal')

    def _read_save(self):
        if not self._read_db:
            return
        # Update DB description from the name field
        name = self._read_dbname_var.get().strip()
        if name:
            self._read_db.description = name
        path = filedialog.asksaveasfilename(
            title='Save database', defaultextension='.dat',
            filetypes=[('DAT files', '*.dat'), ('All files', '*.*')])
        if path:
            try:
                self._read_db.save(path)
                self._log_msg(f'Saved {len(self._read_db.records)} records to {path}')
            except Exception as e:
                messagebox.showerror('Save Error', str(e))

    def _read_load_file(self):
        """Load a .dat file into the Database Read table. Shows restore button if a
        device backup exists so the user can revert to the original device data."""
        path = filedialog.askopenfilename(
            title='Load database', defaultextension='.dat',
            filetypes=[('DAT files', '*.dat'), ('All files', '*.*')])
        if not path:
            return
        try:
            db = Database.load(path)
        except Exception as e:
            messagebox.showerror('Load Error', str(e))
            return
        self._read_db = db
        self._read_dbname_var.set(db.description.strip())
        self._read_tree.delete(*self._read_tree.get_children())
        for i, rec in enumerate(db.records):
            self._read_add_row(i, rec)
        self._read_save_btn.config(state='normal')
        self._read_status_var.set(f'Loaded {len(db.records)} records from file')
        self._log_msg(f'Loaded {len(db.records)} records from {path}')
        # Show restore button (backup must exist from a prior device read)
        if self._read_db_backup:
            self._read_dirty = True
            self._read_restore_btn.pack(side='left', padx=12)

    def _read_flash(self):
        """Flash the current _read_db to the device using the upload protocol
        (0x0a BEGIN, 0x0b RECORD x N, 0x0c END). Stops live polling during flash
        and waits for any in-flight poll to complete before starting."""
        if not self.connected or not self._read_db:
            return
        total = len(self._read_db.records)
        if total > 49:
            messagebox.showerror('Too Many Records',
                                 f'Database has {total} records but the device limit is 49.\n\n'
                                 f'Remove {total - 49} record(s) in the Database Read tab before flashing.')
            return
        if not messagebox.askyesno('Save to Device',
                                   f'Flash {total} records to device?'):
            return
        db = self._read_db
        # Update description from name field
        name = self._read_dbname_var.get().strip()
        if name:
            db.description = name
        total = len(db.records)
        self._read_flash_btn.config(state='disabled')
        self._read_status_var.set('Flashing...')
        # Pause live polling during flash
        self._live_poll_stop()

        def worker():
            import time
            # Wait for any in-flight poll to finish
            while self._poll_busy.is_set():
                time.sleep(0.05)
            time.sleep(0.2)  # let device settle after polling stops
            try:
                t = self.transport
                lock = self._lock

                with lock:
                    resp = t.send_recv(build_firmware_request())
                check_firmware_ack(resp)
                with lock:
                    resp = t.send_recv(build_status_request())
                # Just verify cmd echo; status byte varies with device state
                plain = decrypt_ack(resp)
                if plain[0] != 0x05:
                    raise RuntimeError(f'STATUS: unexpected cmd 0x{plain[0]:02x}')

                with lock:
                    resp = t.send_recv(build_begin_packet(db.description))
                check_ack(resp, 0x0a, 'BEGIN')

                for i, rec in enumerate(db.records):
                    with lock:
                        resp = t.send_recv(build_record_packet(i, rec))
                    check_ack(resp, 0x0b, f'RECORD {i}')
                    self.root.after(0, lambda cur=i+1: self._read_status_var.set(
                        f'Flashing record {cur}/{total}...'))

                with lock:
                    resp = t.send_recv(build_end_packet())
                check_ack(resp, 0x0c, 'END')
                self.root.after(0, self._read_flash_done, None)
            except Exception as e:
                self.root.after(0, self._read_flash_done, e)

        threading.Thread(target=worker, daemon=True).start()

    def _read_flash_done(self, err):
        """Flash completion callback — re-enable button, restart polling, show result."""
        self._read_flash_btn.config(state='normal' if self.connected else 'disabled')
        self._live_poll_start()
        if err:
            self._read_status_var.set(f'Flash error: {err}')
            messagebox.showerror('Flash Error', str(err))
        else:
            self._read_status_var.set(f'Flashed {len(self._read_db.records)} records to device')
            self._log_msg(f'Flashed {len(self._read_db.records)} records to device')
            if self._read_db_backup:
                self._read_dirty = True
                self._read_restore_btn.pack(side='left', padx=12)

    def _read_on_dblclick(self, event):
        """Open an inline Entry over the double-clicked cell."""
        tree = self._read_tree
        item = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if not item or not col_id:
            return
        col_idx = int(col_id.replace('#', '')) - 1
        col_name = self._read_cols[col_idx]
        if col_name == '#':  # row index not editable
            return

        # Get cell bbox and current value
        tree.update_idletasks()
        bbox = tree.bbox(item, col_id)
        if not bbox:
            return
        x, y, w, h = bbox
        current = tree.set(item, col_id)

        # Destroy previous edit widget if any
        self._read_edit_dismiss(save=False)

        entry = tk.Entry(tree, font=('Consolas', 9))
        entry.place(x=x, y=y, width=w, height=h)
        entry.insert(0, current)
        entry.select_range(0, 'end')
        entry.focus_set()

        self._read_edit_entry = entry
        self._read_edit_item = item
        self._read_edit_col = col_id
        self._read_edit_col_idx = col_idx

        # Category column gets a combobox instead
        if col_name == 'Category':
            entry.destroy()
            cb = ttk.Combobox(tree, values=list(CATEGORIES.values()),
                              state='readonly', font=('Consolas', 9))
            cb.place(x=x, y=y, width=w, height=h)
            cb.set(current)
            cb.focus_set()
            cb.bind('<<ComboboxSelected>>', lambda e: self._read_edit_dismiss(save=True))
            cb.bind('<Escape>', lambda e: self._read_edit_dismiss(save=False))
            self._read_edit_entry = cb
            return

        entry.bind('<Return>', lambda e: self._read_edit_dismiss(save=True))
        entry.bind('<Escape>', lambda e: self._read_edit_dismiss(save=False))
        entry.bind('<FocusOut>', lambda e: self._read_edit_dismiss(save=True))

    def _read_edit_dismiss(self, save=True):
        """Close the inline editor and optionally save the value back to the
        treeview and the underlying Record object in _read_db."""
        entry = self._read_edit_entry
        if entry is None:
            return
        new_val = entry.get()
        entry.destroy()
        self._read_edit_entry = None

        if not save:
            return

        item = self._read_edit_item
        col_id = self._read_edit_col
        col_idx = self._read_edit_col_idx
        col_name = self._read_cols[col_idx]
        tree = self._read_tree

        # Update treeview display
        tree.set(item, col_id, new_val)

        # Update the underlying Record in _read_db
        if self._read_db:
            row_idx = tree.index(item)
            if row_idx < len(self._read_db.records):
                rec = self._read_db.records[row_idx]
                if col_name == 'Name':
                    rec.name = new_val
                elif col_name == 'Category':
                    rev = {v: k for k, v in CATEGORIES.items()}
                    rec.category_id = rev.get(new_val, rec.category_id)
                else:
                    # Numeric value columns: ResGrL=0, ResYelL=1, ..., WtMul=8
                    val_col_map = {
                        'ResGrL': 0, 'ResYelL': 1, 'ResGrR': 2, 'ResYelR': 3,
                        'Field5': 4, 'SG': 5, 'Dim+': 6, 'Dim-': 7, 'WtMul': 8
                    }
                    vi = val_col_map.get(col_name)
                    if vi is not None:
                        try:
                            rec.values[vi] = float(new_val)
                        except ValueError:
                            pass  # revert silently

        # Show Restore button on first edit
        if not self._read_dirty and self._read_db_backup:
            self._read_dirty = True
            self._read_restore_btn.pack(side='left', padx=12)

    def _read_restore(self):
        """Restore the DB table to the original values read from device."""
        if not self._read_db_backup:
            return
        # Deep copy backup back into working DB
        self._read_db = Database(
            description=self._read_db_backup.description,
            timestamp=self._read_db_backup.timestamp,
            records=[rec.clone() for rec in self._read_db_backup.records])
        # Refresh treeview
        self._read_tree.delete(*self._read_tree.get_children())
        for i, rec in enumerate(self._read_db.records):
            self._read_add_row(i, rec)
        self._read_dirty = False
        self._read_restore_btn.pack_forget()
        self._read_status_var.set(f'Restored — {len(self._read_db.records)} records')

    # ══════════════════════════════════════════════════════════════════════
    # TAB 3: Learn Metal — live measurement → new DB entry
    # ══════════════════════════════════════════════════════════════════════

    def _build_learn_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Learn Metal')

        # ── Top: metal name, category, duration ──────────────────────────
        # ── Main horizontal layout: left=monitoring, right=learn panel ────
        hpane = ttk.Frame(tab)
        hpane.pack(fill='both', expand=True)

        left = ttk.Frame(hpane)
        left.pack(side='left', fill='both', expand=True)

        # ── Device info panel ────────────────────────────────────────────
        info_fr = ttk.LabelFrame(left, text='Device Info', padding=4)
        info_fr.pack(fill='x', pady=(0, 0))
        self._dev_info_var = tk.StringVar(value='Not connected')
        ttk.Label(info_fr, textvariable=self._dev_info_var,
                  font=('Consolas', 9)).pack(anchor='w')

        # ── Live readings panel ──────────────────────────────────────────
        live_fr = ttk.LabelFrame(left, text='Live Readings', padding=4)
        live_fr.pack(fill='x', pady=(6, 0))

        self._learn_iacs_var = tk.StringVar(value='-- %IACS')
        self._learn_res_var = tk.StringVar(value='-- µΩ·cm')
        self._learn_thick_var = tk.StringVar(value='-- mm')
        self._learn_temp_var = tk.StringVar(value='-- °C')
        self._learn_match_var = tk.StringVar(value='--')

        for col, (label, var) in enumerate([
            ('Conductivity', self._learn_iacs_var),
            ('Resistivity', self._learn_res_var),
            ('Thickness', self._learn_thick_var),
            ('Temperature', self._learn_temp_var),
        ]):
            ttk.Label(live_fr, text=label, foreground='#888888').grid(
                row=0, column=col, padx=(0, 20), sticky='w')
            ttk.Label(live_fr, textvariable=var, font=('Consolas', 12, 'bold')).grid(
                row=1, column=col, padx=(0, 20), sticky='w')

        ttk.Label(live_fr, text='Detected Metal', foreground='#888888').grid(
            row=0, column=4, padx=(0, 0), sticky='w')
        self._learn_match_lbl = tk.Label(live_fr, textvariable=self._learn_match_var,
                                         font=('Consolas', 12, 'bold'),
                                         fg='#888888', bg=live_fr.winfo_toplevel().cget('bg'))
        self._learn_match_lbl.grid(row=1, column=4, padx=(0, 0), sticky='w')

        # ── Bar meter canvas ─────────────────────────────────────────────
        self._learn_canvas = tk.Canvas(left, height=160, bg='#1a1a2e',
                                       highlightthickness=0)
        self._learn_canvas.pack(fill='x', pady=(6, 4))
        self._learn_canvas.bind('<Configure>', self._learn_redraw)

        # ── Threshold info below the bar ─────────────────────────────────
        thresh_fr = ttk.Frame(left)
        thresh_fr.pack(fill='x', pady=(0, 4))
        self._learn_thresh_lbl = ttk.Label(thresh_fr, text='', font=('Consolas', 9))
        self._learn_thresh_lbl.pack(side='left')

        # ── "Learn New Metal" toggle button ──────────────────────────────
        self._learn_toggle_btn = ttk.Button(left, text='Learn New Metal >>',
                                            command=self._learn_toggle_panel)
        self._learn_toggle_btn.pack(anchor='e', pady=(4, 0))

        # ── Right panel (hidden by default) ──────────────────────────────
        self._learn_panel = ttk.LabelFrame(hpane, text='Learn New Metal', padding=8)
        self._learn_panel_visible = False

        # -- Name & category
        ttk.Label(self._learn_panel, text='Metal Name:').grid(
            row=0, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_name_var = tk.StringVar()
        ttk.Entry(self._learn_panel, textvariable=self._learn_name_var, width=20).grid(
            row=0, column=1, sticky='ew', pady=(0, 4))

        ttk.Label(self._learn_panel, text='Category:').grid(
            row=1, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_cat_var = tk.StringVar(value='Gold')
        ttk.Combobox(self._learn_panel, textvariable=self._learn_cat_var,
                     values=list(CATEGORIES.values()), state='readonly', width=18).grid(
            row=1, column=1, sticky='ew', pady=(0, 4))

        # -- Sampling
        ttk.Label(self._learn_panel, text='Sample time (s):').grid(
            row=2, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_dur_var = tk.StringVar(value='30')
        ttk.Entry(self._learn_panel, textvariable=self._learn_dur_var, width=6).grid(
            row=2, column=1, sticky='w', pady=(0, 4))

        btn_fr = ttk.Frame(self._learn_panel)
        btn_fr.grid(row=3, column=0, columnspan=2, sticky='ew', pady=(0, 4))
        self._learn_start_btn = ttk.Button(btn_fr, text='Start Sampling',
                                           command=self._learn_start)
        self._learn_start_btn.pack(side='left', padx=(0, 4))
        self._cmd_buttons.append(self._learn_start_btn)
        self._learn_stop_btn = ttk.Button(btn_fr, text='Stop Sampling', command=self._learn_stop,
                                          state='disabled')
        self._learn_stop_btn.pack(side='left')

        # -- Status / progress
        self._learn_status_var = tk.StringVar(value='Ready')
        ttk.Label(self._learn_panel, textvariable=self._learn_status_var,
                  wraplength=220).grid(row=4, column=0, columnspan=2, sticky='w', pady=(0, 2))
        self._learn_prog = ttk.Progressbar(self._learn_panel, mode='determinate', length=200)
        self._learn_prog.grid(row=5, column=0, columnspan=2, sticky='ew', pady=(0, 2))

        # -- Hint label (orange bold, hidden until sampling done)
        self._learn_hint_var = tk.StringVar()
        self._learn_hint_lbl = tk.Label(self._learn_panel, textvariable=self._learn_hint_var,
                                        fg='#ff8c00', font=('TkDefaultFont', 9, 'bold'),
                                        anchor='w', wraplength=220)
        self._learn_hint_lbl.grid(row=6, column=0, columnspan=2, sticky='w', pady=(0, 2))

        # -- Warning label (red, hidden until similar metal found)
        self._learn_warn_var = tk.StringVar()
        self._learn_warn_lbl = tk.Label(self._learn_panel, textvariable=self._learn_warn_var,
                                        fg='#ff2222', font=('TkDefaultFont', 9, 'bold'),
                                        anchor='w', wraplength=220)
        self._learn_warn_lbl.grid(row=7, column=0, columnspan=2, sticky='w', pady=(0, 4))

        # -- Separator
        ttk.Separator(self._learn_panel, orient='horizontal').grid(
            row=8, column=0, columnspan=2, sticky='ew', pady=(0, 8))

        # -- Record parameters
        ttk.Label(self._learn_panel, text='Specific Gravity (g/cm³):').grid(
            row=9, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_sg_var = tk.StringVar(value='19.30')
        ttk.Entry(self._learn_panel, textvariable=self._learn_sg_var, width=8).grid(
            row=9, column=1, sticky='w', pady=(0, 4))

        ttk.Label(self._learn_panel, text='Copy zones from:').grid(
            row=10, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_f0_var = tk.StringVar()
        self._learn_f0_cb = ttk.Combobox(self._learn_panel, textvariable=self._learn_f0_var,
                                         width=20, state='readonly')
        self._learn_f0_cb.grid(row=10, column=1, sticky='ew', pady=(0, 4))
        self._learn_f0_cb.bind('<<ComboboxSelected>>', self._learn_f0_changed)

        ttk.Label(self._learn_panel, text='Dim+ Tolerance:').grid(
            row=11, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_dimp_var = tk.StringVar(value='1.0')
        ttk.Entry(self._learn_panel, textvariable=self._learn_dimp_var, width=8).grid(
            row=11, column=1, sticky='w', pady=(0, 4))

        ttk.Label(self._learn_panel, text='Dim- Tolerance:').grid(
            row=12, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_dimm_var = tk.StringVar(value='10.0')
        ttk.Entry(self._learn_panel, textvariable=self._learn_dimm_var, width=8).grid(
            row=12, column=1, sticky='w', pady=(0, 4))

        ttk.Label(self._learn_panel, text='Weight Multiplier:').grid(
            row=13, column=0, sticky='w', padx=(0, 4), pady=(0, 4))
        self._learn_wm_var = tk.StringVar(value='10.0')
        ttk.Entry(self._learn_panel, textvariable=self._learn_wm_var, width=8).grid(
            row=13, column=1, sticky='w', pady=(0, 4))

        self._learn_save_btn = ttk.Button(self._learn_panel, text='Add to Database',
                                          command=self._learn_save, state='disabled')
        self._learn_save_btn.grid(row=14, column=0, columnspan=2, sticky='ew', pady=(8, 0))

        # ── Internal state ───────────────────────────────────────────────
        self._learn_samples = []       # list of %IACS readings
        self._learn_collecting = False
        self._learn_timer_id = None
        # Fixed bar range in %IACS
        self._learn_bar_min = 0.0
        self._learn_bar_max = 100.0
        # Thresholds in %IACS (display units) — set after collection
        self._learn_yellow_left = 0.0
        self._learn_green_left = 0.0
        self._learn_green_right = 0.0
        self._learn_yellow_right = 0.0
        self._learn_obs_min = 0.0
        self._learn_obs_max = 0.0
        self._learn_live_value = None
        self._learn_ready = False      # thresholds have been set
        self._learn_scan_rec = None    # matched record during live scanning
        # Drag state
        self._learn_drag_handle = None
        self._learn_canvas.bind('<ButtonPress-1>', self._learn_on_press)
        self._learn_canvas.bind('<B1-Motion>', self._learn_on_drag)
        self._learn_canvas.bind('<ButtonRelease-1>', self._learn_on_release)
        # Populate field0 dropdown when a db is loaded
        self._learn_populate_f0()

    def _learn_populate_f0(self):
        """Populate the ResGreenLeft copy-from dropdown with existing records."""
        items = []
        db = self.loaded_db
        if db:
            for i, rec in enumerate(db.records):
                cat = CATEGORIES.get(rec.category_id, '?')
                items.append(f'{rec.name.strip()} ({cat}) — {rec.values[0]:.0f}')
        if items:
            self._learn_f0_cb['values'] = items
            self._learn_f0_cb.current(0)
            self._learn_f0_changed()
        else:
            self._learn_f0_cb['values'] = ['(load a .dat file in Flash tab first)']
            self._learn_f0_cb.current(0)

    def _learn_f0_changed(self, _event=None):
        """When the user selects a different record in 'Copy zones from',
        update Specific Gravity, Dim+, Dim-, Weight Multiplier from that record.
        If sampling is done, also recalculate zone thresholds from the new reference."""
        idx = self._learn_f0_cb.current()
        if self.loaded_db and 0 <= idx < len(self.loaded_db.records):
            rec = self.loaded_db.records[idx]
            self._learn_sg_var.set(f'{rec.values[5]:.2f}')
            self._learn_dimp_var.set(f'{rec.values[6]:.1f}')
            self._learn_dimm_var.set(f'{rec.values[7]:.1f}')
            self._learn_wm_var.set(f'{rec.values[8]:.1f}')

            # Recalculate zones if we already have samples
            if self._learn_ready and len(self._learn_samples) >= 3:
                center_iacs = sum(self._learn_samples) / len(self._learn_samples)
                ref_f1 = rec.values[1]
                ref_f2 = rec.values[2]
                ref_f3 = rec.values[3]
                ref_f4 = rec.values[4]
                if ref_f1 > 0 and ref_f2 > 0 and ref_f3 > 0 and ref_f4 > 0 and center_iacs > 0:
                    ref_yl_iacs = 100.0 / ref_f4
                    ref_gl_iacs = 100.0 / ref_f3
                    ref_gr_iacs = 100.0 / ref_f2
                    ref_yr_iacs = 100.0 / ref_f1
                    ref_center = (ref_gl_iacs + ref_gr_iacs) / 2.0
                    if ref_center > 0:
                        green_down_pct = (ref_center - ref_gl_iacs) / ref_center
                        green_up_pct = (ref_gr_iacs - ref_center) / ref_center
                        yellow_down_pct = (ref_center - ref_yl_iacs) / ref_center
                        yellow_up_pct = (ref_yr_iacs - ref_center) / ref_center
                        self._learn_green_left = center_iacs * (1.0 - green_down_pct)
                        self._learn_green_right = center_iacs * (1.0 + green_up_pct)
                        self._learn_yellow_left = center_iacs * (1.0 - yellow_down_pct)
                        self._learn_yellow_right = center_iacs * (1.0 + yellow_up_pct)
                        self._learn_redraw()
                        self._learn_update_thresh_label()

    # ── Learn panel toggle ────────────────────────────────────────────

    def _learn_toggle_panel(self):
        if self._learn_panel_visible:
            if self._learn_collecting:
                self._learn_stop()
            self._learn_panel.pack_forget()
            self._learn_toggle_btn.config(text='Learn New Metal >>')
            self._learn_panel_visible = False
            # Clear learn state so bar returns to live scan mode
            self._learn_ready = False
            self._learn_samples = []
            self._learn_redraw()
        else:
            self._learn_panel.pack(side='right', fill='y', padx=(8, 0))
            self._learn_toggle_btn.config(text='<< Hide')
            self._learn_panel_visible = True
            # Expand window to fit the panel, preserving current position
            self.root.update_idletasks()
            req_w = self.root.winfo_reqwidth()
            cur_w = self.root.winfo_width()
            cur_h = self.root.winfo_height()
            if req_w > cur_w:
                x = self.root.winfo_x()
                y = self.root.winfo_y()
                self.root.geometry(f'{req_w}x{cur_h}+{x}+{y}')

    # ── Sampling ─────────────────────────────────────────────────────

    def _learn_start(self):
        if not self.connected:
            self._learn_status_var.set('Not connected — connect to the device first.')
            return
        try:
            duration = int(self._learn_dur_var.get())
        except ValueError:
            duration = 30
        self._learn_samples = []
        self._learn_collecting = True
        self._learn_waiting_for_reading = True  # wait for first valid reading before timing
        self._learn_ready = False
        self._learn_live_value = None
        self._learn_hint_var.set('')
        self._learn_warn_var.set('')
        self._learn_start_btn.config(state='disabled')
        self._learn_stop_btn.config(state='normal')
        self._learn_save_btn.config(state='disabled')
        self._learn_prog['maximum'] = duration
        self._learn_prog['value'] = 0
        self._learn_status_var.set('Waiting for reading... place the metal on the sensor.')
        self._learn_sample_duration = duration
        self._learn_poll()

    def _learn_stop(self):
        self._learn_collecting = False
        if self._learn_timer_id:
            self.root.after_cancel(self._learn_timer_id)
            self._learn_timer_id = None
        self._learn_start_btn.config(state='normal' if self.connected else 'disabled')
        self._learn_stop_btn.config(state='disabled')
        if len(self._learn_samples) >= 3:
            self._learn_finish()
        else:
            self._learn_status_var.set(
                f'Stopped — only {len(self._learn_samples)} samples (need at least 3).')

    def _learn_poll(self):
        if not self._learn_collecting:
            return

        # Don't start timing until warmup is done
        if self._learn_waiting_for_reading or getattr(self, '_learn_warmup', False):
            elapsed = 0
        else:
            elapsed = (datetime.now() - self._learn_sample_start).total_seconds()
            if elapsed >= self._learn_sample_duration:
                self._learn_collecting = False
                self._learn_stop_btn.config(state='disabled')
                self._learn_start_btn.config(state='normal' if self.connected else 'disabled')
                self._learn_finish()
                return
            self._learn_prog['value'] = elapsed

        def worker():
            try:
                readings = {}
                # THICKNESS_DATA (0x14) — 9 float32 values at offset 1
                pkt = build_generic_packet(0x14)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                fields = [struct.unpack_from('<f', plain, 1 + i * 4)[0] for i in range(9)]
                resistivity = fields[5]
                thickness = fields[3]
                if resistivity > 0.01:
                    readings['iacs'] = 100.0 / resistivity
                    readings['resistivity'] = resistivity
                if thickness > 0.01:
                    readings['thickness'] = thickness

                # SYSTEM_STATUS (0x1e) — wand resistivity at byte offset 10
                pkt = build_generic_packet(0x1e)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                if plain[0] not in (0x02, 0x03) and len(plain) >= 14:
                    wand_res = struct.unpack_from('<f', plain, 10)[0]
                    if 0.1 < wand_res < 500:
                        readings['iacs'] = 100.0 / wand_res
                        readings['resistivity'] = wand_res
                        readings['source'] = 'wand'

                # TEMPERATURE (0x09)
                pkt = build_generic_packet(0x09)
                with self._lock:
                    resp = self.transport.send_recv(pkt)
                plain = decrypt_ack(resp)
                temp = struct.unpack_from('<f', plain, 1)[0]
                if 0 < temp < 100:
                    readings['temperature'] = temp

                self.root.after(0, self._learn_got_sample, readings, elapsed)
            except Exception:
                self.root.after(0, self._learn_got_sample, {}, elapsed)

        threading.Thread(target=worker, daemon=True).start()
        self._learn_timer_id = self.root.after(500, self._learn_poll)

    def _learn_got_sample(self, readings, elapsed):
        """Process a sample reading from the learn poll worker. On first valid
        reading, starts a warmup period to let the sensor stabilize before
        recording samples. After warmup, appends %IACS values to the sample list."""
        WARMUP_SECONDS = 2  # warmup to skip initial sensor noise

        # Start the warmup timer on first valid reading
        if self._learn_waiting_for_reading and 'iacs' in readings:
            self._learn_waiting_for_reading = False
            self._learn_warmup = True
            self._learn_sample_start = datetime.now()
            self._learn_status_var.set('Warming up sensor...')

        # Check if warmup period is over
        if getattr(self, '_learn_warmup', False):
            warmup_elapsed = (datetime.now() - self._learn_sample_start).total_seconds()
            if warmup_elapsed >= WARMUP_SECONDS:
                self._learn_warmup = False
                # Reset the timer start so the actual sampling duration begins now
                self._learn_sample_start = datetime.now()
                self._learn_status_var.set('Sampling... keep the metal on the sensor.')

        # Update live readout labels
        src = ' (wand)' if readings.get('source') == 'wand' else ''
        if 'iacs' in readings:
            self._learn_iacs_var.set(f'{readings["iacs"]:.2f} %IACS{src}')
        if 'resistivity' in readings:
            self._learn_res_var.set(f'{readings["resistivity"]:.4f} µΩ·cm{src}')
        if 'thickness' in readings:
            self._learn_thick_var.set(f'{readings["thickness"]:.3f} mm')
        if 'temperature' in readings:
            self._learn_temp_var.set(f'{readings["temperature"]:.1f} °C')

        iacs = readings.get('iacs')
        if iacs is not None and iacs > 0.1:
            self._learn_live_value = iacs
            # Only record samples after warmup
            if not getattr(self, '_learn_warmup', False) and not self._learn_waiting_for_reading:
                self._learn_samples.append(iacs)
                n = len(self._learn_samples)
                mn = min(self._learn_samples)
                mx = max(self._learn_samples)
                self._learn_status_var.set(
                    f'Sampling... {n} readings  |  '
                    f'Live: {iacs:.2f} %IACS  |  '
                    f'Range: {mn:.2f} – {mx:.2f}')
            self._learn_redraw()

    def _learn_finish(self):
        """Calculate recommended thresholds from collected samples.
        Applies IQR outlier removal, then uses the mean of filtered samples as
        center. If a reference record is selected, copies its zone spread
        percentages (green/yellow widths as % of center) and applies them to
        the new metal's center. Otherwise uses a default margin. If the sampled
        metal matches an existing DB entry, warns the user and pre-fills
        SG/Dim+/Dim-/WM from the best match."""
        if len(self._learn_samples) < 3:
            return
        raw = sorted(self._learn_samples)

        # IQR outlier removal
        n = len(raw)
        q1 = raw[n // 4]
        q3 = raw[3 * n // 4]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        samples = [s for s in raw if lower <= s <= upper]
        if len(samples) < 3:
            samples = raw  # fallback if too aggressive

        mn = min(samples)
        mx = max(samples)
        center_iacs = sum(samples) / len(samples)  # mean of filtered samples

        self._learn_obs_min = mn
        self._learn_obs_max = mx
        removed = len(raw) - len(samples)

        # Try to copy zone percentages from the selected reference record
        ref_rec = None
        f0_idx = self._learn_f0_cb.current()
        if self.loaded_db and 0 <= f0_idx < len(self.loaded_db.records):
            ref_rec = self.loaded_db.records[f0_idx]

        if ref_rec and center_iacs > 0:
            # Reference record thresholds are in resistivity (µΩ·cm)
            # Convert to %IACS for percentage calculation
            ref_f1 = ref_rec.values[1]  # ResYellowLeft (lowest res = highest IACS)
            ref_f2 = ref_rec.values[2]  # ResGreenLeft
            ref_f3 = ref_rec.values[3]  # ResGreenRight
            ref_f4 = ref_rec.values[4]  # ResYellowRight (highest res = lowest IACS)
            if ref_f1 > 0 and ref_f2 > 0 and ref_f3 > 0 and ref_f4 > 0:
                # Convert to %IACS
                ref_yl_iacs = 100.0 / ref_f4   # yellow left (lowest IACS)
                ref_gl_iacs = 100.0 / ref_f3   # green left
                ref_gr_iacs = 100.0 / ref_f2   # green right
                ref_yr_iacs = 100.0 / ref_f1   # yellow right (highest IACS)
                ref_center = (ref_gl_iacs + ref_gr_iacs) / 2.0
                if ref_center > 0:
                    # Calculate zone widths as percentage of center
                    green_down_pct = (ref_center - ref_gl_iacs) / ref_center
                    green_up_pct = (ref_gr_iacs - ref_center) / ref_center
                    yellow_down_pct = (ref_center - ref_yl_iacs) / ref_center
                    yellow_up_pct = (ref_yr_iacs - ref_center) / ref_center
                    # Apply percentages to new metal's center
                    self._learn_green_left = center_iacs * (1.0 - green_down_pct)
                    self._learn_green_right = center_iacs * (1.0 + green_up_pct)
                    self._learn_yellow_left = center_iacs * (1.0 - yellow_down_pct)
                    self._learn_yellow_right = center_iacs * (1.0 + yellow_up_pct)
                else:
                    ref_rec = None  # fall through to default

        if ref_rec is None:
            # Default: use observed spread + margin, centered on mean
            spread = mx - mn
            margin = max(spread * 0.5, center_iacs * 0.03)
            self._learn_green_left = center_iacs - (spread / 2 + margin * 0.3)
            self._learn_green_right = center_iacs + (spread / 2 + margin * 0.3)
            self._learn_yellow_left = center_iacs - (spread / 2 + margin)
            self._learn_yellow_right = center_iacs + (spread / 2 + margin)

        self._learn_ready = True
        self._learn_save_btn.config(state='normal')

        # Check if the sampled metal matches an existing DB entry
        center_res = 100.0 / ((mn + mx) / 2) if (mn + mx) > 0 else 0
        if center_res > 0:
            matches = self._match_metal(center_res)
            if matches:
                names = [m[0] for m in matches[:3]]
                self._learn_warn_var.set(f'Similar to: {", ".join(names)}')
                # Pre-fill SG, Dim+, Dim-, WM from the best match
                best_rec = matches[0][2]
                self._learn_sg_var.set(f'{best_rec.values[5]:.2f}')
                self._learn_dimp_var.set(f'{best_rec.values[6]:.1f}')
                self._learn_dimm_var.set(f'{best_rec.values[7]:.1f}')
                self._learn_wm_var.set(f'{best_rec.values[8]:.1f}')
            else:
                self._learn_warn_var.set('')
        else:
            self._learn_warn_var.set('')

        outlier_note = f'  ({removed} outliers removed)' if removed else ''
        self._learn_status_var.set(
            f'Done! {len(samples)} readings{outlier_note}  |  '
            f'Avg: {center_iacs:.2f}  Range: {mn:.2f} – {mx:.2f} %IACS')
        self._learn_hint_var.set('Drag handles to adjust zones.')
        self._learn_redraw()
        self._learn_update_thresh_label()

    def _learn_update_thresh_label(self):
        if not self._learn_ready:
            self._learn_thresh_lbl.config(text='')
            return
        yl = self._learn_yellow_left
        gl = self._learn_green_left
        gr = self._learn_green_right
        yr = self._learn_yellow_right
        # Convert to resistivity for preview
        r_yl = 100.0 / yl if yl > 0 else 0
        r_gl = 100.0 / gl if gl > 0 else 0
        r_gr = 100.0 / gr if gr > 0 else 0
        r_yr = 100.0 / yr if yr > 0 else 0
        self._learn_thresh_lbl.config(
            text=f'YellowLeft={yl:.2f}  GreenLeft={gl:.2f}  '
                 f'GreenRight={gr:.2f}  YellowRight={yr:.2f}  (%IACS)    '
                 f'DB: [{r_yr:.4f}  {r_gr:.4f}  {r_gl:.4f}  {r_yl:.4f}] (µΩ·cm)')

    # ── Bar meter drawing ────────────────────────────────────────────

    def _learn_bar_geometry(self):
        """Return drawing constants based on current canvas size."""
        cw = self._learn_canvas.winfo_width()
        ch = self._learn_canvas.winfo_height()
        pad = 40
        bar_x0 = pad
        bar_x1 = cw - pad
        bar_y0 = 50
        bar_h = 60
        bar_y1 = bar_y0 + bar_h
        return cw, ch, bar_x0, bar_x1, bar_y0, bar_y1, bar_h

    def _learn_val_to_x(self, val, bar_x0, bar_x1, vmin, vmax):
        """Convert a %IACS value to canvas x coordinate. Bar is reversed to match
        the device screen: high %IACS (low resistivity) on the left."""
        if vmax <= vmin:
            return (bar_x0 + bar_x1) / 2
        frac = 1.0 - (val - vmin) / (vmax - vmin)  # reversed: high values on left
        return bar_x0 + frac * (bar_x1 - bar_x0)

    def _learn_x_to_val(self, x, bar_x0, bar_x1, vmin, vmax):
        """Convert canvas x coordinate back to a %IACS value (reversed bar)."""
        if bar_x1 <= bar_x0:
            return (vmin + vmax) / 2
        frac = 1.0 - (x - bar_x0) / (bar_x1 - bar_x0)  # reversed
        return vmin + frac * (vmax - vmin)

    def _learn_redraw(self, _event=None):
        """Redraw the bar meter canvas. Three modes:
        1. _learn_ready: post-sampling with draggable threshold handles
        2. scan_rec is not None: live scan showing matched metal's zones (read-only)
        3. else: collection mode with sample dots on a neutral bar"""
        c = self._learn_canvas
        c.delete('all')
        cw, ch, bx0, bx1, by0, by1, bh = self._learn_bar_geometry()

        # Fixed scale
        vmin = self._learn_bar_min
        vmax = self._learn_bar_max

        scan_rec = getattr(self, '_learn_scan_rec', None)

        samples = self._learn_samples

        def vx(val):
            return self._learn_val_to_x(val, bx0, bx1, vmin, vmax)

        def _draw_zones(yl, gl, gr, yr, draggable=False):
            """Draw the 5-zone bar (red/yellow/green/yellow/red) using x-sorted positions."""
            xs = sorted([
                (vx(yl), yl, 'YL', '#b8860b'),
                (vx(gl), gl, 'GL', '#00cc00'),
                (vx(gr), gr, 'GR', '#00cc00'),
                (vx(yr), yr, 'YR', '#b8860b'),
            ])
            colors = ['#8b0000', '#b8860b', '#006400', '#b8860b', '#8b0000']
            edges = [bx0] + [x for x, *_ in xs] + [bx1]
            for i, col in enumerate(colors):
                c.create_rectangle(edges[i], by0, edges[i + 1], by1, fill=col, outline='')
            c.create_rectangle(bx0, by0, bx1, by1, outline='#444444', width=1)

            handle_y = by1 + 5
            handle_h = 18
            for _hx, val, label, color in xs:
                hx = _hx
                tags = f'handle_{label}' if draggable else ''
                c.create_polygon(hx, handle_y, hx - 8, handle_y + handle_h,
                                 hx + 8, handle_y + handle_h,
                                 fill=color, outline='white', tags=tags)
                tag2 = f'label_{label}' if draggable else ''
                c.create_text(hx, handle_y + handle_h + 10, text=f'{val:.2f}',
                              fill=color, font=('Consolas', 8), tags=tag2)

            for frac in [0, 0.25, 0.5, 0.75, 1.0]:
                sv = vmin + frac * (vmax - vmin)
                sx = vx(sv)
                c.create_text(sx, by0 - 8, text=f'{sv:.1f}', fill='#888888',
                              font=('Consolas', 8))

        # Draw bar based on mode
        if self._learn_ready:
            yl = self._learn_yellow_left
            gl = self._learn_green_left
            gr = self._learn_green_right
            yr = self._learn_yellow_right

            _draw_zones(yl, gl, gr, yr, draggable=True)

            # Observed sample range band
            omn = min(samples)
            omx = max(samples)
            c.create_rectangle(vx(omn), by0 + 2, vx(omx), by1 - 2,
                               fill='', outline='#ffffff', width=2, dash=(4, 2))
        elif scan_rec is not None:
            # Live scan mode — show matched metal zones (read-only, no drag)
            yl = 100.0 / scan_rec.values[4]
            gl = 100.0 / scan_rec.values[3]
            gr = 100.0 / scan_rec.values[2]
            yr = 100.0 / scan_rec.values[1]

            _draw_zones(yl, gl, gr, yr, draggable=False)
        else:
            # Neutral bar (idle or during collection)
            c.create_rectangle(bx0, by0, bx1, by1, fill='#2a2a3e', outline='#444444')
            if samples:
                omn = min(samples)
                omx = max(samples)
                c.create_rectangle(vx(omn), by0 + 2, vx(omx), by1 - 2,
                                   fill='', outline='#00aaff', width=2, dash=(4, 2))
            for frac in [0, 0.25, 0.5, 0.75, 1.0]:
                sv = vmin + frac * (vmax - vmin)
                sx = vx(sv)
                c.create_text(sx, by0 - 8, text=f'{sv:.1f}', fill='#888888',
                              font=('Consolas', 8))

        # Live needle — prominent triangle + line indicator
        if self._learn_live_value is not None:
            nx = vx(self._learn_live_value)
            # Thick vertical line through the bar
            c.create_line(nx, by0 - 2, nx, by1 + 2, fill='#ffffff', width=3)
            # Downward-pointing triangle above the bar
            c.create_polygon(nx, by0 - 4, nx - 7, by0 - 16, nx + 7, by0 - 16,
                             fill='#ff4444', outline='white', width=1)
            # Value label above the triangle
            c.create_text(nx, by0 - 26, text=f'{self._learn_live_value:.2f}',
                          fill='#ff4444', font=('Consolas', 11, 'bold'))

        # Title
        c.create_text(cw / 2, 15, text='%IACS  (100 / resistivity)',
                      fill='#aaaaaa', font=('TkDefaultFont', 10))

    # ── Drag handling ────────────────────────────────────────────────

    def _learn_find_handle(self, x, y):
        """Return handle name if click is near a handle, else None."""
        if not self._learn_ready:
            return None
        cw, ch, bx0, bx1, by0, by1, bh = self._learn_bar_geometry()
        vmin, vmax = self._learn_display_range()
        handles = {
            'YL': self._learn_yellow_left,
            'GL': self._learn_green_left,
            'GR': self._learn_green_right,
            'YR': self._learn_yellow_right,
        }
        for name, val in handles.items():
            hx = self._learn_val_to_x(val, bx0, bx1, vmin, vmax)
            if abs(x - hx) < 12 and by1 < y < by1 + 40:
                return name
        return None

    def _learn_display_range(self):
        return self._learn_bar_min, self._learn_bar_max

    def _learn_on_press(self, event):
        self._learn_drag_handle = self._learn_find_handle(event.x, event.y)

    def _learn_on_drag(self, event):
        """Drag a threshold handle on the bar. Enforces ordering YL < GL < GR < YR
        so zone boundaries never cross each other."""
        h = self._learn_drag_handle
        if not h:
            return
        cw, ch, bx0, bx1, by0, by1, bh = self._learn_bar_geometry()
        vmin, vmax = self._learn_display_range()
        val = self._learn_x_to_val(event.x, bx0, bx1, vmin, vmax)

        # Enforce ordering: YL < GL < GR < YR
        if h == 'YL':
            self._learn_yellow_left = min(val, self._learn_green_left - 0.01)
        elif h == 'GL':
            self._learn_green_left = max(min(val, self._learn_green_right - 0.01),
                                         self._learn_yellow_left + 0.01)
        elif h == 'GR':
            self._learn_green_right = max(min(val, self._learn_yellow_right - 0.01),
                                          self._learn_green_left + 0.01)
        elif h == 'YR':
            self._learn_yellow_right = max(val, self._learn_green_right + 0.01)

        self._learn_redraw()
        self._learn_update_thresh_label()

    def _learn_on_release(self, event):
        self._learn_drag_handle = None

    # ── Save ─────────────────────────────────────────────────────────

    def _learn_save(self):
        """Create a new DB record from the learned thresholds and add it to the database.
        Converts %IACS thresholds back to resistivity (µΩ·cm) for storage, since DB
        fields store resistivity. Higher %IACS = lower resistivity, so ordering inverts."""
        name = self._learn_name_var.get().strip()
        if not name:
            messagebox.showerror('Missing Name', 'Enter a metal name.')
            return

        cat_name = self._learn_cat_var.get()
        cat_id = {v: k for k, v in CATEGORIES.items()}.get(cat_name, 0)

        try:
            sg = float(self._learn_sg_var.get())
        except ValueError:
            messagebox.showerror('Invalid', 'Specific gravity must be a number.')
            return
        try:
            dimp = float(self._learn_dimp_var.get())
            dimm = float(self._learn_dimm_var.get())
            wm = float(self._learn_wm_var.get())
        except ValueError:
            messagebox.showerror('Invalid', 'Tolerances must be numbers.')
            return

        # Get ResGreenLeft (field 0) from the selected existing record
        f0_idx = self._learn_f0_cb.current()
        if self.loaded_db and 0 <= f0_idx < len(self.loaded_db.records):
            field0 = self.loaded_db.records[f0_idx].values[0]
        else:
            messagebox.showerror('Missing',
                                 'Load a .dat file in the Flash tab first so we can '
                                 'copy ResGreenLeft from an existing record.')
            return

        # Convert %IACS thresholds back to resistivity (µΩ·cm)
        # Higher %IACS = lower resistivity, so the ordering inverts:
        #   DB field 1 (ResYellowLeft)  = 100 / yellow_right_iacs  (highest IACS → lowest res)
        #   DB field 2 (ResGreenLeft)   = 100 / green_right_iacs
        #   DB field 3 (ResGreenRight)  = 100 / green_left_iacs
        #   DB field 4 (ResYellowRight) = 100 / yellow_left_iacs   (lowest IACS → highest res)
        r_f1 = 100.0 / self._learn_yellow_right
        r_f2 = 100.0 / self._learn_green_right
        r_f3 = 100.0 / self._learn_green_left
        r_f4 = 100.0 / self._learn_yellow_left

        values = [field0, r_f1, r_f2, r_f3, r_f4, sg, dimp, dimm, wm]
        rec = Record(name, cat_id, values)

        if self.loaded_db:
            if len(self.loaded_db.records) >= 49:
                messagebox.showerror('Database Full',
                                     'The database already has 49 records (device firmware limit).\n\n'
                                     'Remove a record in the Database Read tab before adding a new one.')
                return
            self.loaded_db.records.append(rec)
            self._log_msg(f'Added "{name}" as record #{len(self.loaded_db.records)}')
            # Refresh the flash tab tree
            self._flash_load_db(self.loaded_db)
            # Also refresh the read tab tree if it has data
            if hasattr(self, '_read_tree'):
                self._read_tree.delete(*self._read_tree.get_children())
                for i, r in enumerate(self.loaded_db.records):
                    c = CATEGORIES.get(r.category_id, '?')
                    v = r.values
                    self._read_tree.insert('', 'end', values=(
                        i, r.name.strip(), c,
                        f'{v[0]:.1f}', f'{v[1]:.1f}', f'{v[2]:.1f}', f'{v[3]:.1f}'))
            messagebox.showinfo('Added',
                                f'"{name}" added as record #{len(self.loaded_db.records)}.\n\n'
                                f'Use the Flash tab to save the .dat file and upload to device.')
        else:
            db = Database(description='Custom', timestamp=datetime.now().strftime(
                '%m/%d/%Y %I:%M:%S %p'), records=[rec])
            path = filedialog.asksaveasfilename(
                title='Save new database', defaultextension='.dat',
                filetypes=[('DAT files', '*.dat'), ('All files', '*.*')])
            if path:
                db.save(path)
                self._log_msg(f'Saved new database with "{name}" to {path}')

    def _flash_load_db(self, db):
        """Refresh flash tab treeview from a Database object."""
        self._flash_tree.delete(*self._flash_tree.get_children())
        for i, rec in enumerate(db.records):
            cat = CATEGORIES.get(rec.category_id, '?')
            vals = rec.values
            self._flash_tree.insert('', 'end', values=(
                i, rec.name.strip(), cat,
                f'{vals[0]:.1f}', f'{vals[1]:.1f}', f'{vals[2]:.1f}', f'{vals[3]:.1f}'))

    # ══════════════════════════════════════════════════════════════════════
    # TAB 4: Device Info
    # ══════════════════════════════════════════════════════════════════════

    def _build_info_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Device Info')

        r = 0
        # Firmware version (special parsing)
        self._fw_var = tk.StringVar(value='--')
        btn = ttk.Button(tab, text='Get Firmware Version', width=22,
                         command=self._get_firmware)
        btn.grid(row=r, column=0, sticky='w', padx=4, pady=2)
        self._cmd_buttons.append(btn)
        ttk.Label(tab, textvariable=self._fw_var, anchor='w').grid(
            row=r, column=1, columnspan=3, sticky='w', padx=4)

        r += 1
        # Status (special parsing)
        self._st_var = tk.StringVar(value='--')
        btn = ttk.Button(tab, text='Get Status', width=22, command=self._get_status)
        btn.grid(row=r, column=0, sticky='w', padx=4, pady=2)
        self._cmd_buttons.append(btn)
        ttk.Label(tab, textvariable=self._st_var, anchor='w').grid(
            row=r, column=1, columnspan=3, sticky='w', padx=4)

        r += 1
        self._add_cmd_row(tab, r, 'Get Temperature', 0x09); r += 1
        self._add_cmd_row(tab, r, 'Get System Info', 0x1d); r += 1
        self._add_cmd_row(tab, r, 'Get System Status', 0x1e); r += 1
        self._add_cmd_row(tab, r, 'Get System Settings', 0x1f); r += 1
        self._add_cmd_row(tab, r, 'Diag System Status', 0x28); r += 1

    def _get_firmware(self):
        def cb(plain, err):
            if err:
                self._fw_var.set(f'ERROR: {err}')
            else:
                vb = plain[1:]
                null = vb.find(0)
                ver = vb[:null].decode('ascii', errors='replace') if null > 0 else '(unknown)'
                self._fw_var.set(f'v{ver}  [raw: {plain[:8].hex(" ")}]')
        self._run_command(build_firmware_request(), cb)

    def _get_status(self):
        def cb(plain, err):
            if err:
                self._st_var.set(f'ERROR: {err}')
            else:
                ok = 'OK' if plain[1] == 0x01 else f'status=0x{plain[1]:02x}'
                self._st_var.set(f'{ok}  [raw: {plain[:8].hex(" ")}]')
        self._run_command(build_status_request(), cb)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 4: Calibration
    # ══════════════════════════════════════════════════════════════════════

    def _build_cal_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Calibration')

        r = 0
        self._add_cmd_row(tab, r, '', 0, section_label='Calibration Control'); r += 1
        self._add_cmd_row(tab, r, 'Start Calibration', 0x06, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Get Cal Status', 0x07); r += 1
        self._add_cmd_row(tab, r, 'Get Cal Results', 0x08); r += 1

        self._add_cmd_row(tab, r, '', 0, section_label='Factory / Sensor Calibration'); r += 1
        self._add_cmd_row(tab, r, 'Factory Cal Point', 0x10, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Lift-Off Cal Point', 0x15, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Sensor Dist Cal Point', 0x18, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Sensor Dist Cal Results', 0x19); r += 1
        self._add_cmd_row(tab, r, 'Thickness Cal Point', 0x1b, has_payload=True); r += 1

    # ══════════════════════════════════════════════════════════════════════
    # TAB 5: Configuration
    # ══════════════════════════════════════════════════════════════════════

    def _build_config_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Configuration')

        r = 0
        self._add_cmd_row(tab, r, '', 0, section_label='Sensor Configuration'); r += 1
        self._add_cmd_row(tab, r, 'Get Sensor Config', 0x0e); r += 1
        self._add_cmd_row(tab, r, 'Set Sensor Config', 0x0d, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Store Sensor Config', 0x0f); r += 1

        self._add_cmd_row(tab, r, '', 0, section_label='Meter Configuration'); r += 1
        self._add_cmd_row(tab, r, 'Get Meter Config', 0x12); r += 1
        self._add_cmd_row(tab, r, 'Set Meter Config', 0x11, has_payload=True); r += 1

    # ══════════════════════════════════════════════════════════════════════
    # TAB 6: Wand
    # ══════════════════════════════════════════════════════════════════════

    def _build_wand_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Wand')

        r = 0
        self._add_cmd_row(tab, r, 'Get Wand Config', 0x16); r += 1
        self._add_cmd_row(tab, r, 'Get Wand Status', 0x21); r += 1

        # Erase wand with confirmation
        self._erase_var = tk.StringVar(value='--')
        btn = ttk.Button(tab, text='Erase Wand', width=22, command=self._erase_wand)
        btn.grid(row=r, column=0, sticky='w', padx=4, pady=2)
        self._cmd_buttons.append(btn)
        ttk.Label(tab, textvariable=self._erase_var, anchor='w').grid(
            row=r, column=1, columnspan=3, sticky='w', padx=4)

    def _erase_wand(self):
        if not messagebox.askyesno('Confirm Erase',
                                   'Erase the wand? This may not be reversible.'):
            return

        def cb(plain, err):
            if err:
                self._erase_var.set(f'ERROR: {err}')
            else:
                self._erase_var.set(plain.hex(' '))
        self._run_command(build_generic_packet(0x17), cb)

    # ══════════════════════════════════════════════════════════════════════
    # TAB 7: Diagnostics
    # ══════════════════════════════════════════════════════════════════════

    def _build_diag_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Diagnostics')

        r = 0
        self._add_cmd_row(tab, r, '', 0, section_label='Debug'); r += 1
        self._add_cmd_row(tab, r, 'Get Debug Data', 0x1a); r += 1
        self._add_cmd_row(tab, r, 'Set Debug Data', 0x1c, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Get Debug Legend', 0x26); r += 1

        self._add_cmd_row(tab, r, '', 0, section_label='Analog / Measurement'); r += 1
        self._add_cmd_row(tab, r, 'Get Analog Values', 0x27); r += 1
        self._add_cmd_row(tab, r, 'Get Thickness Data', 0x14); r += 1
        self._add_cmd_row(tab, r, 'Use Target Bulk Res', 0x20, has_payload=True); r += 1

        self._add_cmd_row(tab, r, '', 0, section_label='Device Control'); r += 1
        self._add_cmd_row(tab, r, 'Set Current Metal', 0x24, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Set Weight', 0x25, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Set Dummy Value', 0x13, has_payload=True); r += 1
        self._add_cmd_row(tab, r, 'Set Color', 0x29, has_payload=True); r += 1

    # ══════════════════════════════════════════════════════════════════════
    # TAB 8: Raw Command
    # ══════════════════════════════════════════════════════════════════════

    def _build_raw_tab(self):
        tab = ttk.Frame(self._nb, padding=8)
        self._nb.add(tab, text='Raw Command')

        top = ttk.Frame(tab)
        top.pack(fill='x')

        ttk.Label(top, text='Cmd byte (hex):').pack(side='left')
        self._raw_cmd_var = tk.StringVar(value='04')
        ttk.Entry(top, textvariable=self._raw_cmd_var, width=4).pack(side='left', padx=(4, 12))

        ttk.Label(top, text='Payload (hex):').pack(side='left')
        self._raw_pay_var = tk.StringVar()
        ttk.Entry(top, textvariable=self._raw_pay_var, width=40).pack(side='left', padx=(4, 12))

        send_btn = ttk.Button(top, text='Send', command=self._send_raw)
        send_btn.pack(side='left')
        self._cmd_buttons.append(send_btn)

        ttk.Label(tab, text='Decrypted Response (64 bytes):').pack(anchor='w', pady=(10, 2))

        self._raw_resp = tk.Text(tab, height=6, font=('Consolas', 10), state='disabled',
                                 wrap='none', bg='#1e1e1e', fg='#00ff88')
        self._raw_resp.pack(fill='both', expand=True)

    def _send_raw(self):
        try:
            cmd = int(self._raw_cmd_var.get().strip(), 16)
        except ValueError:
            messagebox.showerror('Error', 'Invalid command byte (enter hex, e.g. 04)')
            return
        payload = b''
        pay_str = self._raw_pay_var.get().strip()
        if pay_str:
            try:
                payload = bytes.fromhex(pay_str.replace(' ', ''))
            except ValueError:
                messagebox.showerror('Error', 'Invalid payload hex')
                return

        pkt = build_generic_packet(cmd, payload)

        def cb(plain, err):
            self._raw_resp.config(state='normal')
            self._raw_resp.delete('1.0', 'end')
            if err:
                self._raw_resp.insert('1.0', f'ERROR: {err}')
            else:
                self._raw_resp.insert('1.0', hex_dump(plain))
            self._raw_resp.config(state='disabled')

        self._run_command(pkt, cb)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    root = tk.Tk()
    PMVApp(root)
    root.mainloop()


if __name__ == '__main__':
    main()
