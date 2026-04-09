"""
pmv_editor.py — Sigma Metalytics PMV Database Editor
=====================================================

This tool reads, edits, and writes the encrypted .dat database files used by
the Sigma Metalytics Precious Metal Verifier (PMV) family of devices:

    - PMV Investor  (Invest x.xx.dat)
    - PMV Standard  (PMV Database x.xx.dat)
    - PMV Pro       (PRO x.xx.dat)
    - PMV Mini      (PMVMiniDatabase.dat)

All four device types use identical file format and encryption.

--- File Format Overview ---

Each .dat file is AES-128-CBC encrypted with PKCS7 padding.  Once decrypted,
the binary content follows C# BinaryReader conventions:

    [string]  Database description  e.g. "Invest 1.15"
    [string]  Creation timestamp    e.g. "2/24/2026 8:57:13 AM"
    [int32]   Number of records     e.g. 49
    For each record:
        [string]   Metal name       e.g. ".999"
        [int32]    Category ID      0=Gold  1=Silver  2=Other  3=Coins/Bullion
        [double]   ResGreenLeft
        [double]   ResYellowLeft
        [double]   ResGreenRight
        [double]   ResYellowRight
        [double]   Field5           (device-internal, purpose not fully documented)
        [double]   SpecificGravity  in g/cm³
        [double]   DimensionModePlusTolerance
        [double]   DimensionModeMinusTolerance
        [double]   TotalWeightMultiplier

Strings are length-prefixed using 7-bit LEB128 encoding (see read_string).
Integers are little-endian signed 32-bit.
Doubles are little-endian IEEE 754 64-bit.

--- Encryption ---

Algorithm : AES-128-CBC
Key size  : 16 bytes  (128 bits)
Block size: 16 bytes
Padding   : PKCS7

The key and IV were extracted from InvestorDatabaseDownloader.exe by parsing
its .NET IL bytecode.  They can be re-extracted if a new version ships with
different values — see extract_key_iv() and the --extract-key flag.

--- Usage ---

    python pmv_editor.py                                   # prompts for a .dat file
    python pmv_editor.py MyFile.dat                        # open any .dat file
    python pmv_editor.py --extract-key Downloader.exe      # extract key from exe
    python pmv_editor.py --use-key <key_hex> <iv_hex> f.dat  # supply key manually

--- Dependencies ---

    pip install pycryptodome    # AES encryption/decryption
    pip install dnfile          # only needed for --extract-key
"""

import sys
import io
import os
import json
import shutil
import struct
from datetime import datetime

# Ensure bundled Crypto/ is findable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


# =============================================================================
# Encryption key and IV
# =============================================================================
#
# The AES-128-CBC key and IV used to encrypt all PMV .dat files.
#
# These values are loaded at startup from 'pmv_key.json' (stored in the same
# directory as this script).  If that file does not exist, the built-in
# defaults below are used instead.
#
# pmv_key.json is created or updated automatically when you run:
#     python pmv_editor.py --extract-key InvestorDatabaseDownloader.exe
# and the extracted values differ from what is currently stored.  You never
# need to edit either file by hand.
#
# --- Built-in defaults (extracted from InvestorDatabaseDownloader.exe) ---

_DEFAULT_KEY = '32392013ded44052ae296db75bf03377'
_DEFAULT_IV  = '774abcf022b64aa193d519726f0144bd'

# Path to the key file — always placed next to this script so it is easy to
# find and is not accidentally committed into the wrong directory.
_KEY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pmv_key.json')


def _load_key_iv():
    """
    Load the AES key and IV from pmv_key.json if it exists, otherwise fall
    back to the built-in default values.

    Returns (key_bytes, iv_bytes) — each a 16-byte bytes object.
    """
    if os.path.exists(_KEY_FILE):
        try:
            data = json.loads(open(_KEY_FILE).read())
            return bytes.fromhex(data['key']), bytes.fromhex(data['iv'])
        except Exception as e:
            print(f"Warning: could not read '{_KEY_FILE}' ({e}). "
                  f"Using built-in default key.")
    return bytes.fromhex(_DEFAULT_KEY), bytes.fromhex(_DEFAULT_IV)


def _save_key_iv(key_bytes, iv_bytes):
    """
    Write the given key and IV to pmv_key.json so they persist across sessions.

    The file is written atomically (temp file then rename) to avoid leaving a
    half-written file if the process is interrupted.
    """
    data = json.dumps({'key': key_bytes.hex(), 'iv': iv_bytes.hex()}, indent=2)
    tmp  = _KEY_FILE + '.tmp'
    open(tmp, 'w').write(data)
    os.replace(tmp, _KEY_FILE)  # atomic on all major platforms
    print(f"  Key file updated: {_KEY_FILE}")


# Load key/IV from file (or defaults) at module import time.
# main() may override these globals with --use-key if the user supplies
# different values on the command line.
KEY, IV = _load_key_iv()


# =============================================================================
# Record field definitions
# =============================================================================
#
# Each metal record contains 9 floating-point measurement parameters.
# These are stored in this fixed order inside the file.

FIELD_NAMES = [
    'ResGreenLeft',               # Offset from the left edge of the bar to where the green zone starts
    'ResYellowLeft',              # Resistance threshold, yellow LED, left probe
    'ResGreenRight',              # Resistance threshold, green LED, right probe
    'ResYellowRight',             # Resistance threshold, yellow LED, right probe
    'Field5',                     # Device-internal value (exact purpose undocumented)
    'SpecificGravity',            # Specific gravity of the metal in g/cm³
    'DimensionModePlusTolerance', # Upper size tolerance for dimension mode
    'DimensionModeMinusTolerance',# Lower size tolerance for dimension mode
    'TotalWeightMultiplier',      # Multiplier applied to total weight reading
]

FIELD_DESCRIPTIONS = [
    'Offset from left edge of bar to start of green zone',
    'Resistance Yellow Left probe',
    'Resistance Green Right probe',
    'Resistance Yellow Right probe',
    'Field 5 (device-internal value)',
    'Specific Gravity (g/cm³)',
    'Dimension Mode Plus Tolerance',
    'Dimension Mode Minus Tolerance',
    'Total Weight Multiplier',
]

# Metal category IDs used in the file
CATEGORIES = {
    0: 'Gold',
    1: 'Silver',
    2: 'Other',          # Platinum, Palladium, Rhodium, Copper, Calibrator
    3: 'Coins/Bullion',  # Specific named coins with known dimensions
}


# =============================================================================
# Binary I/O — reading and writing the decrypted file content
# =============================================================================

def read_string(buf):
    """
    Read a C# BinaryReader string from the buffer.

    C# stores strings as: [length prefix][UTF-8 bytes]

    The length prefix uses 7-bit LEB128 (Little-Endian Base-128) encoding.
    Each byte contributes 7 bits of the length value.  If the high bit (bit 7)
    of a byte is set, another byte follows.  If it is clear, this is the last
    byte of the length prefix.

    Example: the string "Hi" (2 bytes) is stored as: 0x02 0x48 0x69
    Example: a 200-byte string prefix would be:      0xC8 0x01  (two bytes)
        0xC8 = 1100 1000  → high bit set, contributes bits 0–6: 100 1000 = 72
        0x01 = 0000 0001  → high bit clear, contributes bits 7–13: 000 0001
        Combined: 0000001_1001000 = 200 ✓
    """
    length = 0
    shift = 0  # how many bits we have accumulated so far

    while True:
        byte = buf.read(1)[0]

        # The lower 7 bits of this byte are the next 7 bits of the length value.
        # 'shift' tells us where in the final integer these bits belong.
        length |= (byte & 0x7F) << shift
        shift += 7

        # If the high bit is clear, this was the last byte of the prefix.
        if not (byte & 0x80):
            break

    return buf.read(length).decode('utf-8', 'replace')


def write_string(buf, s):
    """
    Write a C# BinaryWriter string to the buffer.

    Encodes the string as UTF-8, then writes the byte length as a 7-bit
    LEB128 prefix, followed by the raw UTF-8 bytes.

    See read_string() for a description of the LEB128 encoding.
    """
    encoded = s.encode('utf-8')
    length = len(encoded)

    # Write the length as 7-bit LEB128.
    # Each iteration emits the lowest 7 bits of the remaining length.
    # If more bits remain after shifting, set the high bit to signal continuation.
    while True:
        # Take the lowest 7 bits
        chunk = length & 0x7F
        length >>= 7

        if length > 0:
            # More bytes to come — set the high bit as a continuation flag
            buf.write(bytes([chunk | 0x80]))
        else:
            # This is the final byte — high bit stays clear
            buf.write(bytes([chunk]))
            break

    buf.write(encoded)


def read_int32(buf):
    """Read a little-endian signed 32-bit integer (4 bytes)."""
    return struct.unpack('<i', buf.read(4))[0]


def write_int32(buf, value):
    """Write a little-endian signed 32-bit integer (4 bytes)."""
    buf.write(struct.pack('<i', value))


def read_double(buf):
    """Read a little-endian IEEE 754 64-bit float (8 bytes)."""
    return struct.unpack('<d', buf.read(8))[0]


def write_double(buf, value):
    """Write a little-endian IEEE 754 64-bit float (8 bytes)."""
    buf.write(struct.pack('<d', value))


# =============================================================================
# Record — one precious metal entry
# =============================================================================

class Record:
    """
    Represents a single precious metal record in the database.

    Attributes:
        name        : Metal name shown on the device display, e.g. ".999"
        category_id : Integer 0–3 (see CATEGORIES)
        values      : List of 9 floats in FIELD_NAMES order
    """

    def __init__(self, name, category_id, values):
        self.name        = name
        self.category_id = category_id
        self.values      = list(values)  # copy so callers can't mutate the source

    @property
    def category(self):
        """Human-readable category name derived from category_id."""
        return CATEGORIES.get(self.category_id, f"Unknown({self.category_id})")

    def clone(self):
        """Return an independent copy of this record."""
        return Record(self.name, self.category_id, list(self.values))

    def to_dict(self):
        """Return all fields as a plain dictionary (useful for JSON/CSV export)."""
        d = {
            'name':        self.name,
            'category_id': self.category_id,
            'category':    self.category,
        }
        for i, field_name in enumerate(FIELD_NAMES):
            d[field_name] = self.values[i]
        return d

    def __repr__(self):
        return f"Record({self.name!r}, {self.category}, {self.values})"


# =============================================================================
# Database — the full .dat file contents
# =============================================================================

class Database:
    """
    Represents the complete contents of a PMV .dat database file.

    Attributes:
        description : Short version string stored in the file, e.g. "Invest 1.15"
        timestamp   : Creation date/time string, e.g. "2/24/2026 8:57:13 AM"
        records     : List of Record objects
    """

    def __init__(self, description='', timestamp='', records=None):
        self.description = description
        self.timestamp   = timestamp
        self.records     = records if records is not None else []

    # -------------------------------------------------------------------------
    # Loading (decrypt → parse)
    # -------------------------------------------------------------------------

    @classmethod
    def load(cls, path):
        """
        Decrypt and parse a .dat file from disk.

        Steps:
          1. Read the raw encrypted bytes from disk.
          2. Decrypt with AES-128-CBC using the global KEY and IV.
          3. Strip PKCS7 padding from the end of the decrypted data.
          4. Parse the binary content using C# BinaryReader conventions.

        Raises an exception if decryption or parsing fails (e.g. wrong key).
        """
        # Step 1: read the encrypted file
        encrypted_data = open(path, 'rb').read()

        # Step 2 & 3: decrypt and remove padding
        # AES.new() creates a new cipher object — it cannot be reused between
        # encrypt and decrypt operations, so we always create a fresh one.
        cipher    = AES.new(KEY, AES.MODE_CBC, IV)
        decrypted = unpad(cipher.decrypt(encrypted_data), AES.block_size)

        # Step 4: parse the binary content
        # Wrap in BytesIO so we can use sequential read calls, just like a file.
        buf = io.BytesIO(decrypted)

        description = read_string(buf)
        timestamp   = read_string(buf)
        num_records = read_int32(buf)

        records = []
        for _ in range(num_records):
            name        = read_string(buf)
            category_id = read_int32(buf)
            values      = [read_double(buf) for _ in range(9)]
            records.append(Record(name, category_id, values))

        # Sanity check: there should be nothing left over after parsing
        remaining = len(decrypted) - buf.tell()
        if remaining > 0:
            print(f"Warning: {remaining} unparsed bytes at end of file — "
                  f"the file format may have changed.")

        return cls(description, timestamp, records)

    # -------------------------------------------------------------------------
    # Saving (serialize → encrypt)
    # -------------------------------------------------------------------------

    def save(self, path):
        """
        Serialize and encrypt the database, then write it to disk.

        This is the exact reverse of load():
          1. Write all fields to a byte buffer using C# BinaryWriter conventions.
          2. Apply PKCS7 padding to make the length a multiple of 16 bytes.
          3. Encrypt with AES-128-CBC.
          4. Write the encrypted bytes to disk.

        The output file is a valid .dat file readable by PMV devices.
        """
        # Step 1: serialize to a byte buffer
        buf = io.BytesIO()

        write_string(buf, self.description)
        write_string(buf, self.timestamp)
        write_int32(buf, len(self.records))

        for record in self.records:
            write_string(buf, record.name)
            write_int32(buf, record.category_id)
            for value in record.values:
                write_double(buf, value)

        plaintext = buf.getvalue()

        # Step 2: pad to a multiple of 16 bytes (AES block size)
        # PKCS7 padding adds between 1 and 16 bytes; if the data already ends
        # on a block boundary, a full extra block of padding is added so the
        # device can always detect and strip the padding unambiguously.
        padded = pad(plaintext, AES.block_size)

        # Step 3: encrypt
        # A new cipher object is required for each encryption operation.
        cipher    = AES.new(KEY, AES.MODE_CBC, IV)
        encrypted = cipher.encrypt(padded)

        # Step 4: write to disk
        open(path, 'wb').write(encrypted)
        print(f"Saved {len(self.records)} records to '{path}' "
              f"({len(encrypted)} bytes encrypted).")

    # -------------------------------------------------------------------------
    # Display helpers
    # -------------------------------------------------------------------------

    def print_header(self):
        """Print the database description, timestamp, and record count."""
        print(f"\n  Description : {self.description}")
        print(f"  Timestamp   : {self.timestamp}")
        print(f"  Records     : {len(self.records)}")

    def print_list(self):
        """Print all records as a table with every column visible."""

        # Determine column widths for the fixed columns based on content
        num_w  = max(len('#'),        len(str(len(self.records))))
        name_w = max(len('Name'),     max((len(r.name)     for r in self.records), default=4))
        cat_w  = max(len('Category'), max((len(r.category) for r in self.records), default=8))

        # Determine column widths for the 9 numeric fields
        value_w = []
        for col, field_name in enumerate(FIELD_NAMES):
            widest_value = max(len(f"{r.values[col]:.6g}") for r in self.records) if self.records else 4
            value_w.append(max(len(field_name), widest_value))

        def make_row(num, name, category, values):
            fixed = f"  {num:>{num_w}}  {name:<{name_w}}  {category:<{cat_w}}"
            # Values can be either strings (header row) or floats (data rows)
            if values and isinstance(values[0], str):
                nums = "  ".join(f"{v:>{value_w[j]}}" for j, v in enumerate(values))
            else:
                nums = "  ".join(f"{v:>{value_w[j]}.6g}" for j, v in enumerate(values))
            return f"{fixed}  {nums}"

        def make_separator():
            fixed = f"  {'-'*num_w}  {'-'*name_w}  {'-'*cat_w}"
            nums  = "  ".join('-' * w for w in value_w)
            return f"{fixed}  {nums}"

        print()
        print(make_row('#', 'Name', 'Category', FIELD_NAMES))
        print(make_separator())
        for i, record in enumerate(self.records):
            print(make_row(i + 1, record.name, record.category, record.values))

    def print_record(self, index):
        """Print the full detail of a single record as a two-row table.
        The first row is the column headers, the second row is the values."""
        record = self.records[index]
        print(f"\n  Record {index+1}: {record.name}  [{record.category}]")

        # Build column widths based on the longer of the field name or value string
        values_str = [f"{v:.6g}" for v in record.values]
        col_widths = [
            max(len(name), len(val))
            for name, val in zip(FIELD_NAMES, values_str)
        ]

        # Header row
        header = "  | " + " | ".join(
            name.center(w) for name, w in zip(FIELD_NAMES, col_widths)
        ) + " |"

        # Separator row
        separator = "  |-" + "-|-".join(
            "-" * w for w in col_widths
        ) + "-|"

        # Value row
        values_row = "  | " + " | ".join(
            val.center(w) for val, w in zip(values_str, col_widths)
        ) + " |"

        print(separator)
        print(header)
        print(separator)
        print(values_row)
        print(separator)


# =============================================================================
# Key/IV extraction from the Windows downloader executable
# =============================================================================

def extract_key_iv(exe_path):
    """
    Automatically extract the AES Key and IV from InvestorDatabaseDownloader.exe
    (or PMVMini.exe / PMVDatabaseDownloader.exe).

    Background
    ----------
    The executable is a .NET (C#) application.  In C#, when you write:

        private readonly byte[] Key = new byte[] { 0x32, 0x39, ... };

    the compiler stores the raw byte values in a special section of the PE file
    called a FieldRVA.  At runtime, the constructor (.ctor) copies those bytes
    into the instance field using RuntimeHelpers.InitializeArray().

    In .NET IL (Intermediate Language) bytecode, this pattern looks like:

        ldc.i4.s  16          // push array size (16 bytes)
        newarr    uint8        // allocate new byte array
        dup                    // duplicate the reference
        ldtoken   <field>      // push a reference to the FieldRVA data  ← 0xD0
        call      InitializeArray  // copy the raw data into the array

    We find this pattern by:
      1. Building a map of which fields have FieldRVA entries (raw data in the PE).
      2. Finding the class constructor (.ctor) method.
      3. Scanning its IL for 'ldtoken' instructions (opcode 0xD0) that reference
         fields in the FieldRVA map.
      4. Reading 16 bytes from the PE at each referenced field's RVA.
      5. The first ldtoken in the constructor initialises the Key; the second
         initialises the IV.

    Returns
    -------
    (key_bytes, iv_bytes) — each a 16-byte bytes object.

    Raises RuntimeError if extraction fails.
    """
    try:
        import dnfile
    except ImportError:
        raise RuntimeError(
            "The 'dnfile' library is required for key extraction.\n"
            "Install it with:  pip install dnfile"
        )

    # Parse the PE file as a .NET assembly
    pe  = dnfile.dnPE(exe_path)
    dn  = pe.net
    raw = pe.__data__  # raw bytes of the entire PE file

    def rva_to_bytes(rva, count):
        """
        Convert a Relative Virtual Address (RVA) to a file offset and read
        'count' bytes from the PE's raw data at that location.

        RVA is an offset relative to the PE's preferred load address.
        dnfile provides get_offset_from_rva() to convert it to a file offset.
        """
        file_offset = pe.get_offset_from_rva(rva)
        return raw[file_offset : file_offset + count]

    # ------------------------------------------------------------------
    # Step 1: Build a map of  field_row_index → RVA
    #         for all fields that have embedded data (FieldRVA table entries)
    # ------------------------------------------------------------------
    # The .NET FieldRVA metadata table records which static fields have their
    # initial values stored directly in the PE file, and at what RVA.
    field_rva_map = {}
    frva_table = dn.mdtables.FieldRva
    for i in range(frva_table.num_rows):
        row = frva_table.rows[i]
        field_row_index         = row.Field.row_index  # 1-based index into Field table
        field_rva_map[field_row_index] = row.Rva

    # ------------------------------------------------------------------
    # Step 2: Scan the constructor's IL for ldtoken references to those fields
    # ------------------------------------------------------------------

    def find_field_ldtokens_in_method(method_rva):
        """
        Parse the IL bytecode of a method and return a list of
        (il_byte_offset, field_row_index) for every 'ldtoken' instruction
        that references a field in the Field metadata table.

        IL Method headers come in two formats:
          - Tiny:  1 byte.  Bits [7:2] = code size, bits [1:0] = 0b10.
          - Fat:   12 bytes.  Contains flags, max stack, code size, etc.
            Identified by bits [1:0] of the first byte being 0b11.
        """
        method_file_offset = pe.get_offset_from_rva(method_rva)
        first_byte = raw[method_file_offset]

        # Determine header format from the lowest 2 bits
        if (first_byte & 0x3) == 0x2:
            # Tiny header: the code starts immediately after the single header byte
            code_size        = first_byte >> 2       # upper 6 bits = size
            code_start_offset = method_file_offset + 1
        else:
            # Fat header: 12-byte header, code size is at bytes 4–7
            # Format: flags(2) + max_stack(2) + code_size(4) + ...
            _flags, _max_stack, code_size = struct.unpack_from(
                '<HHI', raw, method_file_offset
            )
            code_start_offset = method_file_offset + 12

        il_bytes = raw[code_start_offset : code_start_offset + code_size]

        field_refs = []
        pos = 0
        while pos < len(il_bytes):
            opcode = il_bytes[pos]

            if opcode == 0xD0:
                # 'ldtoken' instruction — 4-byte metadata token follows.
                # A metadata token encodes both the table ID and the row index:
                #   Bits [31:24] = table ID   (0x04 = Field table)
                #   Bits [23:0]  = row index  (1-based)
                token = struct.unpack_from('<I', il_bytes, pos + 1)[0]

                table_id  = (token >> 24) & 0xFF    # which metadata table
                row_index = token & 0x00FFFFFF       # which row in that table

                # We only care about references to the Field table (table 0x04)
                if table_id == 0x04:
                    field_refs.append((pos, row_index))

                pos += 5  # 1 byte opcode + 4 byte token

            elif opcode == 0xFE:
                # Two-byte opcode prefix (e.g. ceq, cgt, clt, etc.)
                # Skip both bytes
                pos += 2

            else:
                # All other opcodes: advance by 1 byte.
                # Note: this is a simplified scanner — it does not decode
                # operand lengths for every opcode, but it is sufficient for
                # finding ldtoken instructions in short constructor methods.
                pos += 1

        return field_refs

    # Find the class constructor (.ctor) and collect any ldtoken refs to FieldRVA fields
    methods_table = dn.mdtables.MethodDef
    ctor_field_refs = []  # list of (il_offset, field_row_index, rva)

    for i in range(methods_table.num_rows):
        method_row = methods_table.rows[i]

        # Skip non-constructor methods and abstract/extern methods (RVA = 0)
        if str(method_row.Name) != '.ctor' or method_row.Rva == 0:
            continue

        refs = find_field_ldtokens_in_method(method_row.Rva)

        for il_offset, field_row_index in refs:
            # Only keep references to fields that actually have FieldRVA data
            if field_row_index in field_rva_map:
                rva = field_rva_map[field_row_index]
                ctor_field_refs.append((il_offset, field_row_index, rva))

    # ------------------------------------------------------------------
    # Step 3: Extract the key and IV bytes
    # ------------------------------------------------------------------

    if len(ctor_field_refs) >= 2:
        # Primary method: use the constructor ldtoken order.
        # Sort by IL byte offset to get them in the order the constructor
        # initialises them — first is the Key, second is the IV.
        ctor_field_refs.sort(key=lambda entry: entry[0])  # sort by il_offset

        key_bytes = rva_to_bytes(ctor_field_refs[0][2], 16)
        iv_bytes  = rva_to_bytes(ctor_field_refs[1][2], 16)

    elif len(field_rva_map) >= 2:
        # Fallback: if the constructor scan didn't find enough refs
        # (e.g. due to obfuscation), fall back to reading the FieldRVA entries
        # directly, sorted by their address in the file (lowest RVA first = Key).
        sorted_entries = sorted(field_rva_map.items(), key=lambda entry: entry[1])
        key_bytes = rva_to_bytes(sorted_entries[0][1], 16)
        iv_bytes  = rva_to_bytes(sorted_entries[1][1], 16)

    else:
        raise RuntimeError(
            f"Could not find AES key/IV in '{exe_path}'. "
            f"Found {len(field_rva_map)} FieldRVA entries "
            f"(expected at least 2)."
        )

    return key_bytes, iv_bytes


# =============================================================================
# Interactive prompt helpers
# =============================================================================

def prompt(message, default=None):
    """
    Print a prompt and return the user's input.
    If the user presses Enter without typing anything, return 'default'.
    """
    suffix = f" [{default}]" if default is not None else ""
    value = input(f"  {message}{suffix}: ").strip()
    if value == '' and default is not None:
        return default
    return value


def prompt_float(message, default=None):
    """Prompt repeatedly until the user enters a valid number."""
    while True:
        value = prompt(message, default)
        try:
            return float(value)
        except ValueError:
            print("  Please enter a number.")


def prompt_int(message, choices=None, default=None):
    """
    Prompt repeatedly until the user enters a valid integer.
    If 'choices' is given, also check that the value is in that collection.
    """
    while True:
        value = prompt(message, default)
        try:
            v = int(value)
            if choices is None or v in choices:
                return v
            print(f"  Please choose from: {list(choices)}")
        except ValueError:
            print("  Please enter an integer.")


def prompt_category():
    """Show the category menu and return the chosen category ID."""
    print()
    for category_id, category_name in CATEGORIES.items():
        print(f"    {category_id} = {category_name}")
    return prompt_int("Category", choices=list(CATEGORIES.keys()))


def edit_record_interactive(record):
    """
    Walk through every field of a record and offer the user a chance to change it.
    Changes are made in-place on the record object.
    Returns True if any field was changed, False otherwise.
    """
    changed = False
    print(f"\n  Editing: {record.name}  (press Enter to keep current value)")

    new_name = prompt("Name", record.name)
    if new_name != record.name:
        record.name = new_name
        changed = True

    print(f"\n  Current category: {record.category_id} = {record.category}")
    if prompt("Change category? (y/n)", 'n').lower() == 'y':
        record.category_id = prompt_category()
        changed = True

    print()
    for i, (field_name, field_desc) in enumerate(
            zip(FIELD_NAMES, FIELD_DESCRIPTIONS)):
        new_value = prompt_float(f"{field_name} ({field_desc})", record.values[i])
        if new_value != record.values[i]:
            record.values[i] = new_value
            changed = True

    return changed


# Reference specific gravities shown during calibration
_SG_REFERENCE = [
    ('Gold',      19.30),
    ('Silver',    10.49),
    ('Platinum',  21.45),
    ('Palladium', 12.02),
    ('Rhodium',   12.41),
    ('Copper',     8.96),
    ('Tungsten',  19.25),
    ('Lead',      11.34),
]


def new_record_wizard(db):
    """
    Guided wizard for adding a new metal record to the database.

    Walks the user through every field one step at a time, explaining in plain
    language what each value means, where to find it, and what will happen if
    it is set incorrectly.  All values are entered manually by the user.

    Returns a new Record object, or None if the user cancels.
    """
    print()
    print("  ╔══════════════════════════════════════════════════════╗")
    print("  ║             New Record Wizard                        ║")
    print("  ╚══════════════════════════════════════════════════════╝")
    print()
    print("  This wizard will guide you through adding a new metal to the")
    print("  database.  Each step explains what the field means and how to")
    print("  find the correct value.")
    print()
    print("  You will need:")
    print("    - The name you want shown on the device display")
    print("    - One or more known genuine samples of this metal")
    print("    - The specific gravity (density) of the metal")
    print("    - Access to your PMV device to take bar readings")
    print()
    print("  Press Ctrl-C at any time to cancel.")
    print()

    try:
        # ── Step 1: name ──────────────────────────────────────────────────
        print("  Step 1/5 — Metal name")
        print("  " + "─" * 50)
        print("  This is the label that will appear on the device screen")
        print("  when a sample is tested against this record.  Keep it")
        print("  short and descriptive.")
        print()
        print("  Examples:  .999  |  .9999  |  18k Yellow  |  Maple Leaf 1oz")
        print()
        name = prompt("  Metal name")
        if not name:
            print("  Cancelled — no name entered.")
            return None

        # ── Step 2: category ─────────────────────────────────────────────
        print()
        print("  Step 2/5 — Category")
        print("  " + "─" * 50)
        print("  The category tells the device which group this metal belongs")
        print("  to.  It affects how the device organises and displays records.")
        print()
        print("    0 = Gold          — all gold alloys and gold-plated items")
        print("    1 = Silver        — all silver alloys")
        print("    2 = Other         — platinum, palladium, rhodium, copper,")
        print("                        calibration discs, and anything else")
        print("    3 = Coins/Bullion — specific named coins where the exact")
        print("                        dimensions and weight are also checked")
        print()
        category_id = prompt_int("  Category (0/1/2/3)", choices=list(CATEGORIES.keys()))

        same_cat  = [(i, r) for i, r in enumerate(db.records)
                     if r.category_id == category_id]
        other_cat = [(i, r) for i, r in enumerate(db.records)
                     if r.category_id != category_id]

        # ── Step 3: ResGreenLeft (f0) — offset from bar left to green zone start ──
        # ResGreenLeft stores the offset from the left edge of the measurement bar
        # to the point where the green (pass) zone begins.  It is expressed in a
        # different scale (roughly 350–2200) to the resistance readings visible on
        # the display (typically 1–20), so it cannot be read directly from the
        # device screen.  The safest approach is to copy it from a record that uses
        # the same physical bar position — i.e. a metal with a similar resistance
        # profile — and adjust with 'e' afterwards if necessary.
        print()
        print("  Step 3/5 — Green zone left offset (ResGreenLeft)")
        print("  " + "─" * 50)
        print("  ResGreenLeft defines how far from the left edge of the measurement")
        print("  bar the green (pass) zone begins.  It is stored in a different scale")
        print("  to the resistance readings shown on the display (range roughly")
        print("  350–2200), so its value cannot be read directly off the screen.")
        print()
        print("  The best approach is to copy it from the most physically similar")
        print("  existing record — a metal whose green zone starts at roughly the")
        print("  same position on the bar.")
        print()
        print("  How to choose:")
        print("    - For a new gold alloy, pick the gold record closest in purity.")
        print("    - For a new silver alloy, pick the silver record closest in purity.")
        print("    - For a coin, pick another coin of the same base metal.")
        print("    - If nothing is similar, pick any record in the same category")
        print("      and fine-tune later with 'e' if the device does not respond")
        print("      as expected.")
        print()

        if db.records:
            if same_cat:
                print(f"  Same category ({CATEGORIES[category_id]}):")
                for i, r in same_cat:
                    print(f"    {i+1:3}.  {r.name:<28}  ResGreenLeft = {r.values[0]:.6g}")
            if other_cat:
                print()
                print("  Other categories:")
                for i, r in other_cat:
                    print(f"    {i+1:3}.  [{r.category:<14}]  {r.name:<20}  "
                          f"ResGreenLeft = {r.values[0]:.6g}")
            print()
            ref_idx = prompt_int(
                f"  Copy ResGreenLeft from record number (1–{len(db.records)})",
                choices=range(1, len(db.records) + 1)
            ) - 1
            f0 = db.records[ref_idx].values[0]
            print(f"  Using ResGreenLeft = {f0:.6g}  "
                  f"(copied from '{db.records[ref_idx].name}')")
        else:
            print("  No existing records to copy from.")
            print("  The default value of 1975 is typical for gold.")
            f0 = prompt_float("  Enter ResGreenLeft manually", 1975.0)

        # ── Step 4: bar thresholds (f1–f4) ───────────────────────────────
        # The measurement bar on the device display is divided into colour zones:
        #
        #   Red (fail) | Yellow (caution) | Green (pass) | Yellow (caution) | Red (fail)
        #              f1                 f2             f3                 f4
        #
        # f1 = outer left boundary  — below this the bar shows red (fail)
        # f2 = inner left boundary  — between f1 and f2 the bar shows yellow (caution)
        # f3 = inner right boundary — between f2 and f3 the bar shows green (pass)
        # f4 = outer right boundary — between f3 and f4 yellow again; above f4 red
        #
        # We ask for the lowest and highest readings seen on known genuine samples.
        # Those become f2 and f3 (the green zone).  A 7 % margin is added on each
        # side to create the yellow caution zones (f1 and f4).
        print()
        print("  Step 4/5 — Measurement bar thresholds")
        print("  " + "─" * 50)
        print("  The device shows a horizontal bar when a sample is tested.")
        print("  The bar is divided into colour zones:")
        print()
        print("    Red (fail) | Yellow (caution) | Green (pass) | Yellow (caution) | Red (fail)")
        print()
        print("  A genuine sample should produce a reading that lands in the")
        print("  GREEN zone.  You need to tell the wizard what range of readings")
        print("  you observe when testing known genuine pieces.")
        print()
        print("  How to get the readings:")
        print("    1. Place a known genuine sample flat on the device probes.")
        print("    2. Wait for the reading to stabilise (stops changing).")
        print("    3. Note the numeric value shown beside or below the bar.")
        print("    4. Repeat with as many genuine pieces as you have.")
        print("    5. Enter the lowest and highest values you observed below.")
        print()
        print("  If you only have one piece, use its reading for both min and max.")
        print("  The wizard will automatically add a safety margin around the")
        print("  green zone to create the yellow caution zone.")
        print()

        while True:
            bar_min = prompt_float("  Lowest  reading observed across genuine samples")
            bar_max = prompt_float("  Highest reading observed across genuine samples")
            if bar_min <= 0 or bar_max <= 0:
                print("  Readings must be positive numbers.  Please try again.")
                continue
            if bar_min > bar_max:
                bar_min, bar_max = bar_max, bar_min
                print(f"  (Values swapped: treating {bar_min} as min and {bar_max} as max)")
            break

        # Calculate the four thresholds.
        # The green zone is exactly the observed range [bar_min, bar_max].
        # The yellow zones are a 7 % margin outside that range.
        # Below f1 or above f4 the device shows red.
        margin = 0.07
        f1 = round(bar_min * (1.0 - margin), 4)   # outer yellow left boundary
        f2 = round(bar_min, 4)                      # green zone left edge
        f3 = round(bar_max, 4)                      # green zone right edge
        f4 = round(bar_max * (1.0 + margin), 4)    # outer yellow right boundary

        print()
        print("  Calculated thresholds (7 % yellow margin applied):")
        print(f"    ResYellowLeft  = {f1}  ← outer yellow left  (below this = red/fail)")
        print(f"    ResGreenRight  = {f2}  ← green zone starts  (genuine range begins)")
        print(f"    ResYellowRight = {f3}  ← green zone ends    (genuine range ends)")
        print(f"    Field5         = {f4}  ← outer yellow right (above this = red/fail)")
        print()
        print("  Bar zones with these thresholds:")
        print(f"    Red | Yellow | Green | Yellow | Red")
        print(f"        {f1:<9} {f2:<9} {f3:<9} {f4}")
        print()
        print("  The yellow zones act as a caution buffer — a reading there")
        print("  means 'close but not firmly within the known genuine range'.")
        print("  Adjust the margin if you want a wider or narrower buffer.")
        print()

        tweak = prompt("  Yellow margin % (press Enter to keep 7 %)", None)
        if tweak:
            try:
                margin = float(tweak) / 100.0
                f1 = round(bar_min * (1.0 - margin), 4)
                f4 = round(bar_max * (1.0 + margin), 4)
                print(f"  Updated outer boundaries: f1={f1}  f4={f4}")
            except ValueError:
                print("  Invalid number — keeping 7 % margin.")

        # ── Step 5: specific gravity and tolerances ───────────────────────
        print()
        print("  Step 5/5 — Specific gravity and tolerances")
        print("  " + "─" * 50)
        print("  Specific gravity is the density of the metal relative to water.")
        print("  Pure water = 1.0.  Gold is about 19.3 times denser, so its")
        print("  specific gravity is 19.3.")
        print()
        print("  The device uses this value in its hydrostatic weighing mode")
        print("  to estimate whether the sample's density matches the record.")
        print()
        print("  Common reference values (g/cm³ = specific gravity for metals):")
        cols = [_SG_REFERENCE[i:i+4] for i in range(0, len(_SG_REFERENCE), 4)]
        for row in cols:
            print("    " + "   ".join(f"{sg_name:<10} {sg:.2f}" for sg_name, sg in row))
        print()
        print("  For alloys (e.g. 18k gold), use the published density for that")
        print("  specific alloy composition, or look it up by karat/fineness.")
        print("  Example: 18k yellow gold ≈ 15.5,  14k yellow gold ≈ 13.1")
        print()
        f5 = prompt_float("  Specific gravity (g/cm³)")

        # Dimension tolerances and weight multiplier
        # These are only relevant when the device is used in dimension/coin mode,
        # where it checks that the physical size and weight of a coin match the
        # record.  For generic metal records they have little effect.
        # We default to values from a similar existing record, or 1.0 if none exist.
        print()
        print("  Dimension tolerances and weight multiplier")
        print("  " + "─" * 50)
        print("  These three values are only relevant if you use the device in")
        print("  Dimension Mode (coin checking).  They define how much the")
        print("  physical size and weight of a sample may deviate from the")
        print("  expected value before the device flags it:")
        print()
        print("    DimensionModePlusTolerance  — maximum allowed size above spec")
        print("    DimensionModeMinusTolerance — maximum allowed size below spec")
        print("    TotalWeightMultiplier       — scaling factor for the weight")
        print("                                  reading (1.0 = no adjustment)")
        print()
        print("  For standard resistivity testing (not coin mode) these values")
        print("  have no effect.  It is safe to accept the defaults.")
        print()

        if same_cat:
            ref = same_cat[0][1]
            def_f6, def_f7, def_f8 = ref.values[6], ref.values[7], ref.values[8]
            print(f"  Defaults copied from '{ref.name}' (same category):")
        else:
            def_f6, def_f7, def_f8 = 1.0, 1.0, 1.0
            print("  No same-category record found — using 1.0 for all three.")

        print(f"    DimensionModePlusTolerance  = {def_f6}")
        print(f"    DimensionModeMinusTolerance = {def_f7}")
        print(f"    TotalWeightMultiplier       = {def_f8}")
        print()
        if prompt("  Change any of these values? (y/n)", 'n').lower() == 'y':
            print()
            print("  Enter new values, or press Enter to keep the default shown.")
            f6 = prompt_float("  DimensionModePlusTolerance",  def_f6)
            f7 = prompt_float("  DimensionModeMinusTolerance", def_f7)
            f8 = prompt_float("  TotalWeightMultiplier",       def_f8)
        else:
            f6, f7, f8 = def_f6, def_f7, def_f8

        # ── Summary and confirm ───────────────────────────────────────────
        record = Record(name, category_id, [f0, f1, f2, f3, f4, f5, f6, f7, f8])
        print()
        print("  ── New record summary ──────────────────────────────────────")
        print(f"  Name        : {record.name}")
        print(f"  Category    : {record.category}")
        for fname, fval in zip(FIELD_NAMES, record.values):
            print(f"  {fname:<28}: {fval:.6g}")
        print()
        if prompt("  Add this record to the database? (y/n)", 'y').lower() != 'y':
            print("  Cancelled.")
            return None

        return record

    except KeyboardInterrupt:
        print("\n  Wizard cancelled.")
        return None


# =============================================================================
# Backup helper
# =============================================================================

def make_backup(original_path):
    """
    Copy original_path to Backup-<filename> in the same directory.

    The backup is only created once per session — if the backup file already
    exists from a previous call this session, it is left untouched so it
    continues to reflect the true original state of the file before any edits
    were made.

    Returns the backup path.
    """
    directory = os.path.dirname(os.path.abspath(original_path))
    basename  = os.path.basename(original_path)
    backup_path = os.path.join(directory, 'Backup-' + basename)

    if not os.path.exists(backup_path):
        shutil.copy2(original_path, backup_path)
        print(f"  Backup created: {backup_path}")
    else:
        print(f"  Backup already exists, not overwritten: {backup_path}")

    return backup_path


# =============================================================================
# Main — command-line interface and interactive menu
# =============================================================================

def main():
    # KEY and IV are module-level globals so that --extract-key and --use-key
    # can override them before any file is loaded or saved.
    global KEY, IV

    args = sys.argv[1:]

    # ------------------------------------------------------------------
    # --extract-key : read the key and IV from a downloader .exe
    # ------------------------------------------------------------------
    if '--extract-key' in args:
        args.remove('--extract-key')
        exe_path = args[0] if args else None

        if not exe_path or not os.path.exists(exe_path):
            print("Usage: python pmv_editor.py --extract-key <InvestorDatabaseDownloader.exe>")
            sys.exit(1)

        print(f"Extracting key/IV from: {exe_path}")
        try:
            extracted_key, extracted_iv = extract_key_iv(exe_path)
            print(f"  Key : {extracted_key.hex()}")
            print(f"  IV  : {extracted_iv.hex()}")

            if extracted_key == KEY and extracted_iv == IV:
                # The extracted values match what is already in use — nothing to do.
                print("  Key/IV match the current values — no update needed.")
            else:
                # The key has changed.  Write it to pmv_key.json so all future
                # sessions pick it up automatically without any manual editing.
                print("  Key/IV differ from the current values — saving to key file.")
                _save_key_iv(extracted_key, extracted_iv)
                KEY, IV = extracted_key, extracted_iv
                print("  New key/IV is now active for this session and all future sessions.")

        except Exception as error:
            print(f"  Extraction failed: {error}")
            sys.exit(1)

        # If the user only wanted to extract the key (no .dat file given), stop.
        remaining_args = args[1:]
        if not remaining_args:
            sys.exit(0)
        args = remaining_args

    # ------------------------------------------------------------------
    # --use-key : manually supply a hex key and IV on the command line
    # ------------------------------------------------------------------
    if '--use-key' in args:
        idx     = args.index('--use-key')
        key_hex = args[idx + 1]
        iv_hex  = args[idx + 2]
        KEY     = bytes.fromhex(key_hex)
        IV      = bytes.fromhex(iv_hex)
        # Remove the flag and its two arguments from the list
        args    = args[:idx] + args[idx + 3:]
        print(f"Using supplied key={key_hex}  iv={iv_hex}")

    # If no filename was given on the command line, ask the user for one.
    # Keep asking until they provide a path that actually exists.
    if args:
        dat_path = args[0]
    else:
        while True:
            dat_path = input("  Enter path to .dat file: ").strip()
            if dat_path:
                break
            print("  Please enter a filename.")

    if not os.path.exists(dat_path):
        print(f"File not found: {dat_path}")
        sys.exit(1)
    print(f"Loading: {dat_path}")
    try:
        db = Database.load(dat_path)
    except Exception:
        # The most common cause of a load failure is a changed AES key.
        # pycryptodome raises ValueError("PKCS#7 padding is incorrect") when
        # the wrong key is used, but we catch all exceptions here so any other
        # parse error also gets a helpful message instead of a raw traceback.
        print()
        print("  ERROR: Failed to decrypt or parse the database file.")
        print()
        print("  The most likely cause is that Sigma Metalytics has released a new")
        print("  version of their downloader with a different AES encryption key.")
        print()
        print("  To fix this:")
        print("    1. Download the latest installer from:")
        print("       https://www.sigmametalytics.com/pages/database-updaters")
        print("    2. Extract InvestorDatabaseDownloader.exe from the installer.")
        print("    3. Run:")
        print("         python pmv_editor.py --extract-key InvestorDatabaseDownloader.exe")
        print("       This will update pmv_key.json automatically.")
        print("    4. Then open your .dat file again.")
        print()
        print("  If the problem persists, the file may be corrupted or not a valid")
        print("  PMV database file.")
        sys.exit(1)

    db.print_header()

    # Track whether there are changes that haven't been saved yet,
    # so we can warn the user before they quit.
    unsaved_changes = False

    # original_path holds the path of the file that was loaded at startup.
    # It is used by make_backup() to always back up the true original, even
    # if the user later saves to a different filename with 'w'.
    original_path = os.path.abspath(dat_path)

    # ------------------------------------------------------------------
    # Interactive menu loop
    # ------------------------------------------------------------------
    #
    # Commands that operate on a specific record (v, e, d, c) accept an
    # optional row number directly in the input, e.g. "v 3" or "e 12".
    # If no number is given, the full list is shown and the user is asked
    # to enter one.

    def parse_command(raw_input):
        """
        Split user input into a command letter and an optional row number.

        Examples:
            "l"    → ('l', None)
            "v 3"  → ('v', 3)
            "e12"  → ('e', 12)   ← space is optional
        """
        parts = raw_input.strip().lower().split(None, 1)  # split on first whitespace
        if not parts:
            return '', None
        letter = parts[0][0]       # first character is the command letter
        # If the user wrote the number immediately after the letter (e.g. "e12"),
        # the rest of parts[0] is the number; otherwise it's in parts[1].
        inline_num = parts[0][1:]  # anything after the command letter
        if inline_num:
            num_str = inline_num
        elif len(parts) > 1:
            num_str = parts[1]
        else:
            num_str = ''
        # Try to convert to an integer; leave as None if not present or invalid
        try:
            number = int(num_str) if num_str else None
        except ValueError:
            number = None
        return letter, number

    def resolve_record_number(row_num, action_label):
        """
        Return a valid 0-based record index.

        If row_num was already provided (from the command input), validate it.
        If not, show the list and prompt the user to choose.
        Returns None if the user provides an invalid number.
        """
        if row_num is not None:
            # The user supplied a number with their command — validate it
            if 1 <= row_num <= len(db.records):
                return row_num - 1
            else:
                print(f"  Record {row_num} does not exist. "
                      f"Valid range: 1–{len(db.records)}.")
                return None
        else:
            # No number given — show the list so the user can choose
            db.print_list()
            return prompt_int(
                f"Record number to {action_label} (1-{len(db.records)})",
                choices=range(1, len(db.records) + 1)
            ) - 1

    while True:
        print(f"\n{'─'*55}")
        status = " [unsaved changes]" if unsaved_changes else ""
        print(f"  DATABASE: {db.description.strip()}  ({len(db.records)} records){status}")
        print(f"{'─'*55}")
        print("  l              List all records in the database")
        print("  v [#]          View all fields of one record in detail")
        print("  e [#]          Edit the fields of one record")
        print("  n              Add a new metal record (guided wizard)")
        print("  d [#]          Delete a record permanently")
        print("  c [#]          Duplicate a record as a starting point for a new one")
        print("  s              Save all changes back to the current file (auto-backup created)")
        print("  q              Quit — you will be warned if there are unsaved changes")
        print()
        print("  Tip: for v, e, d and c you can include the record number directly,")
        print("       e.g.  'v 5'  or  'e 12'  to skip the list step.")
        print()

        cmd, row_num = parse_command(input("  Choice: "))

        if cmd == 'l':
            db.print_list()

        elif cmd == 'v':
            idx = resolve_record_number(row_num, 'view')
            if idx is not None:
                db.print_record(idx)

        elif cmd == 'e':
            idx = resolve_record_number(row_num, 'edit')
            if idx is not None:
                db.print_record(idx)
                changed = edit_record_interactive(db.records[idx])
                if changed:
                    unsaved_changes = True
                    print("  Record updated.")
                else:
                    print("  No changes made.")

        elif cmd == 'n':
            new_record = new_record_wizard(db)
            if new_record is not None:
                if db.records:
                    db.print_list()
                    pos = prompt_int(
                        f"Insert at position (1-{len(db.records)+1}, default=end)",
                        default=len(db.records) + 1
                    ) - 1
                    pos = max(0, min(pos, len(db.records)))
                else:
                    pos = 0
                db.records.insert(pos, new_record)
                unsaved_changes = True
                print(f"  Added '{new_record.name}' at position {pos + 1}.")

        elif cmd == 'd':
            idx = resolve_record_number(row_num, 'delete')
            if idx is not None:
                db.print_record(idx)
                record = db.records[idx]
                if prompt(f"  Delete '{record.name}'? This cannot be undone. (y/n)", 'n').lower() == 'y':
                    db.records.pop(idx)
                    unsaved_changes = True
                    print(f"  Deleted '{record.name}'.")
                else:
                    print("  Cancelled.")

        elif cmd == 'c':
            idx = resolve_record_number(row_num, 'copy')
            if idx is not None:
                clone = db.records[idx].clone()
                clone.name = clone.name + ' (copy)'
                # Insert the copy immediately after the original
                db.records.insert(idx + 1, clone)
                unsaved_changes = True
                print(f"  Copied to position {idx + 2} as '{clone.name}'.")
                print("  Use 'e' to rename and edit the copy.")

        elif cmd == 's':
            make_backup(original_path)
            db.save(dat_path)
            unsaved_changes = False

        elif cmd == 'q':
            if unsaved_changes:
                if prompt("You have unsaved changes. Quit anyway? (y/n)", 'n').lower() != 'y':
                    continue
            print("Goodbye.")
            break

        else:
            print("  Unknown option. Type a letter from the menu above.")


if __name__ == '__main__':
    main()
