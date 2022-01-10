from __future__ import annotations

import struct
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO
from typing import Any, ContextManager, Dict, List, cast, get_origin, overload


class DecodingBuffer(object):
    """
    A buffer that allows automatic decoding of AMQP wire protocol objects.
    """

    def __init__(self, payload_data: bytes):
        """
        :param payload_data: The payload itself to decode.
        """

        self._data = payload_data
        self._offset = 0

        # copied when doing a read bit
        self._last_bit_data = 0
        self._last_bit_offset = 0

    @property
    def has_data(self) -> bool:
        """
        Returns True if there is still data to read.
        """

        return self._offset < len(self._data)

    def _unpack(self, fmt):
        fmt = "!" + fmt
        size = struct.calcsize(fmt)
        items = struct.unpack_from(fmt, self._data, self._offset)
        self._offset += size

        # wipe bit data
        self._last_bit_data = 0
        self._last_bit_offset = 0

        return items

    def _read_size(self, size: int) -> bytes:
        data = self._data[self._offset : self._offset + size]
        self._offset += size

        self._last_bit_data = 0
        self._last_bit_offset = 0

        return data

    def read_octet_signed(self) -> int:
        """
        Reads a single signed octet from the stream.
        """

        return self._unpack("b")[0]

    def read_octet(self) -> int:
        """
        Reads a single octet from the stream.
        """

        return self._unpack("B")[0]

    def read_short_signed(self) -> int:
        """
        Reads a single signed short from the stream.
        """

        return self._unpack("h")[0]

    def read_short(self) -> int:
        """
        Reads a single short (2-byte) from the stream.
        """

        return self._unpack("H")[0]

    def read_long_signed(self) -> int:
        """
        Reads a single signed long (4-byte) from the stream.
        """

        return self._unpack("i")[0]

    def read_long(self) -> int:
        """
        Reads a single long (4-byte) from the stream.
        """

        return self._unpack("I")[0]

    def read_longlong_signed(self) -> int:
        """
        Reads a single signed long long (8-byte) from the stream.
        """

        return self._unpack("l")[0]

    def read_longlong(self) -> int:
        """
        Reads a single long-long (8-byte) from the stream.
        """

        return self._unpack("L")[0]

    def read_short_string(self) -> str:
        """
        Reads a single short string from the stream.
        """

        size = self.read_octet()
        return self._read_size(size).decode(encoding="utf-8")

    def read_long_string(self) -> bytes:
        """
        Reads a single long string from the stream.
        """

        size = self.read_long()
        return self._read_size(size)

    def read_field_value(self) -> Any:
        """
        Reads a type-prefixed field value.
        """

        buf = self
        type = chr(self.read_octet())

        if type == "t":
            item = buf.read_octet() == 1
        elif type == "b":
            item = buf.read_octet_signed()
        elif type == "B":
            item = buf.read_octet()
        elif type == "U":
            item = buf.read_short_signed()
        elif type == "u":
            item = buf.read_short()
        elif type == "I":
            item = buf.read_long_signed()
        elif type == "i":
            item = buf.read_long()
        elif type == "L":
            item = buf.read_longlong_signed()
        elif type == "l":
            # item = buf.read_longlong()  # see errata
            item = buf.read_longlong_signed()
        elif type == "f" or type == "d":
            item = buf._unpack(type)
        elif type == "D":
            raise ValueError("Fuck you")
        elif type == "s":
            # item = buf.read_short_string()  # see errata
            item = buf.read_short_signed()
        elif type == "S" or type == "x":
            item = buf.read_long_string()
        elif type == "A":
            item = self.read_array()
        elif type == "T":
            item = self.read_longlong()
        elif type == "F":
            item = self.read_table()
        elif type == "V":
            item = None
        else:
            raise ValueError(f"Unknown type code {type}")

        return item

    def read_array(self) -> List[Any]:
        """
        Reads an array of values.
        """

        item_count = self.read_long()
        return [self.read_field_value() for _ in range(0, item_count)]

    def read_table(self) -> Dict[str, Any]:
        """
        Reads a table from the stream.
        """

        table = self.read_long_string()
        buf = DecodingBuffer(table)

        result = {}

        while buf.has_data:
            key = buf.read_short_string()
            item = buf.read_field_value()
            result[key] = item

        return result

    def read_bit(self) -> bool:
        """
        Reads a single bit from the stream, as a boolean.
        """

        if 0 < self._last_bit_offset < 8:
            bit = ((self._last_bit_data) & (1 << self._last_bit_offset)) == 1
            self._last_bit_offset += 1
            return bit

        # copy next bit off
        data = self._unpack("B")[0]
        self._last_bit_data = data
        bit = (data & 1) == 1
        self._last_bit_offset += 1
        return bit


class EncodingBuffer(object):
    """
    A buffer that writes data in AMQP format.
    """

    def __init__(self):
        self._table_mode = False
        self._data = BytesIO()

        self._last_bit_data = 0
        self._last_bit_offset = 0

    def _write(self, data: bytes):
        if self._last_bit_offset > 0:
            self._data.write(self._last_bit_data.to_bytes(length=1, byteorder="big"))
            self._last_bit_data = 0
            self._last_bit_offset = 0

        self._data.write(data)

    def get_data(self) -> bytes:
        """
        Gets the raw data in this buffer. This preserves the previous cursor, so data can be
        written even after calling this method.
        """

        pos = self._data.tell()
        self._data.seek(0)
        data = self._data.read()
        self._data.seek(pos)

        return data

    def write_long_string(self, data: bytes):
        """
        Writes a long string to the buffer.
        """

        if self._table_mode:
            self._write(b"S")

        size = struct.pack(">I", len(data))
        self._write(size)
        self._write(data)

    def _write_string(self, data: str):
        encoded = data.encode("utf-8")
        size = struct.pack(">B", len(encoded))
        self._write(size)
        self._write(encoded)

    def write_short_string(self, data: str):
        """
        Writes a short string to the buffer.
        """

        if self._table_mode:
            raise ValueError("short strings have no table type")

        self._write_string(data)

    def write_octet(self, value: int):
        """
        Writes a single byte to the buffer.
        """

        if self._table_mode:
            self._write(b"B")

        self._write(struct.pack(">B", value))

    def write_octet_signed(self, value: int):
        """
        Writes a single signed byte to the buffer.
        """

        if self._table_mode:
            self._write(b"B")

        self._write(struct.pack(">b", value))

    def write_short(self, value: int):
        """
        Writes a single short to the buffer.
        """

        if self._table_mode:
            self._write(b"u")

        self._write(struct.pack(">H", value))

    def write_short_signed(self, value: int):
        """
        Writes a single signed short to the buffer.
        """

        if self._table_mode:
            self._write(b"U")

        self._write(struct.pack(">h", value))

    def write_long(self, value: int):
        """
        Writes a single long to the buffer.
        """

        if self._table_mode:
            self._write(b"i")

        self._write(struct.pack(">I", value))

    def write_long_signed(self, value: int):
        """
        Writes a single signed long to the buffer.
        """

        if self._table_mode:
            self._write(b"I")

        self._write(struct.pack(">i", value))

    def write_longlong(self, value: int):
        """
        Writes a single long long to the buffer.
        """

        if self._table_mode:
            raise ValueError("unsigned longlongs have no table type")

        self._write(struct.pack(">L", value))

    def write_longlong_signed(self, value: int):
        """
        Writes a single signed long long to the buffer.
        """

        if self._table_mode:
            self._write(b"l")

        self._write(struct.pack(">l", value))

    @overload
    def write_timestamp(self, value: datetime):
        ...

    @overload
    def write_timestamp(self, value: int):
        ...

    def write_timestamp(self, value):
        """
        Writes a timestamp to the buffer.
        """

        if self._table_mode:
            self._write(b"T")

        if isinstance(value, datetime):
            value = int(value.timestamp())

        # time_t, long
        self._write(struct.pack(">L", value))

    def write_float(self, value: float):
        """
        Writes a single precision float to the buffer.
        """

        if self._table_mode:
            self._write(b"f")

        self._write(struct.pack(">f", value))

    def write_double(self, value: float):
        """
        Writes a double precision float to the buffer.
        """

        if self._table_mode:
            self._write(b"d")

        self._write(struct.pack(">d", value))

    def write_bit(self, value: bool):
        """
        Writes a single bit to the buffer.
        """

        if self._last_bit_offset >= 8:
            self._write(b"")  # forces a bit write
        else:
            self._last_bit_data = (self._last_bit_data << 1) & value
            self._last_bit_offset += 1

    def force_write_bits(self):
        """
        Forces a trailing bit write if needed.
        """

        self._write(b"")

    @contextmanager
    def _table_cm(self):
        buf = TableWriter()
        yield buf

        if self._table_mode:
            self._write(b"F")

        self.write_long_string(buf.get_data())

    def start_writing_table(self) -> ContextManager[TableWriter]:
        """
        Writes a table to the stream. This is a context manager that allows writing key/values to
        a table.
        """

        return self._table_cm()

    def write_table(self, table: Dict[str, Any]):
        """
        Writes a complete table.
        """

        with self.start_writing_table() as writer:
            for key, value in table.items():
                writer.automatically_write_value(key, value)


class TableWriter(EncodingBuffer):
    """
    A subclass of the encoding buffer that has extensions for table methods.
    """

    def __init__(self):
        super().__init__()

        self._table_mode = True

    def write_key(self, key: str):
        """
        Writes a standalone key to the buffer. This ignores table mode.
        """

        self._write_string(key)

    def automatically_write_value(self, key: str, value: Any):
        """
        Automatically writes a value. This will detect the type of the value and write the
        appropriate type. This is primarily useful for writing table data.

        :param key: The key for this value.
        :param value: The value to write.
        """

        self.write_key(key)

        if isinstance(value, int):
            bl = value.bit_length()
            if bl <= 32:
                self.write_long(value)
            else:
                self.write_longlong(value)

        elif isinstance(value, float):
            self.write_double(value)

        elif isinstance(value, str):
            self.write_long_string(value.encode("utf-8"))

        elif isinstance(value, bytes):
            self.write_long_string(value)

        elif isinstance(value, datetime):
            self.write_timestamp(value)

        elif isinstance(value, dict):
            self.write_table(value)

        else:
            raise ValueError(f"Unknown item: {value} ({type(value)})")