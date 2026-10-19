"""Immutable sorted string table.

File layout:
  data blocks: repeated entries (klen u32, vlen u32, flag u8, key, value)
  index: sparse, one (klen, key, offset) per `stride` entries
  bloom bytes
  footer: index_off u64, bloom_off u64, count u64, magic u32
"""
import bisect
import struct

MAGIC = 0x4C534D31
FOOT = struct.Struct("<QQQI")
ENT = struct.Struct("<IIB")
TOMB = 1


def write_sstable(path, items, bloom, stride=16):
    """items: iterable of (key, value, flag) sorted by key, unique keys."""
    index = []
    count = 0
    with open(path, "wb") as f:
        for key, value, flag in items:
            if count % stride == 0:
                index.append((key, f.tell()))
            f.write(ENT.pack(len(key), len(value), flag) + key + value)
            bloom.add(key)
            count += 1
        index_off = f.tell()
        for key, off in index:
            f.write(struct.pack("<IQ", len(key), off) + key)
        bloom_off = f.tell()
        f.write(bloom.to_bytes())
        f.write(FOOT.pack(index_off, bloom_off, count, MAGIC))
    return count


class SSTable:
    def __init__(self, path):
        from .bloom import Bloom
        self.path = path
        with open(path, "rb") as f:
            self.raw = f.read()
        index_off, bloom_off, self.count, magic = FOOT.unpack(self.raw[-FOOT.size:])
        if magic != MAGIC:
            raise ValueError("bad sstable: " + path)
        self.data_end = index_off
        self.bloom = Bloom.from_bytes(self.raw[bloom_off:len(self.raw) - FOOT.size])
        self.index_keys, self.index_offs = [], []
        pos = index_off
        while pos < bloom_off:
            kl, off = struct.unpack_from("<IQ", self.raw, pos)
            pos += 12
            self.index_keys.append(self.raw[pos:pos + kl])
            self.index_offs.append(off)
            pos += kl
        self.min_key = self.index_keys[0] if self.index_keys else None
        self.bloom_checks = 0
        self.bloom_skips = 0

    def _scan(self, pos):
        raw, end = self.raw, self.data_end
        while pos < end:
            kl, vl, flag = ENT.unpack_from(raw, pos)
            pos += ENT.size
            yield raw[pos:pos + kl], raw[pos + kl:pos + kl + vl], flag
            pos += kl + vl

    def get(self, key):
        """Return (value, flag) or None."""
        self.bloom_checks += 1
        if not self.bloom.might_contain(key):
            self.bloom_skips += 1
            return None
        i = bisect.bisect_right(self.index_keys, key) - 1
        if i < 0:
            return None
        for k, v, flag in self._scan(self.index_offs[i]):
            if k == key:
                return v, flag
            if k > key:
                break
        return None

    def items(self, start=None):
        pos = 0
        if start is not None:
            i = max(0, bisect.bisect_right(self.index_keys, start) - 1)
            pos = self.index_offs[i] if self.index_offs else 0
        for k, v, flag in self._scan(pos):
            if start is None or k >= start:
                yield k, v, flag
