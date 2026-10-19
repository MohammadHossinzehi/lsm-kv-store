"""Bloom filter with double hashing, serialisable to bytes."""
import hashlib
import math
import struct


class Bloom:
    def __init__(self, n_items, fp_rate=0.01, bits=None, k=None, data=None):
        if bits is None:
            n = max(1, n_items)
            bits = max(8, int(-n * math.log(fp_rate) / (math.log(2) ** 2)))
            k = max(1, round(bits / n * math.log(2)))
        self.bits = bits
        self.k = k
        self.data = bytearray(data) if data is not None else bytearray((bits + 7) // 8)

    def _positions(self, key):
        d = hashlib.blake2b(key, digest_size=16).digest()
        h1, h2 = struct.unpack("<QQ", d)
        h2 |= 1
        return [(h1 + i * h2) % self.bits for i in range(self.k)]

    def add(self, key):
        for p in self._positions(key):
            self.data[p >> 3] |= 1 << (p & 7)

    def might_contain(self, key):
        return all(self.data[p >> 3] & (1 << (p & 7)) for p in self._positions(key))

    def to_bytes(self):
        return struct.pack("<II", self.bits, self.k) + bytes(self.data)

    @classmethod
    def from_bytes(cls, raw):
        bits, k = struct.unpack("<II", raw[:8])
        return cls(0, bits=bits, k=k, data=raw[8:])
