"""Write-ahead log. Record: crc32 | klen | vlen | flag | key | value.
A torn or corrupt tail record ends replay (crash safety)."""
import os
import struct
import zlib

HDR = struct.Struct("<IIIB")  # crc, klen, vlen, flag
PUT, DEL = 0, 1


class WAL:
    def __init__(self, path, sync=False):
        self.path = path
        self.sync = sync
        self.f = open(path, "ab")

    def append(self, key, value, flag):
        body = struct.pack("<IIB", len(key), len(value), flag) + key + value
        self.f.write(struct.pack("<I", zlib.crc32(body)) + body)
        self.f.flush()
        if self.sync:
            os.fsync(self.f.fileno())

    def close(self):
        self.f.close()

    def remove(self):
        self.f.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    @staticmethod
    def replay(path):
        out = []
        if not os.path.exists(path):
            return out
        with open(path, "rb") as f:
            buf = f.read()
        pos = 0
        while pos + HDR.size <= len(buf):
            crc, kl, vl, flag = HDR.unpack_from(buf, pos)
            end = pos + HDR.size + kl + vl
            if end > len(buf):
                break
            body = buf[pos + 4:end]
            if zlib.crc32(body) != crc:
                break
            ks = pos + HDR.size
            out.append((buf[ks:ks + kl], buf[ks + kl:end], flag))
            pos = end
        return out
