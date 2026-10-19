"""The LSM tree: memtable + WAL, immutable SSTables, size-tiered compaction."""
import heapq
import json
import os

from .bloom import Bloom
from .sstable import SSTable, write_sstable, TOMB
from .wal import WAL, PUT, DEL


class DB:
    def __init__(self, path, memtable_limit=4096, compact_threshold=4, sync=False):
        self.path = path
        self.memtable_limit = memtable_limit
        self.compact_threshold = compact_threshold
        os.makedirs(path, exist_ok=True)
        self.mem = {}  # key -> (value, flag)
        self.mem_bytes = 0
        self.tables = []  # oldest first
        self._seq = 0
        self._load_manifest()
        wal_path = os.path.join(path, "wal.log")
        for k, v, flag in WAL.replay(wal_path):
            self._mem_put(k, v, flag)
        self.wal = WAL(wal_path, sync=sync)
        # Re-write the wal cleanly so a torn tail does not poison later appends.
        self._rewrite_wal()

    # ---- manifest ----
    def _manifest_path(self):
        return os.path.join(self.path, "MANIFEST")

    def _load_manifest(self):
        mp = self._manifest_path()
        names = []
        if os.path.exists(mp):
            with open(mp) as f:
                m = json.load(f)
            names, self._seq = m["tables"], m["seq"]
        self.tables = [SSTable(os.path.join(self.path, n)) for n in names]
        self.names = names
        live = set(names) | {"MANIFEST", "wal.log"}
        for fn in os.listdir(self.path):  # orphans from an interrupted compaction
            if fn.endswith(".sst") and fn not in live:
                os.remove(os.path.join(self.path, fn))

    def _save_manifest(self):
        tmp = self._manifest_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"tables": self.names, "seq": self._seq}, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._manifest_path())  # atomic commit point

    def _rewrite_wal(self):
        self.wal.close()
        p = os.path.join(self.path, "wal.log")
        tmp = p + ".tmp"
        w = WAL(tmp)
        for k, (v, flag) in self.mem.items():
            w.append(k, v, flag)
        w.close()
        os.replace(tmp, p)
        self.wal = WAL(p, sync=self.wal.sync)

    # ---- memtable ----
    def _mem_put(self, k, v, flag):
        old = self.mem.get(k)
        if old is not None:
            self.mem_bytes -= len(k) + len(old[0])
        self.mem[k] = (v, flag)
        self.mem_bytes += len(k) + len(v)

    @staticmethod
    def _b(x):
        return x.encode() if isinstance(x, str) else bytes(x)

    def put(self, key, value):
        k, v = self._b(key), self._b(value)
        self.wal.append(k, v, PUT)
        self._mem_put(k, v, PUT)
        self._maybe_flush()

    def delete(self, key):
        k = self._b(key)
        self.wal.append(k, b"", DEL)
        self._mem_put(k, b"", DEL)
        self._maybe_flush()

    def get(self, key):
        k = self._b(key)
        if k in self.mem:
            v, flag = self.mem[k]
            return None if flag == DEL else v
        for t in reversed(self.tables):  # newest wins
            r = t.get(k)
            if r is not None:
                return None if r[1] == TOMB else r[0]
        return None

    def scan(self, start=None, end=None):
        """Yield live (key, value) in key order for start <= key < end."""
        s = self._b(start) if start is not None else None
        e = self._b(end) if end is not None else None
        sources = [self._mem_iter(s)] + [t.items(s) for t in reversed(self.tables)]
        tagged = [((k, i, v, f) for k, v, f in src) for i, src in enumerate(sources)]
        last = None
        for k, i, v, f in heapq.merge(*tagged, key=lambda x: (x[0], x[1])):
            if k == last:
                continue  # an older version of a key we already resolved
            last = k
            if e is not None and k >= e:
                return
            if s is not None and k < s:
                continue
            if f != TOMB:
                yield k, v

    def _mem_iter(self, s):
        for k in sorted(self.mem):
            if s is None or k >= s:
                v, f = self.mem[k]
                yield k, v, f

    # ---- flush & compaction ----
    def _maybe_flush(self):
        if self.mem_bytes >= self.memtable_limit:
            self.flush()

    def _new_name(self):
        self._seq += 1
        return "%06d.sst" % self._seq

    def flush(self):
        if not self.mem:
            return
        name = self._new_name()
        items = [(k, v, f) for k, (v, f) in sorted(self.mem.items())]
        bloom = Bloom(len(items))
        write_sstable(os.path.join(self.path, name), items, bloom)
        self.names.append(name)
        self._save_manifest()
        self.tables.append(SSTable(os.path.join(self.path, name)))
        self.mem, self.mem_bytes = {}, 0
        self._rewrite_wal()
        if len(self.tables) >= self.compact_threshold:
            self.compact()

    def compact(self):
        """Merge every table into one. Tombstones are dropped because the output
        is the bottom level (nothing older can be resurrected)."""
        if len(self.tables) < 2:
            return
        srcs = [((k, i, v, f) for k, v, f in t.items()) for i, t in enumerate(reversed(self.tables))]
        merged, last = [], None
        for k, i, v, f in heapq.merge(*srcs, key=lambda x: (x[0], x[1])):
            if k == last:
                continue
            last = k
            if f != TOMB:
                merged.append((k, v, PUT))
        old = list(self.names)
        name = self._new_name()
        write_sstable(os.path.join(self.path, name), merged, Bloom(len(merged)))
        self.names = [name]
        self._save_manifest()  # commit point: crash before this keeps old tables
        self.tables = [SSTable(os.path.join(self.path, name))]
        for n in old:
            os.remove(os.path.join(self.path, n))

    def stats(self):
        return {
            "memtable_keys": len(self.mem),
            "sstables": len(self.tables),
            "bloom_checks": sum(t.bloom_checks for t in self.tables),
            "bloom_skips": sum(t.bloom_skips for t in self.tables),
        }

    def close(self):
        self.wal.close()
