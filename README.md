# lsm-kv-store

A log-structured merge-tree key-value store written from scratch in pure Python. Zero dependencies.

LSM trees power LevelDB, RocksDB, Cassandra and many other write-heavy databases. This project implements the core ideas in about 400 readable lines so the mechanics are easy to follow and test.

## What it implements

* **Memtable + write-ahead log**: every write is appended to a CRC-protected WAL before it lands in the in-memory table, so a crash loses nothing that was acknowledged.
* **SSTables**: when the memtable fills it is flushed to an immutable sorted file with a sparse index and a Bloom filter.
* **Bloom filters**: double-hashed, sized from the target false-positive rate, so reads for missing keys skip most tables without touching their data.
* **Tombstones**: deletes are recorded as markers that shadow older versions, and are dropped only when compaction reaches the bottom level.
* **Compaction**: a k-way heap merge of all tables, newest version wins, committed atomically through a manifest swap.
* **Range scans**: a merged iterator over the memtable and all tables with correct shadowing.
* **Crash recovery**: WAL replay stops at a torn or corrupt tail record; orphan SSTables from an interrupted compaction are removed on open.

## Usage

```python
from lsmkv import DB

db = DB("./data", memtable_limit=4096, compact_threshold=4)
db.put("user:1", "ada")
db.put("user:2", "grace")
db.delete("user:2")
print(db.get("user:1"))                  # b'ada'
print(list(db.scan("user:", "user:~")))  # [(b'user:1', b'ada')]
print(db.stats())
db.close()
```

CLI:

```
python -m lsmkv ./data put name ada
python -m lsmkv ./data get name
python -m lsmkv ./data scan
python -m lsmkv ./data stats
```

## Run the tests

```
python -m unittest tests.test_lsm -v
```

## Design notes

* **Commit point**: the `MANIFEST` file lists live tables and is replaced with `os.replace`, which is atomic. A crash before the swap keeps the old tables; a crash after it leaves only deletable leftovers.
* **Sparse index**: one index entry per 16 records keeps memory small; a lookup is a binary search plus a short linear scan.
* **Newest wins**: in merges each source is tagged with its age, and the heap orders by `(key, age)` so the first record seen for a key is the newest.
* **Testing**: unit tests cover the Bloom filter's false-positive rate, torn WAL tails, orphan cleanup and tombstone shadowing. A randomised test runs 3000 puts, deletes and reopens against a plain `dict` oracle and compares both point reads and full scans.
* **Limits**: single process, single writer, no multi-level compaction and no block cache. Those are the natural next steps.

## Layout

```
lsmkv/bloom.py     Bloom filter
lsmkv/wal.py       write-ahead log
lsmkv/sstable.py   SSTable writer and reader
lsmkv/db.py        memtable, flush, compaction, scans
lsmkv/__main__.py  CLI
tests/test_lsm.py  tests
```
