import os
import random
import tempfile
import unittest

from lsmkv import DB
from lsmkv.bloom import Bloom
from lsmkv.wal import WAL


class BloomTests(unittest.TestCase):
    def test_no_false_negatives_and_low_fp(self):
        b = Bloom(1000, 0.01)
        for i in range(1000):
            b.add(b"k%d" % i)
        self.assertTrue(all(b.might_contain(b"k%d" % i) for i in range(1000)))
        fp = sum(b.might_contain(b"x%d" % i) for i in range(5000))
        self.assertLess(fp / 5000, 0.03)

    def test_roundtrip(self):
        b = Bloom(10)
        b.add(b"a")
        c = Bloom.from_bytes(b.to_bytes())
        self.assertTrue(c.might_contain(b"a"))


class DBTests(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()

    def test_basic_and_overwrite_delete(self):
        db = DB(self.d)
        db.put("a", "1"); db.put("a", "2"); db.put("b", "3")
        self.assertEqual(db.get("a"), b"2")
        db.delete("b")
        self.assertIsNone(db.get("b"))
        self.assertIsNone(db.get("zzz"))

    def test_flush_and_compaction_preserve_semantics(self):
        db = DB(self.d, memtable_limit=200, compact_threshold=3)
        db.put("k", "old")
        for i in range(100):
            db.put("f%03d" % i, "v%d" % i)
        db.put("k", "new"); db.delete("f010")
        db.flush()
        self.assertEqual(db.get("k"), b"new")
        self.assertIsNone(db.get("f010"))
        self.assertEqual(db.get("f099"), b"v99")
        db.compact()
        self.assertEqual(db.stats()["sstables"], 1)
        self.assertIsNone(db.get("f010"))

    def test_tombstone_shadows_older_table(self):
        db = DB(self.d, compact_threshold=99)
        db.put("x", "1"); db.flush()
        db.delete("x"); db.flush()
        self.assertIsNone(db.get("x"))
        self.assertEqual(list(db.scan()), [])

    def test_scan_ranges(self):
        db = DB(self.d, memtable_limit=100, compact_threshold=99)
        for i in range(50):
            db.put("%02d" % i, "v")
        db.delete("20")
        keys = [k.decode() for k, _ in db.scan("15", "25")]
        self.assertEqual(keys, ["15", "16", "17", "18", "19", "21", "22", "23", "24"])

    def test_recovery_from_wal(self):
        db = DB(self.d)
        db.put("a", "1"); db.delete("a"); db.put("b", "2")
        db.close()  # no flush: simulates a crash
        db2 = DB(self.d)
        self.assertIsNone(db2.get("a"))
        self.assertEqual(db2.get("b"), b"2")

    def test_torn_wal_tail_is_ignored(self):
        db = DB(self.d)
        db.put("a", "1"); db.put("b", "2")
        db.close()
        p = os.path.join(self.d, "wal.log")
        with open(p, "ab") as f:
            f.write(b"\x01\x02\x03garbage")
        db2 = DB(self.d)
        self.assertEqual(db2.get("a"), b"1")
        db2.put("c", "3"); db2.close()
        self.assertEqual(DB(self.d).get("c"), b"3")

    def test_reopen_after_flush(self):
        db = DB(self.d, memtable_limit=100)
        for i in range(100):
            db.put("k%d" % i, "v%d" % i)
        db.close()
        db2 = DB(self.d)
        for i in range(100):
            self.assertEqual(db2.get("k%d" % i), b"v%d" % i)

    def test_orphan_sstable_cleaned(self):
        db = DB(self.d); db.put("a", "1"); db.flush(); db.close()
        orphan = os.path.join(self.d, "999999.sst")
        open(orphan, "wb").write(b"junk")
        DB(self.d)
        self.assertFalse(os.path.exists(orphan))

    def test_bloom_skips_missing_keys(self):
        db = DB(self.d, compact_threshold=99)
        for i in range(200):
            db.put("k%d" % i, "v")
        db.flush()
        for i in range(500):
            db.get("missing%d" % i)
        s = db.stats()
        self.assertGreater(s["bloom_skips"], 450)

    def test_randomised_against_dict_oracle(self):
        rng = random.Random(7)
        db = DB(self.d, memtable_limit=300, compact_threshold=3)
        model = {}
        for step in range(3000):
            k = "k%03d" % rng.randrange(150)
            r = rng.random()
            if r < 0.6:
                v = "v%d" % step
                db.put(k, v); model[k.encode()] = v.encode()
            elif r < 0.8:
                db.delete(k); model.pop(k.encode(), None)
            elif r < 0.85:
                db.close(); db = DB(self.d, memtable_limit=300, compact_threshold=3)
            else:
                self.assertEqual(db.get(k), model.get(k.encode()))
        self.assertEqual(dict(db.scan()), model)
        self.assertEqual(list(db.scan()), sorted(model.items()))


if __name__ == "__main__":
    unittest.main()
