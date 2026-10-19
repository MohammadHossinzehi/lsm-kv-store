"""Tiny CLI: python -m lsmkv DIR put K V | get K | del K | scan [START END] | stats"""
import sys
from .db import DB


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 1
    db = DB(argv[1])
    cmd, args = argv[2], argv[3:]
    if cmd == "put":
        db.put(args[0], args[1])
    elif cmd == "get":
        v = db.get(args[0])
        print("(nil)" if v is None else v.decode())
    elif cmd == "del":
        db.delete(args[0])
    elif cmd == "scan":
        for k, v in db.scan(*(args[:2])):
            print(k.decode(), v.decode())
    elif cmd == "stats":
        print(db.stats())
    db.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
