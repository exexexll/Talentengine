#!/usr/bin/env python3
"""Print a bcrypt hash for use in FIGWORK_ACCOUNTS_JSON (password_hash field)."""

import getpass
import sys

try:
    import bcrypt
except ImportError as e:
    print("Install bcrypt: pip install bcrypt", file=sys.stderr)
    raise SystemExit(1) from e


def main() -> None:
    if len(sys.argv) >= 2:
        raw = sys.argv[1].encode("utf-8")
    else:
        p1 = getpass.getpass("Password: ").encode("utf-8")
        p2 = getpass.getpass("Again:    ").encode("utf-8")
        if p1 != p2:
            print("Passwords do not match.", file=sys.stderr)
            raise SystemExit(1)
        raw = p1
    h = bcrypt.hashpw(raw, bcrypt.gensalt(rounds=12))
    print(h.decode("ascii"))


if __name__ == "__main__":
    main()
