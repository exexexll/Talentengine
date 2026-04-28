"""Load internal users from env for Figwork deployments (DigitalOcean, etc.).

Accounts are defined in ``FIGWORK_ACCOUNTS_JSON`` as a JSON array. Each entry
supports either:

* ``password_hash`` — bcrypt hash (recommended for production). Generate with::

      python -c "import bcrypt; print(bcrypt.hashpw(b'YOUR_PASSWORD', bcrypt.gensalt()).decode())"

* ``password`` — plain text (only for first-time bootstrap behind HTTPS; rotate
  to hashes before wider exposure).

Fields:
  * ``username`` (required) — login id (case-sensitive).
  * ``display_name`` (optional) — shown in UI after login.
"""

from __future__ import annotations

import hmac
import json
import os
import re
from typing import Any

import bcrypt

_USERNAME_RE = re.compile(r"^[a-zA-Z0-9._@+-]{1,128}$")


def _parse_accounts() -> list[dict[str, Any]]:
    raw = os.getenv("FIGWORK_ACCOUNTS_JSON", "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def list_usernames() -> list[str]:
    """Usernames only (for health checks / admin tooling)."""
    return [str(r.get("username", "")).strip() for r in _parse_accounts() if r.get("username")]


def verify_credentials(username: str, password: str) -> dict[str, Any] | None:
    """Return the account row (without secrets) if login succeeds, else ``None``."""
    u_in = (username or "").strip()
    if not u_in or not password:
        return None
    if not _USERNAME_RE.match(u_in):
        return None
    for row in _parse_accounts():
        u = str(row.get("username", "")).strip()
        if u != u_in:
            continue
        ph = row.get("password_hash")
        if isinstance(ph, str) and ph.startswith("$2"):
            try:
                if bcrypt.checkpw(password.encode("utf-8"), ph.encode("utf-8")):
                    return _public_row(row)
            except ValueError:
                pass
            return None
        plain = row.get("password")
        if isinstance(plain, str) and plain:
            try:
                if hmac.compare_digest(plain.encode("utf-8"), password.encode("utf-8")):
                    return _public_row(row)
            except Exception:
                return None
            return None
        return None
    return None


def _public_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "username": str(row.get("username", "")).strip(),
        "display_name": str(row.get("display_name") or row.get("username") or "").strip(),
    }
