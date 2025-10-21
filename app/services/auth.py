import os
import hmac
import hashlib
import base64
from typing import Optional

from ..db import get_db


def _bcrypt_available() -> bool:
    try:
        import bcrypt  # type: ignore
        return True
    except Exception:
        return False


def hash_password(password: str) -> str:
    if _bcrypt_available():
        import bcrypt  # type: ignore
        salt = bcrypt.gensalt(rounds=12)
        return "bcrypt$" + bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")
    # Fallback PBKDF2-HMAC-SHA256
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return "pbkdf2$" + base64.b64encode(salt + dk).decode("ascii")


def verify_password(password: str, password_hash: str) -> bool:
    if password_hash.startswith("bcrypt$"):
        if not _bcrypt_available():
            return False
        import bcrypt  # type: ignore
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.split("$", 1)[1].encode("utf-8"))
    if password_hash.startswith("pbkdf2$"):
        raw = base64.b64decode(password_hash.split("$", 1)[1].encode("ascii"))
        salt, dk = raw[:16], raw[16:]
        new_dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
        return hmac.compare_digest(dk, new_dk)
    return False


def authenticate(username: str, password: str) -> Optional[str]:
    db = get_db()
    # Obtener hash
    from ..db.sqlite_db import SQLiteDB  # type: ignore
    if isinstance(db, SQLiteDB):
        ph = db.get_user_hash(username)
    else:
        # Interfaz opcional si se implementa MySQLDB en el futuro
        try:
            ph = db.get_user_hash(username)  # type: ignore[attr-defined]
        except Exception:
            ph = None
    if not ph:
        return None
    return username if verify_password(password, ph) else None

