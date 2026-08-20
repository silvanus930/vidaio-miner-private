"""HTTP Basic auth for the CMS -- this runs behind a public cloudflared
tunnel with no other access control, so it shouldn't be wide open.

Credentials: CMS_USERNAME/CMS_PASSWORD env vars if set, otherwise a random
password is generated once and persisted to a local file (not the repo, not
logged anywhere public) so it's stable across restarts without requiring
manual setup.
"""
from __future__ import annotations

import os
import secrets
from pathlib import Path

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

CREDENTIALS_FILE = Path("/root/vidaio-cms/.credentials")
security = HTTPBasic()


def _generate_and_store() -> tuple[str, str]:
    username = "admin"
    password = secrets.token_urlsafe(18)
    CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    CREDENTIALS_FILE.write_text(f"{username}:{password}\n")
    CREDENTIALS_FILE.chmod(0o600)
    return username, password


def get_credentials() -> tuple[str, str]:
    env_user = os.getenv("CMS_USERNAME")
    env_pass = os.getenv("CMS_PASSWORD")
    if env_user and env_pass:
        return env_user, env_pass
    if CREDENTIALS_FILE.exists():
        line = CREDENTIALS_FILE.read_text().strip()
        if ":" in line:
            user, _, pw = line.partition(":")
            if user and pw:
                return user, pw
    return _generate_and_store()


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    expected_user, expected_pass = get_credentials()
    user_ok = secrets.compare_digest(credentials.username, expected_user)
    pass_ok = secrets.compare_digest(credentials.password, expected_pass)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=401, detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
