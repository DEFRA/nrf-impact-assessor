import os
from pathlib import Path

from fastapi import APIRouter

router = APIRouter()


def _get_git_hash() -> str:
    if git_hash := os.environ.get("GIT_HASH"):
        return git_hash
    try:
        return Path(".git-hash").read_text().strip()
    except OSError:
        return "unknown"


GIT_HASH = _get_git_hash()


@router.get("/version")
async def version():
    return {"version": GIT_HASH}
