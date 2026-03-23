import os
import subprocess

from fastapi import APIRouter

router = APIRouter()


def _get_git_hash() -> str:
    if git_hash := os.environ.get("GIT_HASH"):
        return git_hash
    try:
        return (
            subprocess.check_output(
                ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


_git_hash = _get_git_hash()


@router.get("/version")
async def version():
    return {"version": _git_hash}
