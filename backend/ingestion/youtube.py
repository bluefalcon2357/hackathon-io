import asyncio
import json
import logging
import os
import shutil
import tempfile

from backend.schemas import StreamKind

log = logging.getLogger(__name__)

_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_cookies_cache: str | None = None
_cookies_cache_resolved = False


def _writable_cookies_path() -> str | None:
    """Cloud Run mounts secrets read-only, but yt-dlp writes refreshed cookies
    back after YouTube rotates them on each request. Copy the secret-mounted
    file into tmpfs on first use so yt-dlp's save_cookies() succeeds."""
    global _cookies_cache, _cookies_cache_resolved
    if _cookies_cache_resolved:
        return _cookies_cache

    src = os.environ.get("YT_DLP_COOKIES")
    if not src or not os.path.exists(src):
        _cookies_cache_resolved = True
        return None
    if os.access(src, os.W_OK):
        _cookies_cache = src
    else:
        dst = os.path.join(tempfile.gettempdir(), "yt-cookies.txt")
        shutil.copyfile(src, dst)
        os.chmod(dst, 0o600)
        log.info("copied read-only cookies %s -> %s (yt-dlp needs write access)", src, dst)
        _cookies_cache = dst
    _cookies_cache_resolved = True
    return _cookies_cache


def _ydl_cli_args(url: str) -> list[str]:
    """Build yt-dlp CLI args. We invoke yt-dlp as a subprocess rather than
    in-process for two reasons:
      1. yt-dlp does TTY/stdin operations on import that fail with [Errno 22]
         in non-TTY environments (Cloud Run) when run inside asyncio.to_thread.
      2. Process isolation means a single bad URL can't corrupt the parent
         interpreter's state.
    """
    args = [
        "yt-dlp",
        "--quiet",
        "--no-warnings",
        "--skip-download",
        "--no-playlist",
        "--dump-single-json",
        "--geo-bypass",
        "--no-check-certificate",
        "--retries", "3",
        "--user-agent", os.environ.get("YT_DLP_USER_AGENT", _DEFAULT_UA),
    ]
    cookies_file = _writable_cookies_path()
    if cookies_file:
        args.extend(["--cookies", cookies_file])
        log.info("yt-dlp using cookies file %s", cookies_file)
    args.append(url)
    return args


async def probe(url: str) -> dict:
    """Resolve YouTube metadata via the yt-dlp CLI."""
    proc = await asyncio.create_subprocess_exec(
        *_ydl_cli_args(url),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.DEVNULL,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode("utf-8", errors="replace").strip()
        if "Sign in to confirm" in err or "bot" in err.lower():
            raise IngestionError(
                "YouTube is blocking this server's IP (bot check). "
                "Mount a cookies.txt file and set YT_DLP_COOKIES to its path."
            )
        raise IngestionError(f"yt-dlp could not resolve URL: {err[:500]}")
    try:
        return json.loads(stdout.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        raise IngestionError(f"yt-dlp returned non-JSON output: {exc}") from exc


async def classify(url: str) -> tuple[StreamKind, dict]:
    """Detect whether the URL is live or recorded, and return (kind, metadata)."""
    info = await probe(url)
    is_live = bool(info.get("is_live")) or info.get("live_status") in {
        "is_live",
        "is_upcoming",
    }
    kind = StreamKind.LIVE if is_live else StreamKind.RECORDED
    log.info("classified %s as %s", url, kind.value)
    return kind, info


def guess_kind_from_url(url: str) -> StreamKind:
    """Cheap URL-shape heuristic so the HTTP handler can return a session_id
    without awaiting yt-dlp. The runner re-classifies authoritatively."""
    if "/live/" in url or "live=1" in url:
        return StreamKind.LIVE
    return StreamKind.RECORDED


class IngestionError(RuntimeError):
    """Raised when yt-dlp can't resolve a YouTube URL."""


def manifest_url(info: dict) -> str | None:
    """Pull the best audio HLS/HTTP URL out of yt-dlp metadata."""
    if "url" in info and info["url"]:
        return info["url"]
    formats = info.get("formats") or []
    audio_only = [f for f in formats if f.get("vcodec") == "none" and f.get("url")]
    if audio_only:
        audio_only.sort(key=lambda f: f.get("abr") or 0, reverse=True)
        return audio_only[0]["url"]
    if formats:
        return formats[-1].get("url")
    return None
