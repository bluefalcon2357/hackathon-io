import asyncio
import logging
import os

import yt_dlp

from backend.schemas import StreamKind

log = logging.getLogger(__name__)

_DEFAULT_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _ydl_opts() -> dict:
    opts: dict = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "geo_bypass": True,
        "nocheckcertificate": True,
        "retries": 3,
        "extractor_retries": 3,
        "http_headers": {
            "User-Agent": os.environ.get("YT_DLP_USER_AGENT", _DEFAULT_UA),
            "Accept-Language": "en-US,en;q=0.9",
        },
    }
    cookies_file = os.environ.get("YT_DLP_COOKIES")
    if cookies_file and os.path.exists(cookies_file):
        opts["cookiefile"] = cookies_file
        log.info("yt-dlp using cookies file %s", cookies_file)
    return opts


def _probe_sync(url: str) -> dict:
    with yt_dlp.YoutubeDL(_ydl_opts()) as ydl:
        return ydl.extract_info(url, download=False)


async def probe(url: str) -> dict:
    """Return yt-dlp metadata for a YouTube URL. Runs sync yt-dlp in a thread."""
    return await asyncio.to_thread(_probe_sync, url)


async def classify(url: str) -> tuple[StreamKind, dict]:
    """Detect whether the URL is live or recorded, and return (kind, metadata).

    Raises a clear exception on yt-dlp errors so callers can surface them to the
    user (Cloud Run egress IPs commonly trigger YouTube's bot-protection wall;
    the user fixes this by mounting cookies via the YT_DLP_COOKIES env var)."""
    try:
        info = await probe(url)
    except yt_dlp.utils.DownloadError as exc:
        msg = str(exc)
        if "Sign in to confirm" in msg or "bot" in msg.lower():
            raise IngestionError(
                "YouTube is blocking this server's IP (bot check). "
                "Mount a cookies.txt file and set YT_DLP_COOKIES to its path."
            ) from exc
        raise IngestionError(f"yt-dlp could not resolve URL: {msg}") from exc
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
