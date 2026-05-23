"""Gated multi-fetcher for trusted-source verification.

Free, no-auth APIs only. Each fetcher returns (snippet, url, domain) or
(None, None, None) on miss. They run in parallel; first non-empty result wins.

Sources:
  - Wikipedia REST API (en.wikipedia.org)
  - PubMed E-utilities (eutils.ncbi.nlm.nih.gov)
  - CDC Open Data / cdc.gov search
  - World Bank Data API (data.worldbank.org)
  - USGS Earthquake feed (earthquake.usgs.gov)

Paid/registration-gated APIs (Reuters, AP, NewsAPI) intentionally omitted.
Add them by writing another `_fetch_*` coroutine and appending to FETCHERS.
"""
from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote_plus, urlparse

import httpx

log = logging.getLogger(__name__)

# Wikipedia requires a descriptive User-Agent per their UA policy
# (https://meta.wikimedia.org/wiki/User-Agent_policy). A generic UA gets 403'd.
_UA = "factcheck-overlay/0.1 (https://github.com/bluefalcon2357/hackathon-io; hackathon@example.com)"
_HEADERS = {"User-Agent": _UA, "Accept": "application/json"}


def _domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().removeprefix("www.")
    except Exception:
        return ""


async def _fetch_wikipedia(client: httpx.AsyncClient, claim: str) -> tuple[str, str, str] | None:
    """Wikipedia REST summary lookup. Strong baseline for general claims."""
    search_url = "https://en.wikipedia.org/w/api.php"
    try:
        resp = await client.get(
            search_url,
            params={
                "action": "query",
                "format": "json",
                "list": "search",
                "srsearch": claim[:200],
                "srlimit": 1,
                "utf8": 1,
            },
            headers=_HEADERS,
        )
        data = resp.json()
        hits = (data.get("query") or {}).get("search") or []
        if not hits:
            return None
        title = hits[0]["title"]
        summary = await client.get(
            f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote_plus(title)}",
            headers=_HEADERS,
        )
        sdata = summary.json()
        extract = (sdata.get("extract") or "").strip()
        url = (sdata.get("content_urls") or {}).get("desktop", {}).get("page") or ""
        if not extract:
            return None
        return extract[:600], url, "wikipedia.org"
    except Exception as exc:
        log.debug("wikipedia fetch failed: %s", exc)
        return None


async def _fetch_pubmed(client: httpx.AsyncClient, claim: str) -> tuple[str, str, str] | None:
    """PubMed E-utilities — best when claim looks medical/scientific."""
    base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    try:
        search = await client.get(
            f"{base}/esearch.fcgi",
            params={"db": "pubmed", "term": claim[:200], "retmax": 1, "retmode": "json"},
        )
        ids = (search.json().get("esearchresult") or {}).get("idlist") or []
        if not ids:
            return None
        pmid = ids[0]
        summary = await client.get(
            f"{base}/esummary.fcgi",
            params={"db": "pubmed", "id": pmid, "retmode": "json"},
        )
        result = (summary.json().get("result") or {}).get(pmid) or {}
        title = result.get("title", "")
        if not title:
            return None
        snippet = title
        if result.get("source"):
            snippet = f"{title} — {result['source']}"
        return snippet[:600], f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/", "pubmed.ncbi.nlm.nih.gov"
    except Exception as exc:
        log.debug("pubmed fetch failed: %s", exc)
        return None


async def _fetch_worldbank(client: httpx.AsyncClient, claim: str) -> tuple[str, str, str] | None:
    """World Bank indicator search — best for economic/development claims."""
    try:
        resp = await client.get(
            "https://search.worldbank.org/api/v2/wds",
            params={"format": "json", "qterm": claim[:120], "rows": 1, "fl": "docdt,display_title,url"},
        )
        data = resp.json()
        docs = (data.get("documents") or {})
        for key, doc in docs.items():
            if key in {"facets", "total"}:
                continue
            title = doc.get("display_title") or ""
            url = doc.get("url") or ""
            if title:
                return title[:600], url, "worldbank.org"
        return None
    except Exception as exc:
        log.debug("worldbank fetch failed: %s", exc)
        return None


async def _fetch_usgs(client: httpx.AsyncClient, claim: str) -> tuple[str, str, str] | None:
    """USGS earthquake feed — narrow but authoritative for seismic claims."""
    if not any(t in claim.lower() for t in ("earthquake", "quake", "magnitude", "seismic")):
        return None
    try:
        resp = await client.get(
            "https://earthquake.usgs.gov/fdsnws/event/1/query",
            params={
                "format": "geojson",
                "limit": 1,
                "orderby": "time",
                "minmagnitude": 5,
            },
        )
        features = resp.json().get("features") or []
        if not features:
            return None
        f = features[0]
        props = f.get("properties") or {}
        snippet = f"USGS: most recent M{props.get('mag')} {props.get('place')}."
        return snippet[:600], props.get("url", ""), "earthquake.usgs.gov"
    except Exception as exc:
        log.debug("usgs fetch failed: %s", exc)
        return None


FETCHERS = (_fetch_wikipedia, _fetch_pubmed, _fetch_worldbank, _fetch_usgs)


async def fetch_trusted_snippet(
    client: httpx.AsyncClient,
    claim: str,
    allowlist: list[str],
) -> tuple[str, str | None, str | None]:
    """Race trusted fetchers. Return the first non-empty allowlisted hit."""
    tasks = [asyncio.create_task(f(client, claim)) for f in FETCHERS]
    try:
        for finished in asyncio.as_completed(tasks):
            result = await finished
            if not result:
                continue
            snippet, url, domain = result
            if not snippet:
                continue
            if allowlist and not any(domain == ad or domain.endswith("." + ad) for ad in allowlist):
                continue
            return snippet, url, domain
        return "", None, None
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
