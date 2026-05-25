from __future__ import annotations

import asyncio
import logging
import traceback
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from backend.config import get_settings
from backend.ingestion import youtube
from backend.runtime import runner
from backend.runtime.session_manager import manager
from backend.schemas import SessionRequest, SessionResponse
from backend.transport import sse, ws

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("factcheck")

app = FastAPI(title="Live Reality Fact-Check Overlay")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


class NoCacheStaticFiles(StaticFiles):
    """Serve static assets with ``Cache-Control: no-cache`` so a browser never
    keeps running a stale ``app.js`` after a redeploy. ``no-cache`` forces
    revalidation on every load; unchanged files still come back as a cheap 304
    via ETag/Last-Modified, so this costs a round-trip, not a re-download.
    """

    async def get_response(self, path: str, scope):  # type: ignore[override]
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-cache"
        return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    log.error("unhandled %s on %s: %s\n%s",
              type(exc).__name__, request.url.path, exc, traceback.format_exc())
    return JSONResponse(
        status_code=500,
        content={"error": type(exc).__name__, "detail": str(exc)},
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.post("/api/sessions", response_model=SessionResponse)
async def create_session(req: SessionRequest) -> SessionResponse:
    """Returns the session_id immediately. yt-dlp classification + ingestion
    happen in the background task; failures surface to the client as `error`
    events on the SSE stream (not as a 500 on this call)."""
    if not req.youtube_url:
        raise HTTPException(status_code=400, detail="youtube_url is required")

    kind = req.kind or youtube.guess_kind_from_url(req.youtube_url)
    session = manager.create(req.youtube_url, kind, mode=req.mode)
    session.task = asyncio.create_task(runner.run(session))

    return SessionResponse(session_id=session.session_id, kind=kind)


@app.get("/api/sessions/{session_id}/stream")
async def stream(session_id: str) -> StreamingResponse:
    session = manager.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session not found")
    return StreamingResponse(
        sse.event_stream(session),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.websocket("/api/sessions/{session_id}/ws")
async def ws_endpoint(websocket: WebSocket, session_id: str) -> None:
    session = manager.get(session_id)
    if session is None:
        await websocket.close(code=4404)
        return
    await ws.serve(websocket, session)


@app.delete("/api/sessions/{session_id}")
async def end_session(session_id: str) -> dict[str, str]:
    session = manager.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="session not found")
    if session.task and not session.task.done():
        session.task.cancel()
    manager.remove(session_id)
    return {"status": "ended"}


if FRONTEND_DIR.exists():
    app.mount("/static", NoCacheStaticFiles(directory=FRONTEND_DIR), name="static")

    @app.get("/")
    async def root_index() -> FileResponse:
        return FileResponse(
            FRONTEND_DIR / "index.html", headers={"Cache-Control": "no-cache"}
        )


@app.on_event("startup")
async def on_startup() -> None:
    s = get_settings()
    log.info(
        "starting | model=%s local_mode=%s stub_llm=%s chunk_seconds=%d",
        s.gemini_model, s.local_mode, s.stub_llm, s.chunk_seconds,
    )
