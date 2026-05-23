# Live Reality Fact-Check Overlay

Point a YouTube URL — recorded or livestreamed — at this app and watch claims get checked in real time. Floating overlay surfaces green / yellow / red verdicts on every factual claim within seconds of being spoken.

Built on **Google ADK** with **Gemini Flash** (`gemini-flash-latest`), running on Vertex AI + Cloud Run + Pub/Sub + Firestore.

## Architecture

```
YouTube URL ──► yt-dlp + ffmpeg ──► 5s audio chunks
                                          │
                                          ▼
                              RootOrchestrator (ADK)
                                          │
                ┌─────────────────────────┼─────────────────────────┐
                ▼                         ▼                         ▼
       TranscriberAgent          ClaimExtractorAgent           ContextAgent
       (Flash audio-in)        (structured JSON output)       (session.state)
                                          │
                                per extracted claim
                                          │
                        ┌─────────────────┴─────────────────┐
                        ▼                                   ▼
                  SearchAgent                       TrustedSourceAgent
              (Flash + google_search)       (Flash + allowlist FunctionTool)
                        ▼                                   ▼
                        └────────────► VerdictAgent ◄───────┘
                                              │
                                              ▼
                                  SSE ──► Frontend overlay
                                          (synced to player.getCurrentTime())
```

## Quick start

```bash
# 1. Install
pip install -e ".[dev]"

# 2. Configure
cp .env.example .env
# Edit .env: set GOOGLE_CLOUD_PROJECT, run `gcloud auth application-default login`

# 3. Run
make dev

# 4. Open http://localhost:8080 and paste a YouTube URL
```

Set `STUB_LLM=true` in `.env` to bypass Gemini calls during offline development.

## Layout

```
backend/
  main.py              FastAPI app + SSE + static mount
  ingestion/           yt-dlp + ffmpeg chunkers (recorded + live HLS)
  agents/              ADK agent factories
  runtime/             Session manager, runner, dedupe, Firestore cache
  transport/           SSE, WebSocket, Pub/Sub shim
  tools/               Google Search wrapper + trusted-source FunctionTool
frontend/
  index.html           YouTube IFrame + overlay
  app.js               Player API + EventSource + RAF scheduler
infra/                 Cloud Run + Pub/Sub + Firestore config
scripts/               Demo helpers
tests/                 Pytest
```

## Demo

```bash
make demo-recorded   # posts a known short clip
make demo-live       # posts a known livestream URL
```

## Deploy to Google Cloud

One-shot deploy:

```bash
export GOOGLE_CLOUD_PROJECT=your-project
gcloud auth login

make deploy
# or: ./scripts/deploy.sh
```

The script enables the required APIs (Vertex AI, Cloud Run, Cloud Build, Pub/Sub,
Firestore, Artifact Registry), creates a Pub/Sub topic pair, provisions a Firestore
Native database, builds the container with Cloud Build, and deploys to Cloud Run
with `--min-instances=1 --no-cpu-throttling --concurrency=80` so SSE sessions stay
warm. It prints the service URL at the end. Use `make teardown` to remove the
Cloud Run service and Pub/Sub topics.

### Auto-deploy on push to `main`

`cloudbuild.yaml` is the build recipe; you also need a Cloud Build *trigger*
to fire it on push. One-time setup:

1. Connect the repo to Cloud Build (manual — OAuth can't be scripted):
   https://console.cloud.google.com/cloud-build/triggers/connect — pick
   **GitHub (Cloud Build GitHub App)**, authorize, select
   `bluefalcon2357/hackathon-io`. Also install the
   [Cloud Build GitHub App](https://github.com/apps/google-cloud-build) on
   the repo.
2. Create the trigger:
   ```bash
   make setup-trigger
   ```
   Idempotent — safe to re-run. Sets up a trigger named `hackathon-io-main`
   that fires on every push to `main` and runs `cloudbuild.yaml`.

After that, merging a PR auto-deploys. View runs at
https://console.cloud.google.com/cloud-build/builds — fire manually with
`gcloud builds triggers run hackathon-io-main --branch=main`.

### Working around YouTube's bot wall

Cloud Run egress IPs are frequently flagged by YouTube. Symptom: every session
ends immediately with an error pill saying "YouTube is blocking this server's
IP (bot check)". Fix: mount a real browser's cookies and tell `yt-dlp` to use
them.

```bash
# 1. Export cookies (Netscape format) from a logged-in browser session.
#    Easiest: install a "Get cookies.txt" browser extension and save the
#    YouTube cookies.

# 2. Upload as a Secret Manager secret.
gcloud secrets create yt-cookies --replication-policy=automatic
gcloud secrets versions add yt-cookies --data-file=cookies.txt

# 3. Re-deploy with the secret mounted as a file + YT_DLP_COOKIES pointing at it.
gcloud run services update hackathon-io --region $VERTEX_LOCATION \
  --update-secrets=/secrets/yt-cookies/cookies.txt=yt-cookies:latest \
  --update-env-vars=YT_DLP_COOKIES=/secrets/yt-cookies/cookies.txt
```

Refresh the cookie file when it expires.
