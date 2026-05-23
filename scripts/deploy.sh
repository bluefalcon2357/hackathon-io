#!/usr/bin/env bash
# One-shot Google Cloud deployment.
#
# Prereqs (one-time):
#   - gcloud CLI installed and authenticated: `gcloud auth login`
#   - A GCP project with billing enabled
#   - `gcloud config set project <PROJECT>` (or export GOOGLE_CLOUD_PROJECT)
#
# Usage:
#   ./scripts/deploy.sh                 # uses $GOOGLE_CLOUD_PROJECT
#   GOOGLE_CLOUD_PROJECT=foo ./scripts/deploy.sh
#
# What it does:
#   1. Enables required APIs (idempotent)
#   2. Creates Pub/Sub topics + Firestore database (idempotent)
#   3. Builds the container with Cloud Build
#   4. Deploys to Cloud Run with sensible defaults
#   5. Prints the service URL
set -euo pipefail

PROJECT="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
if [ -z "${PROJECT}" ] || [ "${PROJECT}" = "(unset)" ]; then
  echo "error: set GOOGLE_CLOUD_PROJECT or run \`gcloud config set project <id>\`" >&2
  exit 1
fi

REGION="${VERTEX_LOCATION:-us-central1}"
SERVICE="${CLOUD_RUN_SERVICE:-hackathon-io}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${SERVICE}/${SERVICE}:latest}"
REPO="${SERVICE}"

echo "==> project=${PROJECT} region=${REGION} service=${SERVICE}"

echo "==> enabling APIs"
gcloud services enable \
  aiplatform.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  pubsub.googleapis.com \
  firestore.googleapis.com \
  --project "${PROJECT}" --quiet

echo "==> ensuring Artifact Registry repo"
gcloud artifacts repositories describe "${REPO}" \
  --location "${REGION}" --project "${PROJECT}" >/dev/null 2>&1 || \
gcloud artifacts repositories create "${REPO}" \
  --repository-format=docker \
  --location="${REGION}" \
  --project "${PROJECT}" --quiet

echo "==> ensuring Pub/Sub topics"
for topic in fact-check-chunks fact-check-verdicts; do
  gcloud pubsub topics describe "${topic}" --project "${PROJECT}" >/dev/null 2>&1 || \
  gcloud pubsub topics create "${topic}" --project "${PROJECT}" --quiet
done

echo "==> ensuring Firestore (Native mode)"
gcloud firestore databases describe --database='(default)' \
  --project "${PROJECT}" >/dev/null 2>&1 || \
gcloud firestore databases create \
  --location="${REGION}" \
  --type=firestore-native \
  --project "${PROJECT}" --quiet

echo "==> building image with Cloud Build"
gcloud builds submit --tag "${IMAGE}" --project "${PROJECT}" .

echo "==> deploying to Cloud Run"
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --project "${PROJECT}" \
  --platform managed \
  --allow-unauthenticated \
  --concurrency 80 \
  --min-instances 1 \
  --max-instances 5 \
  --cpu 2 \
  --memory 2Gi \
  --timeout 3600 \
  --no-cpu-throttling \
  --update-env-vars "LOCAL_MODE=false,GEMINI_MODEL=gemini-flash-latest,GOOGLE_GENAI_USE_VERTEXAI=true,GOOGLE_CLOUD_PROJECT=${PROJECT},VERTEX_LOCATION=${REGION}"
# `--update-env-vars` is additive: it only touches the listed keys and
# preserves anything set out-of-band (e.g. YT_DLP_COOKIES + the
# /secrets/cookies.txt mount applied via `gcloud run services update
# --update-secrets`). Switching back to `--set-env-vars` would wipe them.

URL=$(gcloud run services describe "${SERVICE}" \
  --region "${REGION}" --project "${PROJECT}" --format='value(status.url)')

echo
echo "==> deployed: ${URL}"
echo "    health:  ${URL}/healthz"
