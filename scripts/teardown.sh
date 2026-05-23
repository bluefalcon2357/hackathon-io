#!/usr/bin/env bash
# Tear down the deployed Cloud Run service + auxiliary resources.
# Firestore data is NOT deleted (databases can't be deleted via gcloud); the
# Firestore database itself is left in place.
set -euo pipefail

PROJECT="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${VERTEX_LOCATION:-us-central1}"
SERVICE="${CLOUD_RUN_SERVICE:-hackathon-io}"

if [ -z "${PROJECT}" ] || [ "${PROJECT}" = "(unset)" ]; then
  echo "error: set GOOGLE_CLOUD_PROJECT" >&2
  exit 1
fi

echo "==> deleting Cloud Run service ${SERVICE}"
gcloud run services delete "${SERVICE}" \
  --region "${REGION}" --project "${PROJECT}" --quiet || true

echo "==> deleting Pub/Sub topics"
for topic in fact-check-chunks fact-check-verdicts; do
  gcloud pubsub topics delete "${topic}" --project "${PROJECT}" --quiet || true
done

echo "==> note: Firestore database and Artifact Registry repo left in place."
echo "    Remove manually if desired."
