#!/usr/bin/env bash
# Create a Cloud Build trigger that auto-deploys on push to main.
#
# Prereq (one-time, can't be scripted): connect the GitHub repo to Cloud Build.
#   https://console.cloud.google.com/cloud-build/triggers/connect?project=$GOOGLE_CLOUD_PROJECT
#   Choose GitHub (Cloud Build GitHub App) → authorize → pick
#   bluefalcon2357/hackathon-io.
#
# Usage:
#   ./scripts/setup-trigger.sh
#
# Idempotent: skips creation if a trigger with the same name already exists.
set -euo pipefail

PROJECT="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
if [ -z "${PROJECT}" ] || [ "${PROJECT}" = "(unset)" ]; then
  echo "error: set GOOGLE_CLOUD_PROJECT or run \`gcloud config set project <id>\`" >&2
  exit 1
fi

REPO_OWNER="${REPO_OWNER:-bluefalcon2357}"
REPO_NAME="${REPO_NAME:-hackathon-io}"
BRANCH="${BRANCH:-^main$}"
TRIGGER_NAME="${TRIGGER_NAME:-hackathon-io-main}"

echo "==> project=${PROJECT} repo=${REPO_OWNER}/${REPO_NAME} branch=${BRANCH}"

echo "==> ensuring Cloud Build API is enabled"
gcloud services enable cloudbuild.googleapis.com --project "${PROJECT}" --quiet

if gcloud builds triggers describe "${TRIGGER_NAME}" \
   --project "${PROJECT}" >/dev/null 2>&1; then
  echo "==> trigger '${TRIGGER_NAME}' already exists; nothing to do"
  echo "    view: https://console.cloud.google.com/cloud-build/triggers?project=${PROJECT}"
  exit 0
fi

echo "==> creating trigger '${TRIGGER_NAME}'"
if ! gcloud builds triggers create github \
  --name="${TRIGGER_NAME}" \
  --repo-owner="${REPO_OWNER}" \
  --repo-name="${REPO_NAME}" \
  --branch-pattern="${BRANCH}" \
  --build-config=cloudbuild.yaml \
  --project "${PROJECT}" 2>&1; then
  cat >&2 <<EOF

==> trigger creation failed.

The most common cause: this GCP project hasn't been connected to the GitHub
repo yet. Connect it once via the Cloud Build console, then re-run this script:

  https://console.cloud.google.com/cloud-build/triggers/connect?project=${PROJECT}

Pick "GitHub (Cloud Build GitHub App)", authorize, and select
${REPO_OWNER}/${REPO_NAME}. The Cloud Build GitHub App must be installed on
the repo as well: https://github.com/apps/google-cloud-build

EOF
  exit 1
fi

echo
echo "==> trigger created. it will fire on every push to ${BRANCH}."
echo "    triggers: https://console.cloud.google.com/cloud-build/triggers?project=${PROJECT}"
echo "    runs:     https://console.cloud.google.com/cloud-build/builds?project=${PROJECT}"
echo
echo "==> to fire it manually right now without pushing a commit:"
echo "    gcloud builds triggers run ${TRIGGER_NAME} --branch=main --project ${PROJECT}"
