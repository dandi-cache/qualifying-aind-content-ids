#!/usr/bin/env bash
#
# Drive the update while keeping generated results off the code branch.
#
#   - `main`        holds only the code (this checkout).
#   - `derivatives` is a persistent DataLad dataset on its own branch. The processing
#                   command is recorded there via `datalad run`, so every update carries
#                   full provenance (the command, the input subdataset commit, and the
#                   output diff) and the history is retained.
#   - `min`         is the lightweight, force-recreated publication artifact consumed by
#                   downstream users (see README.md).
#
# The input `content-id-to-nwb-files` lives inside the `derivatives` dataset as a
# subdataset, so it is captured as a provenance input rather than tracked by the code.
#
# Required environment variables:
#   REPO_URL    Authenticated https remote for this repository (clone/push).
#   WORKSPACE   Path to the `main` checkout that holds the code (this repository).
# Optional:
#   LIMIT       Number of sessions (content IDs) to process this run (default: 2000).
#   GITHUB_SHA  Recorded in the provenance message to link results to the code commit.
#
set -euo pipefail

: "${REPO_URL:?REPO_URL must be set}"
: "${WORKSPACE:?WORKSPACE must be set}"
LIMIT="${LIMIT:-2000}"

DS="${RUNNER_TEMP:-/tmp}/derivatives-dataset"
SEED="${RUNNER_TEMP:-/tmp}/derivatives-seed"
MINDIR="${RUNNER_TEMP:-/tmp}/min-publish"
SUBDATASET_PATH="sourcedata/content-id-to-nwb-files"
SUBDATASET_URL="https://github.com/dandi-cache/content-id-to-nwb-files.git"
BOT_NAME="github-actions[bot]"
BOT_EMAIL="github-actions[bot]@users.noreply.github.com"

git config --global user.name "${BOT_NAME}"
git config --global user.email "${BOT_EMAIL}"

# DataLad is declared in the `envs` project rather than installed as a standalone tool,
# so invoke it through the same environment used for the Python processing.
datalad() {
  uv run --project "${WORKSPACE}/envs" datalad "$@"
}

retry() {
  # retry <max-attempts> <command...> with exponential backoff
  local max="$1"
  shift
  local attempt=1 delay=2
  until "$@"; do
    if [ "${attempt}" -ge "${max}" ]; then
      return 1
    fi
    echo "Attempt ${attempt} failed; retrying in ${delay}s..." >&2
    sleep "${delay}"
    attempt=$((attempt + 1))
    delay=$((delay * 2))
  done
}

configure_repo() {
  git -C "$1" config user.name "${BOT_NAME}"
  git -C "$1" config user.email "${BOT_EMAIL}"
}

rm -rf "${DS}" "${SEED}" "${MINDIR}"

if git ls-remote --heads "${REPO_URL}" derivatives | grep -q "refs/heads/derivatives"; then
  echo "Reusing the existing 'derivatives' dataset branch."
  retry 4 git clone --branch derivatives --single-branch "${REPO_URL}" "${DS}"
  configure_repo "${DS}"
else
  echo "Bootstrapping a new 'derivatives' DataLad dataset."
  datalad create --no-annex "${DS}"
  configure_repo "${DS}"
  mkdir -p "${DS}/derivatives" "${DS}/logs"

  # Seed prior state so already-processed IDs are not reprocessed. The 'min' branch
  # currently mirrors the previous full results (YAML state + logs).
  if retry 4 git clone --branch min --single-branch "${REPO_URL}" "${SEED}"; then
    cp -a "${SEED}"/derivatives/*.yaml "${DS}/derivatives/" 2>/dev/null || true
    cp -a "${SEED}/logs/." "${DS}/logs/" 2>/dev/null || true
  else
    echo "No 'min' branch to seed from; starting from an empty state." >&2
  fi

  retry 4 datalad clone -d "${DS}" "${SUBDATASET_URL}" "${DS}/${SUBDATASET_PATH}"
  datalad save -d "${DS}" -m "Initialize derivatives dataset"
fi

# Advance the input subdataset to its latest commit and record the pointer.
retry 4 git -C "${DS}" submodule update --init --remote "${SUBDATASET_PATH}"
datalad save -d "${DS}" -m "Update input subdataset to latest" -- "${SUBDATASET_PATH}" || true

# Record the processing as provenance on the 'derivatives' branch. '--explicit' keeps
# datalad from clearing the outputs beforehand, which is required because the YAML files
# are both prior state (input) and output of this run.
datalad run -d "${DS}" \
  --input "${SUBDATASET_PATH}" \
  --output derivatives \
  --output logs \
  --explicit \
  -m "Update qualifying AIND content IDs (code @ ${GITHUB_SHA:-unknown})" \
  "uv run --project '${WORKSPACE}/envs' python '${WORKSPACE}/code/update.py' --base-directory '${DS}' --limit '${LIMIT}'"

retry 4 git -C "${DS}" push origin HEAD:derivatives

# Publish the consumer-facing minified artifact to the force-recreated 'min' branch.
uv run --project "${WORKSPACE}/envs" python "${WORKSPACE}/code/minify.py" --base-directory "${DS}"

mkdir -p "${MINDIR}/derivatives"
cp "${DS}"/derivatives/*.min.json.gz "${MINDIR}/derivatives/"
git -C "${MINDIR}" init -q
configure_repo "${MINDIR}"
git -C "${MINDIR}" checkout -q -b min
git -C "${MINDIR}" add derivatives
git -C "${MINDIR}" commit -q -m "Publish minified qualifying AIND content IDs"
retry 4 git -C "${MINDIR}" push -f "${REPO_URL}" min:min
