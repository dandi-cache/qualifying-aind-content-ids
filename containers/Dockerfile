# syntax=docker/dockerfile:1

# The container IS the pinned, reproducible environment: dependencies are resolved fresh
# from the loose requirements.in at build time, and the resulting image (by digest) is the
# lock. This keeps the repository's external requirements loose -- there is no lockfile to
# hold in sync -- while every published image remains a single, persistent, fully
# provenanced artifact for the update pipeline (which operates the `derivatives` branch).
#
# NeuroDebian is used as the base layer because it ships a recent `git-annex-standalone`,
# which DataLad requires for the annex-backed `derivatives` dataset and its provenance.
FROM neurodebian:bookworm

# git-annex (for DataLad / annex provenance) plus the basics for cloning and TLS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        git-annex-standalone \
    && rm -rf /var/lib/apt/lists/*

# uv installs Python 3.13 (downloaded) and resolves the environment.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    VIRTUAL_ENV=/opt/venv

WORKDIR /opt/qualifying-aind-content-ids

# Resolve the loose requirements into a pinned environment, then record the exact resolved
# versions inside the image so the build's provenance is inspectable. The image digest is
# the authoritative lock; this file is purely for human inspection.
COPY requirements.in ./requirements.in
RUN uv venv --python 3.13 "${VIRTUAL_ENV}" \
    && uv pip install --python "${VIRTUAL_ENV}" --requirements requirements.in \
    && uv pip freeze --python "${VIRTUAL_ENV}" > /opt/requirements.lock.txt

# The processing code itself.
COPY code/ ./code/

# Make the resolved environment the default interpreter.
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

CMD ["python", "--version"]
