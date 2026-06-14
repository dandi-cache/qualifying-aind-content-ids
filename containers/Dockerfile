# syntax=docker/dockerfile:1

# Container bundling the pinned Python environment together with the processing code so
# that the update pipeline (which operates on the `derivatives` branch) can execute from a
# single, persistent, fully provenanced artifact instead of reinstalling git-annex and the
# environment on every run.
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

# uv drives a reproducible, lockfile-pinned Python environment (including Python 3.13
# itself, which uv downloads). The exact dependency set is pinned by envs/uv.lock, so the
# uv binary version below does not affect the resolved environment.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_INSTALL_DIR=/opt/uv/python \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

WORKDIR /opt/qualifying-aind-content-ids

# Resolve the locked environment first so this expensive layer is cached independently of
# changes to the processing code.
COPY envs/ ./envs/
RUN uv sync --project envs --locked --no-dev

# The processing code itself.
COPY code/ ./code/

# Make the locked environment the default interpreter.
ENV PATH="/opt/venv/bin:${PATH}"

CMD ["python", "--version"]
