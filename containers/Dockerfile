# syntax=docker/dockerfile:1

# The container IS the project's pinned, reproducible environment and the official way to
# run the pipeline. The loose dependencies declared in envs/pyproject.toml are resolved
# fresh at build time, and the resulting image (by digest) is the lock -- there is no
# lockfile in the repository to hold in sync. uv is intentionally not used inside the
# image; it stays an outside-the-container convenience for running locally while debugging
# (see the README).
#
# NeuroDebian trixie (Debian 13) is the base layer: it provides Python 3.13 and a recent
# git-annex-standalone, which DataLad requires for the annex-backed `derivatives` dataset
# and its provenance.
FROM neurodebian:trixie

# git-annex (for DataLad / annex provenance), Python, and the basics for cloning and TLS.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        git \
        git-annex-standalone \
        python3 \
        python3-pip \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

ENV VIRTUAL_ENV=/opt/venv \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# An isolated virtual environment keeps our dependencies off the system interpreter.
RUN python3 -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

WORKDIR /opt/qualifying-aind-content-ids

# Resolve the loose dependencies from pyproject.toml into a pinned environment. The image
# (by digest) is the authoritative lock; the exact resolved versions can be read back at any
# time with `pip freeze`.
COPY envs/pyproject.toml ./pyproject.toml
RUN pip install .

# The processing code itself.
COPY code/ ./code/

CMD ["python", "--version"]
