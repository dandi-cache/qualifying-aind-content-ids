# syntax=docker/dockerfile:1

# The container is the project's pinned, reproducible runtime environment and the official
# way to run the pipeline. The loose dependencies declared in envs/pyproject.toml are
# resolved fresh at build time, and the resulting image (by digest) is the lock -- there is
# no lockfile in the repository to hold in sync. uv is intentionally not used inside the
# image; it stays an outside-the-container convenience for running locally while debugging
# (see the README).
#
# The image holds only the environment, not the code: the code is supplied at run time
# (checked out or mounted), so a single image serves any revision of the code.
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

# Debian's system interpreter is externally managed (PEP 668), so install into a dedicated
# virtual environment rather than forcing pip with --break-system-packages; this also keeps
# our dependencies cleanly separated from the apt-managed Python packages.
RUN python3 -m venv "${VIRTUAL_ENV}"
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Resolve the loose dependencies from pyproject.toml into the pinned environment. pip cannot
# read [project.dependencies] without building the project, so install the (virtual) project
# from a throwaway build directory. The image (by digest) is the authoritative lock; the
# exact resolved versions can be read back at any time with `pip freeze`.
COPY envs/pyproject.toml /tmp/build/pyproject.toml
RUN pip install /tmp/build \
    && rm -rf /tmp/build

CMD ["python", "--version"]
