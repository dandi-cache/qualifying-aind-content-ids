# syntax=docker/dockerfile:1

# The pinned runtime environment for this cache.
#
# Everything shared comes from the base image: Debian, git-annex, the virtual environment, the
# `dandi_cache_utils` library that `code/update.py` imports, and the vendored `update_pipeline.sh`
# that CI extracts and runs on the runner.
#
# The `:nwb` tag rather than `:latest`, because the assessment streams remote NWB files: that tag
# carries remfile, h5py and pynwb alongside the DANDI API client. SpikeInterface is this cache's
# own, and is the only thing installed here.
#
# The loose dependencies in envs/pyproject.toml are resolved fresh at build time, and the resulting
# image (by digest) is the lock, so there is no lockfile in the repository to hold in sync.
#
# The image holds only the environment, not this cache's code. The code is mounted at run time, so
# a single image serves any revision of it.
FROM ghcr.io/dandi-cache/dandi-cache-utils:nwb

# The source label associates the image on the GitHub Container Registry with this repository.
LABEL org.opencontainers.image.source="https://github.com/dandi-cache/qualifying-aind-content-ids"
LABEL org.opencontainers.image.description="The pinned runtime environment for the DANDI Cache qualifying-aind-content-ids update pipeline."

# pip cannot read [project.dependencies] without building the project, so install the (virtual)
# project from a throwaway build directory.
COPY envs/pyproject.toml /tmp/build/pyproject.toml
RUN pip install /tmp/build \
    && rm -rf /tmp/build

CMD ["python", "--version"]
