"""
Drive the update while keeping generated results off the code branch.

  - `main`        holds only the code (this checkout).
  - `derivatives` is a persistent DataLad dataset on its own branch, checked out as a git
                  worktree of this repository. The processing command is recorded there
                  via ``datalad run``, so every update carries full provenance (the
                  command, the input subdataset commit, and the output diff) and the
                  history is retained.
  - `min`         is the lightweight, force-recreated publication artifact consumed by
                  downstream users (see README.md), assembled on a throwaway worktree.

The input `content-id-to-nwb-files` lives inside the `derivatives` dataset as a
subdataset, so it is captured as a provenance input rather than tracked by the code.

Environment variables:
  REPO_URL     Authenticated https remote for this repository (clone/push). [required]
  WORKSPACE    Path to the `main` checkout that holds the code (this repository). [required]
  LIMIT        Number of sessions (content IDs) to process this run. [default: 2000]
  GITHUB_SHA   Recorded in the provenance message to link results to the code commit.
  RUNNER_TEMP  Scratch directory for the worktrees. [default: /tmp]
"""

import os
import pathlib
import shutil
import subprocess
import sys

import datalad.api as datalad

BOT_NAME = "github-actions[bot]"
BOT_EMAIL = "github-actions[bot]@users.noreply.github.com"
SUBDATASET_PATH = "sourcedata/content-id-to-nwb-files"
SUBDATASET_URL = "https://github.com/dandi-cache/content-id-to-nwb-files.git"


def _git(repository: pathlib.Path, *arguments: str) -> None:
    subprocess.run(["git", "-C", str(repository), *arguments], check=True)


def _branch_exists(repo_url: str, branch: str) -> bool:
    result = subprocess.run(
        ["git", "ls-remote", "--heads", repo_url, branch],
        check=True,
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def main() -> None:
    repo_url = os.environ["REPO_URL"]
    workspace = pathlib.Path(os.environ["WORKSPACE"])  # the `main` code checkout (a git repo)
    limit = os.environ.get("LIMIT", "2000")
    github_sha = os.environ.get("GITHUB_SHA", "unknown")

    scratch = pathlib.Path(os.environ.get("RUNNER_TEMP", "/tmp"))
    dataset_dir = scratch / "derivatives-dataset"
    min_dir = scratch / "min-publish"

    subprocess.run(["git", "config", "--global", "user.name", BOT_NAME], check=True)
    subprocess.run(["git", "config", "--global", "user.email", BOT_EMAIL], check=True)

    # The worktrees are administered from the code checkout's repository; clear any stale ones.
    for path in (dataset_dir, min_dir):
        subprocess.run(["git", "-C", str(workspace), "worktree", "remove", "--force", str(path)], check=False)
    _git(workspace, "worktree", "prune")

    # The `derivatives` dataset as a worktree of the code checkout.
    if _branch_exists(repo_url, "derivatives"):
        print("Reusing the existing 'derivatives' dataset branch.")
        _git(workspace, "fetch", "--no-tags", repo_url, "+refs/heads/derivatives:refs/heads/derivatives")
        _git(workspace, "worktree", "add", str(dataset_dir), "derivatives")
        _git(dataset_dir, "submodule", "update", "--init", SUBDATASET_PATH)
    else:
        print("Bootstrapping a new 'derivatives' DataLad dataset.")
        # The dataset shares no history with `main`, so start it on an orphan worktree.
        _git(workspace, "worktree", "add", "--orphan", "-b", "derivatives", str(dataset_dir))
        datalad.create(path=str(dataset_dir), force=True, annex=False)
        datalad.clone(dataset=str(dataset_dir), source=SUBDATASET_URL, path=str(dataset_dir / SUBDATASET_PATH))
        datalad.save(dataset=str(dataset_dir), message="Initialize derivatives dataset")

    _git(dataset_dir, "config", "user.name", BOT_NAME)
    _git(dataset_dir, "config", "user.email", BOT_EMAIL)
    (dataset_dir / "derivatives").mkdir(exist_ok=True)
    (dataset_dir / "logs").mkdir(exist_ok=True)

    # Advance the input subdataset to its latest commit and record the pointer.
    _git(dataset_dir, "submodule", "update", "--init", "--remote", SUBDATASET_PATH)
    datalad.save(dataset=str(dataset_dir), message="Update input subdataset to latest", path=SUBDATASET_PATH)

    # Record the processing as provenance. `explicit` keeps datalad from clearing the
    # outputs beforehand, which is required because the YAML files are both prior state
    # (input) and output of this run.
    datalad.run(
        cmd=[
            sys.executable,
            str(workspace / "code" / "update.py"),
            "--base-directory",
            str(dataset_dir),
            "--limit",
            str(limit),
        ],
        dataset=str(dataset_dir),
        inputs=[SUBDATASET_PATH],
        outputs=["derivatives", "logs"],
        explicit=True,
        message=f"Update qualifying AIND content IDs (code @ {github_sha})",
    )

    _git(dataset_dir, "push", repo_url, "HEAD:derivatives")

    # Publish the consumer-facing minified artifact to the force-recreated `min` branch,
    # assembled on a throwaway orphan worktree.
    subprocess.run(
        [sys.executable, str(workspace / "code" / "minify.py"), "--base-directory", str(dataset_dir)],
        check=True,
    )
    _git(workspace, "worktree", "add", "--orphan", "-b", "min-publish", str(min_dir))
    (min_dir / "derivatives").mkdir(parents=True, exist_ok=True)
    for artifact in (dataset_dir / "derivatives").glob("*.min.json.gz"):
        shutil.copy(artifact, min_dir / "derivatives" / artifact.name)
    _git(min_dir, "config", "user.name", BOT_NAME)
    _git(min_dir, "config", "user.email", BOT_EMAIL)
    _git(min_dir, "add", "derivatives")
    _git(min_dir, "commit", "-q", "-m", "Publish minified qualifying AIND content IDs")
    _git(min_dir, "push", "-f", repo_url, "min-publish:min")

    # Tidy the worktrees and the temporary local branch used for publication.
    _git(workspace, "worktree", "remove", "--force", str(dataset_dir))
    _git(workspace, "worktree", "remove", "--force", str(min_dir))
    subprocess.run(["git", "-C", str(workspace), "branch", "-D", "min-publish"], check=False)


if __name__ == "__main__":
    main()
