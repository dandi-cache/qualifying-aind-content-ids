import argparse
import itertools
import json
import pathlib
import traceback

import dandi.dandiapi
import h5py
import hdmf_zarr
import numpy
import nwbinspector
import pynwb
import remfile
import spikeinterface.extractors


def _nwb_file_qualifies(s3_url: str) -> bool:
    """
    Determine whether an NWB file qualifies.

    Only ElectricalSeries in the acquisition submodule with a sampling rate above 10kHz are
    assessed; lower-rate series (e.g. LFP) are ignored. The pipeline processes every such series,
    so a single non-processable one would make it fail: each must have a duration longer than 120
    seconds, have no NaN channel locations, and survive the pipeline's split-then-aggregate step.
    The file qualifies when at least one acquisition ElectricalSeries exceeds 10kHz and every
    series that does passes those checks.
    """
    electrical_series_paths = spikeinterface.extractors.NwbRecordingExtractor.fetch_available_electrical_series_paths(
        file_path=s3_url, stream_mode="remfile"
    )
    acquisition_series_paths = [
        electrical_series_path
        for electrical_series_path in electrical_series_paths
        if electrical_series_path.startswith("acquisition/")
    ]
    if not acquisition_series_paths:
        return False

    any_above_rate_threshold = False
    for electrical_series_path in acquisition_series_paths:
        extractor = spikeinterface.extractors.NwbRecordingExtractor(
            file_path=s3_url, stream_mode="remfile", electrical_series_path=electrical_series_path
        )

        # Only series above the rate threshold are spike-sorted by the pipeline, so the remaining
        # (more expensive) assessments only apply to them; filter on the cheap sampling-rate
        # metadata first and skip everything else.
        if extractor.get_sampling_frequency() <= 10_000:
            continue
        any_above_rate_threshold = True

        if extractor.get_total_duration() <= 120:
            return False

        # A NaN channel location breaks the pipeline's downstream distance/geometry computations
        # just as surely as the aggregation failure below, so exclude it the same way.
        if numpy.isnan(extractor.get_channel_locations()).any():
            return False

        # Mimic the pipeline as closely as possible. job_dispatch (aind-ephys-job-dispatch) splits
        # a recording with `recording.split_by("group")` when it has more than one channel group,
        # and nwb_ecephys (aind-ecephys-nwb) then recombines those per-group recordings with
        # `spikeinterface.aggregate_channels`. That recombination raises "Locations are not
        # unique!" when the per-group "location" properties collide -- exactly the failure we are
        # trying to predict (channelsaggregationrecording.py). Reproduce the same split-then-
        # aggregate here so that any session that would crash nwb_ecephys is excluded. We catch
        # every exception because any aggregation failure (not just the location assertion) would
        # equally break the pipeline.
        if len(set(extractor.get_channel_groups())) > 1:
            recording_groups = list(extractor.split_by(property="group").values())
            try:
                spikeinterface.aggregate_channels(recording_groups)
            except Exception:
                return False

    return any_above_rate_threshold


def _load_ids(file_path: pathlib.Path) -> set:
    """Load a set of IDs from a JSONL file, returning an empty set if the file does not exist."""
    if not file_path.exists():
        return set()

    with file_path.open(mode="r") as file_stream:
        return {json.loads(line) for line in file_stream if line.strip()}


def _is_nwb_file(path: str) -> bool:
    """Whether a path points to an NWB asset, which ends in `.nwb` (HDF5) or `.nwb.zarr` (Zarr)."""
    suffixes = pathlib.Path(path).suffixes
    return suffixes[-2:] == [".nwb", ".zarr"] or suffixes[-1:] == [".nwb"]


# The `derivatives` dataset is a persistent DataLad dataset: these logs accumulate across every
# run forever (entries are never otherwise removed), and GitHub hard-rejects any single blob over
# 100 MB. Keep each log file comfortably under that limit by dropping the oldest entries once it
# grows past this cap.
_MAX_LOG_FILE_SIZE_BYTES = 80_000_000


def _log_error(log_file_path: pathlib.Path, message: str) -> None:
    """Append a single error report to the given error log, separated by a blank line.

    If the file has grown past `_MAX_LOG_FILE_SIZE_BYTES`, the oldest entries are dropped first so
    the file never approaches GitHub's 100 MB per-file limit.
    """
    with log_file_path.open(mode="a") as file_stream:
        file_stream.write(f"{message}\n\n")

    if log_file_path.stat().st_size <= _MAX_LOG_FILE_SIZE_BYTES:
        return

    with log_file_path.open(mode="rb") as file_stream:
        file_stream.seek(-_MAX_LOG_FILE_SIZE_BYTES, 2)
        tail = file_stream.read()

    # Realign to the start of the next whole entry so no partial entry is kept.
    next_entry_offset = tail.find(b"\n\n")
    if next_entry_offset != -1:
        tail = tail[next_entry_offset + 2 :]

    with log_file_path.open(mode="wb") as file_stream:
        file_stream.write(tail)


def _run(base_directory: pathlib.Path, limit: int | None) -> None:
    submodule_dir = base_directory / "sourcedata" / "content-id-to-usage-dandiset-path" / "derivatives"
    submodule_file_path = submodule_dir / "content_id_to_usage_dandiset_path.jsonl"
    content_id_to_dandiset_path = {}
    with submodule_file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                content_id_to_dandiset_path.update(json.loads(line))

    logs_dir = base_directory / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    dandi_api_errors_log_file_path = logs_dir / "dandi_api_errors.txt"
    file_open_errors_log_file_path = logs_dir / "file_open_errors.txt"
    nwb_inspector_errors_log_file_path = logs_dir / "nwb_inspector_errors.txt"
    spikeinterface_errors_log_file_path = logs_dir / "spikeinterface_errors.txt"
    unexpected_errors_log_file_path = logs_dir / "unexpected_errors.txt"

    # Each processing stage routes its failures to a dedicated log; anything unmapped (e.g. a
    # failure before the first labelled stage) falls back to the catch-all `unexpected_errors.txt`.
    stage_to_log_file_path = {
        "retrieving asset information from the DANDI API": dandi_api_errors_log_file_path,
        "opening the NWB file": file_open_errors_log_file_path,
        "running the NWB Inspector": nwb_inspector_errors_log_file_path,
        "validating SpikeInterface metadata": spikeinterface_errors_log_file_path,
    }

    error_ids_file_path = base_directory / "derivatives" / "error_ids.jsonl"
    error_ids = _load_ids(error_ids_file_path)

    processed_ids_file_path = base_directory / "derivatives" / "processed_ids.jsonl"
    processed_ids = _load_ids(processed_ids_file_path)

    # Only NWB assets can qualify, so skip everything else (raw imaging, video, ephys bundles, ...)
    # up front based on the mapped path, before spending any DANDI API or file-open work on them.
    content_ids_to_process = {
        content_id
        for content_id in content_id_to_dandiset_path.keys() - error_ids - processed_ids
        if _is_nwb_file(next(iter(content_id_to_dandiset_path[content_id].values())))
    }

    qualifying_aind_content_ids_file_path = base_directory / "derivatives" / "qualifying_aind_content_ids.jsonl"
    qualifying_aind_content_ids = _load_ids(qualifying_aind_content_ids_file_path)

    client = dandi.dandiapi.DandiAPIClient()  # Run tokenless to ensure only public dandisets are accessed
    dandi_config = nwbinspector.load_config("dandi")
    for content_id in itertools.islice(content_ids_to_process, limit):
        # Defaults so the catch-all handler can report context even if the very first step fails.
        dandiset_id = first_path = s3_url = None
        stage = "loading the source path"
        try:
            dandiset_id, first_path = next(iter(content_id_to_dandiset_path[content_id].items()))

            stage = "retrieving asset information from the DANDI API"
            dandiset = client.get_dandiset(dandiset_id=dandiset_id)
            asset = dandiset.get_asset_by_path(path=first_path)
            s3_url = asset.get_content_url(follow_redirects=1, strip_query=True)

            stage = "opening the NWB file"
            if ".zarr" in pathlib.Path(first_path).suffixes:
                io = hdmf_zarr.NWBZarrIO(s3_url, mode="r")
                nwbfile = io.read()
            else:
                rem_file = remfile.File(url=s3_url)
                h5py_file = h5py.File(name=rem_file, mode="r")
                io = pynwb.NWBHDF5IO(file=h5py_file)
                nwbfile = io.read()

            stage = "running the NWB Inspector"
            inspector_messages = list(
                nwbinspector.inspect_nwbfile_object(
                    nwbfile_object=nwbfile,
                    config=dandi_config,
                    importance_threshold=nwbinspector.Importance.CRITICAL,
                )
            )
            if inspector_messages:
                joined_messages = "\n\n".join(str(inspector_message) for inspector_message in inspector_messages)
                _log_error(
                    log_file_path=nwb_inspector_errors_log_file_path,
                    message=(
                        f"NWB Inspector found CRITICAL issues for `{content_id=}` "
                        f"(dandiset ID {dandiset_id}, path {first_path}, URL {s3_url})!\n\n"
                        f"{joined_messages}"
                    ),
                )
                error_ids.add(content_id)
                continue

            stage = "validating SpikeInterface metadata"
            qualifies = _nwb_file_qualifies(s3_url=s3_url)
        except Exception as exception:
            _log_error(
                log_file_path=stage_to_log_file_path.get(stage, unexpected_errors_log_file_path),
                message=(
                    f"Error while {stage} for `{content_id=}` "
                    f"(dandiset ID {dandiset_id}, path {first_path}, URL {s3_url})!\n\n"
                    f"{type(exception)}:{str(exception)}\n\n"
                    f"{traceback.format_exc()}"
                ),
            )
            error_ids.add(content_id)
            continue

        if qualifies:
            qualifying_aind_content_ids.add(content_id)
        processed_ids.add(content_id)

    with error_ids_file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps(id_)}\n" for id_ in sorted(error_ids))
    with processed_ids_file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps(id_)}\n" for id_ in sorted(processed_ids))
    with qualifying_aind_content_ids_file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps(id_)}\n" for id_ in sorted(qualifying_aind_content_ids))


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Process qualifying AIND content IDs.")
    parser.add_argument(
        "--limit",
        type=int,
        default=2_000,
        help="The number of sessions (content IDs) to process in this run.",
    )
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `sourcedata`, `derivatives`, and `logs` directories. "
            "Primarily used in tests. Defaults to the repository root."
        ),
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, limit=args.limit)
