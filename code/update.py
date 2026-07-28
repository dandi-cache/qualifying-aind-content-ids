import argparse
import itertools
import json
import pathlib
import traceback

import dandi.dandiapi
import numpy
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


def _load_dict(file_path: pathlib.Path) -> dict:
    """Load a dict from a JSONL file of single-key objects, returning an empty dict if missing."""
    result = {}
    if not file_path.exists():
        return result

    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                result.update(json.loads(line))
    return result


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
    # The base universe of content IDs to consider: only content IDs already confirmed to
    # qualify for the LFP pipeline are candidates for the (stricter) AIND ephys pipeline.
    lfp_submodule_dir = base_directory / "sourcedata" / "qualifying-lfp-content-ids" / "derivatives"
    lfp_content_ids_file_path = lfp_submodule_dir / "qualifying_lfp_content_ids.jsonl"
    qualifying_lfp_content_ids = _load_ids(lfp_content_ids_file_path)

    # Used only to resolve a content ID to the dandiset/path needed to fetch it from the DANDI API.
    submodule_dir = base_directory / "sourcedata" / "content-id-to-usage-dandiset-path" / "derivatives"
    submodule_file_path = submodule_dir / "content_id_to_usage_dandiset_path.jsonl"
    content_id_to_dandiset_path = _load_dict(submodule_file_path)

    # The `content-id-to-valid-nwb-file` cache already confirms each NWB file opens successfully
    # and passes the NWB Inspector at the CRITICAL threshold, so this pipeline can trust that
    # result instead of repeating the file-open/inspection work itself.
    valid_nwb_submodule_dir = base_directory / "sourcedata" / "content-id-to-valid-nwb-file" / "derivatives"
    valid_nwb_file_path = valid_nwb_submodule_dir / "content_id_to_valid_nwb_file.jsonl"
    content_id_to_valid_nwb_file = _load_dict(valid_nwb_file_path)

    logs_dir = base_directory / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    dandi_api_errors_log_file_path = logs_dir / "dandi_api_errors.txt"
    spikeinterface_errors_log_file_path = logs_dir / "spikeinterface_errors.txt"
    unexpected_errors_log_file_path = logs_dir / "unexpected_errors.txt"

    # Each processing stage routes its failures to a dedicated log; anything unmapped (e.g. a
    # failure before the first labelled stage) falls back to the catch-all `unexpected_errors.txt`.
    stage_to_log_file_path = {
        "retrieving asset information from the DANDI API": dandi_api_errors_log_file_path,
        "validating SpikeInterface metadata": spikeinterface_errors_log_file_path,
    }

    # `content_id_to_qualifies` is both the record of every content ID already assessed (whether
    # it qualifies, doesn't, or errored -- all recorded as `False` other than a confirmed
    # qualification) and the published output. `error_ids` separately tracks which of those
    # `False` entries were due to an error rather than a confirmed non-qualification, so the
    # reason behind a given `False` can still be recovered.
    qualifying_aind_content_ids_file_path = base_directory / "derivatives" / "qualifying_aind_content_ids.jsonl"
    content_id_to_qualifies = _load_dict(qualifying_aind_content_ids_file_path)

    error_ids_file_path = base_directory / "derivatives" / "error_ids.jsonl"
    error_ids = _load_ids(error_ids_file_path)

    # Content IDs the upstream cache has already flagged as not a valid NWB file are disqualified
    # without spending any further work on them; they are not errors, just non-qualifying.
    for content_id in qualifying_lfp_content_ids - content_id_to_qualifies.keys():
        if content_id_to_valid_nwb_file.get(content_id) is False:
            content_id_to_qualifies[content_id] = False

    # Only NWB assets that the upstream cache has confirmed are valid can qualify. Assets not yet
    # present in that cache are left for a future run once upstream has assessed them.
    content_ids_to_process = {
        content_id
        for content_id in qualifying_lfp_content_ids - content_id_to_qualifies.keys()
        if content_id in content_id_to_dandiset_path
        and content_id_to_valid_nwb_file.get(content_id) is True
        and _is_nwb_file(next(iter(content_id_to_dandiset_path[content_id].values())))
    }

    client = dandi.dandiapi.DandiAPIClient()  # Run tokenless to ensure only public dandisets are accessed
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
            content_id_to_qualifies[content_id] = False
            error_ids.add(content_id)
            continue

        content_id_to_qualifies[content_id] = qualifies

    with qualifying_aind_content_ids_file_path.open(mode="w") as file_stream:
        file_stream.writelines(
            f"{json.dumps({content_id: content_id_to_qualifies[content_id]})}\n"
            for content_id in sorted(content_id_to_qualifies)
        )
    with error_ids_file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps(id_)}\n" for id_ in sorted(error_ids))


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
