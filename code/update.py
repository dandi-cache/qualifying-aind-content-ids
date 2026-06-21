import argparse
import itertools
import json
import pathlib
import traceback

import dandi.dandiapi
import h5py
import hdmf_zarr
import nwbinspector
import pynwb
import remfile
import spikeinterface.extractors
import yaml


def _any_electrical_series_qualifies(s3_url: str) -> bool:
    """
    Determine whether an NWB file qualifies, which is when at least one ElectricalSeries qualifies.

    An ElectricalSeries qualifies when it is under the acquisition submodule, has a unique
    "location" property across all channels (matching the uniqueness assertion that the
    downstream `spikeinterface.aggregate_channels` step in the pipeline enforces), a rate above
    10kHz, and a duration longer than 120 seconds.
    """
    electrical_series_paths = spikeinterface.extractors.NwbRecordingExtractor.fetch_available_electrical_series_paths(
        file_path=s3_url, stream_mode="remfile"
    )
    for electrical_series_path in electrical_series_paths:
        if not electrical_series_path.startswith("acquisition/"):
            continue

        extractor = spikeinterface.extractors.NwbRecordingExtractor(
            file_path=s3_url, stream_mode="remfile", electrical_series_path=electrical_series_path
        )

        # Mirror exactly what the downstream pipeline (aind-ecephys-nwb) checks. The pipeline
        # splits the recording by probe group and recombines the groups with
        # `spikeinterface.aggregate_channels`, which asserts that the raw "location" property is
        # unique across all aggregated channels (channelsaggregationrecording.py). We must
        # reproduce that exact comparison: previously we used `get_channel_locations()`, which
        # reads positions from the probe's `contact_vector` (the global ProbeGroup layout, which
        # is unique across probes) rather than the "location" property the pipeline compares. With
        # multiple identical probes (e.g. two "vprobe" groups) the per-probe-relative "location"
        # values collide across probes, so the pipeline's aggregation crashes even though
        # `get_channel_locations()` looked unique -- which is how this session slipped through.
        #
        # `aggregate_channels` only enforces uniqueness when a "location" property is present, so
        # we gate on that here as well; if it is absent the pipeline would not hit the assertion.
        try:
            channel_locations = extractor.get_property("location")
        except Exception as exception:
            if "no channel locations" in str(exception).lower():
                continue
            raise

        if channel_locations is not None:
            unique_locations = set(tuple(loc) for loc in channel_locations)
            if len(unique_locations) != extractor.get_num_channels():
                continue

        if extractor.get_sampling_frequency() <= 10_000:
            continue

        if extractor.get_total_duration() <= 120:
            continue

        return True

    return False


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


def _log_error(log_file_path: pathlib.Path, message: str) -> None:
    """Append a single error report to the given error log, separated by a blank line."""
    with log_file_path.open(mode="a") as file_stream:
        file_stream.write(f"{message}\n\n")


def _run(base_directory: pathlib.Path, limit: int | None) -> None:
    submodule_dir = base_directory / "sourcedata" / "content-id-to-usage-dandiset-path" / "derivatives"
    submodule_file_path = submodule_dir / "content_id_to_usage_dandiset_path.yaml"
    with submodule_file_path.open(mode="r") as file_stream:
        content_id_to_dandiset_path = yaml.safe_load(file_stream)

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
            qualifies = _any_electrical_series_qualifies(s3_url=s3_url)
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
