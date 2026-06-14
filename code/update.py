import itertools
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


def _electrical_series_qualifies(s3_url: str, electrical_series_path: str) -> bool:
    """
    Determine whether a single ElectricalSeries qualifies.

    A series qualifies when it is under the acquisition submodule, has unique channel locations,
    a rate above 10kHz, and a duration longer than 120 seconds.
    """
    if not electrical_series_path.startswith("acquisition/"):
        return False

    extractor = spikeinterface.extractors.NwbRecordingExtractor(
        file_path=s3_url, stream_mode="remfile", electrical_series_path=electrical_series_path
    )

    try:
        channel_locations = extractor.get_channel_locations()
    except Exception as exception:
        if "no channel locations" in str(exception).lower():
            return False
        raise

    unique_locations = set(tuple(loc) for loc in channel_locations)
    if len(unique_locations) != extractor.get_num_channels():
        return False

    if extractor.get_sampling_frequency() <= 10_000:
        return False

    if extractor.get_total_duration() <= 120:
        return False

    return True


def _nwbfile_qualifies(s3_url: str) -> bool:
    """Determine whether an NWB file qualifies, which is when at least one ElectricalSeries qualifies."""
    electrical_series_paths = spikeinterface.extractors.NwbRecordingExtractor.fetch_available_electrical_series_paths(
        file_path=s3_url, stream_mode="remfile"
    )
    for electrical_series_path in electrical_series_paths:
        if _electrical_series_qualifies(s3_url=s3_url, electrical_series_path=electrical_series_path):
            return True

    return False


def _run(base_directory: pathlib.Path, limit: int | None) -> None:
    submodule_dir = base_directory / "sourcedata" / "content-id-to-nwb-files" / "derivatives"
    submodule_file_path = submodule_dir / "content_id_to_nwb_files.yaml"
    with submodule_file_path.open(mode="r") as file_stream:
        content_id_to_dandiset_paths = yaml.safe_load(file_stream)

    dandi_api_errors_log_file_path = base_directory / "logs" / "dandi_api_errors.txt"
    file_open_errors_log_file_path = base_directory / "logs" / "file_open_errors.txt"
    nwb_inspector_errors_log_dir = base_directory / "logs" / "nwb_inspector_errors"
    nwb_inspector_errors_log_dir.mkdir(exist_ok=True)
    error_ids_file_path = base_directory / "derivatives" / "error_ids.yaml"
    with error_ids_file_path.open(mode="r") as file_stream:
        yaml_content = yaml.safe_load(file_stream)
        error_ids = set(yaml_content) if yaml_content is not None else set()

    processed_ids_file_path = base_directory / "derivatives" / "processed_ids.yaml"
    with processed_ids_file_path.open(mode="r") as file_stream:
        yaml_content = yaml.safe_load(file_stream)
        processed_ids = set(yaml_content) if yaml_content is not None else set()

    content_ids_to_process = set(content_id_to_dandiset_paths.keys()) - error_ids - processed_ids

    qualifying_aind_content_ids_file_path = base_directory / "derivatives" / "qualifying_aind_content_ids.yaml"
    with qualifying_aind_content_ids_file_path.open(mode="r") as file_stream:
        yaml_content = yaml.safe_load(file_stream)
        qualifying_aind_content_ids = set(yaml_content) if yaml_content is not None else set()

    client = dandi.dandiapi.DandiAPIClient()  # Run tokenless to ensure only public dandisets are accessed
    dandi_config = nwbinspector.load_config("dandi")
    for content_id in itertools.islice(content_ids_to_process, limit):
        dandiset_id, dandiset_paths = next(iter(content_id_to_dandiset_paths[content_id].items()))
        first_path = dandiset_paths[0]  # Only test the first element and trust the rest

        try:
            dandiset = client.get_dandiset(dandiset_id=dandiset_id)
            asset = dandiset.get_asset_by_path(path=first_path)
            s3_url = asset.get_content_url(follow_redirects=1, strip_query=True)
        except Exception as exception:
            with dandi_api_errors_log_file_path.open(mode="a") as file_stream:
                message = (
                    f"Error retrieving information about file at path {first_path} in dandiset ID {dandiset_id} "
                    "with `{content_id=}`!\n\n"
                    f"{type(exception)}:{str(exception)}\n\n"
                    f"{traceback.format_exc()}"
                )
                file_stream.write(message)

            error_ids.add(content_id)
            continue

        try:
            suffixes = pathlib.Path(first_path).suffixes
            if ".zarr" in suffixes:
                io = hdmf_zarr.NWBZarrIO(s3_url, mode="r")
                nwbfile = io.read()
            else:
                rem_file = remfile.File(url=s3_url)
                h5py_file = h5py.File(name=rem_file, mode="r")
                io = pynwb.NWBHDF5IO(file=h5py_file)
                nwbfile = io.read()
        except Exception as exception:
            with file_open_errors_log_file_path.open(mode="a") as file_stream:
                message = (
                    f"Error opening file at path {first_path} in dandiset ID {dandiset_id} from URL {s3_url} "
                    "with `{content_id=}`!\n\n"
                    f"{type(exception)}:{str(exception)}\n\n"
                    f"{traceback.format_exc()}"
                )
                file_stream.write(message)

            error_ids.add(content_id)
            continue

        inspector_messages = list(
            nwbinspector.inspect_nwbfile_object(
                nwbfile_object=nwbfile,
                config=dandi_config,
                importance_threshold=nwbinspector.Importance.CRITICAL,
            )
        )
        if inspector_messages:
            nwb_inspector_errors_log_file_path = nwb_inspector_errors_log_dir / f"{content_id}.txt"
            with nwb_inspector_errors_log_file_path.open(mode="w") as file_stream:
                message = (
                    f"NWB Inspector found CRITICAL issues in file at path {first_path} "
                    f"in dandiset ID {dandiset_id} with `{content_id=}`!\n\n"
                )
                for inspector_message in inspector_messages:
                    message += f"{inspector_message}\n\n"
                file_stream.write(message)

            error_ids.add(content_id)
            continue

        # Qualify the session if at least one ElectricalSeries under acquisition qualifies
        try:
            qualifies = _nwbfile_qualifies(s3_url=s3_url)
        except Exception as exception:
            with file_open_errors_log_file_path.open(mode="a") as file_stream:
                message = (
                    f"Error validating SpikeInterface metadata for file at path {first_path} "
                    f"in dandiset ID {dandiset_id} from URL {s3_url} with `{content_id=}`!\n\n"
                    f"{type(exception)}:{str(exception)}\n\n"
                    f"{traceback.format_exc()}"
                )
                file_stream.write(message)

            error_ids.add(content_id)
            continue

        if qualifies:
            qualifying_aind_content_ids.add(content_id)
        processed_ids.add(content_id)

    with error_ids_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=sorted(list(error_ids)), stream=file_stream)
    with processed_ids_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=sorted(list(processed_ids)), stream=file_stream)
    with qualifying_aind_content_ids_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=sorted(list(qualifying_aind_content_ids)), stream=file_stream)


if __name__ == "__main__":
    repo_head = pathlib.Path(__file__).parent.parent

    _run(repo_head, limit=3_000)
