import gzip
import itertools
import json
import pathlib

import dandi.dandiapi
import h5py
import hdmf_zarr
import pynwb
import remfile
import requests
import yaml


def _run(base_directory: pathlib.Path, limit: int | None = 1_000) -> None:
    url = "https://raw.githubusercontent.com/dandi-cache/content-id-to-nwb-files/refs/heads/min/derivatives/content_id_to_nwb_files.min.json.gz"
    response = requests.get(url)
    content_id_to_dandiset_paths = json.loads(gzip.decompress(data=response.content))

    processed_ids_file_path = base_directory / "derivatives" / "processed_ids.yaml"
    with processed_ids_file_path.open(mode="r") as file_stream:
        yaml_content = yaml.safe_load(file_stream)
        processed_ids = set(yaml_content) if yaml_content is not None else set()
    content_ids_to_process = set(content_id_to_dandiset_paths.keys()) - processed_ids

    qualifying_aind_content_ids_file_path = base_directory / "derivatives" / "qualifying_aind_content_ids.yaml"
    with qualifying_aind_content_ids_file_path.open(mode="r") as file_stream:
        yaml_content = yaml.safe_load(file_stream)
        qualifying_aind_content_ids = set(yaml_content) if yaml_content is not None else set()

    client = dandi.dandiapi.DandiAPIClient()  # Run tokenless to ensure only public dandisets are accessed
    for content_id in itertools.islice(content_ids_to_process, limit):
        # Only test the first element and trust the rest
        dandiset_id, dandiset_paths = next(iter(content_id_to_dandiset_paths[content_id].items()))
        first_path = dandiset_paths[0]

        dandiset = client.get_dandiset(dandiset_id=dandiset_id)
        asset = dandiset.get_asset_by_path(path=first_path)
        s3_url = asset.get_content_url(follow_redirects=1, strip_query=True)

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
            # If file fails to read, count as 'processed' and move on
            print(exception)
            processed_ids.add(content_id)
            continue

        for neurodata_object in nwbfile.acquisition.values():
            if not isinstance(neurodata_object, pynwb.ecephys.ElectricalSeries):
                continue

            if neurodata_object.rate is not None and neurodata_object.rate > 10_000:
                qualifying_aind_content_ids.add(content_id)

    with processed_ids_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=sorted(list(processed_ids)), stream=file_stream)
    with qualifying_aind_content_ids_file_path.open(mode="w") as file_stream:
        yaml.safe_dump(data=sorted(list(qualifying_aind_content_ids)), stream=file_stream)


if __name__ == "__main__":
    repo_head = pathlib.Path(__file__).parent.parent

    _run(repo_head)
