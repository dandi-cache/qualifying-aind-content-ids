import gzip
import json
import pathlib

import yaml


def _minify(file_path: pathlib.Path, /) -> None:

    with file_path.open(mode="r") as file_stream:
        file_content = yaml.safe_load(file_stream)

    minified_file_path = file_path.parent / f"{file_path.stem}.min.json.gz"
    with gzip.open(filename=minified_file_path, mode="wt", encoding="utf-8") as file_stream:
        json.dump(obj=file_content, fp=file_stream)


if __name__ == "__main__":
    repo_head = pathlib.Path(__file__).parent.parent
    derivatives_dir = repo_head / "derivatives"

    for yaml_file_path in derivatives_dir.glob("*.yaml"):
        _minify(yaml_file_path)
