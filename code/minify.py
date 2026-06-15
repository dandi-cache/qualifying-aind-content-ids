import argparse
import gzip
import json
import pathlib


def _minify(file_path: pathlib.Path, /) -> None:

    with file_path.open(mode="r") as file_stream:
        file_content = [json.loads(line) for line in file_stream if line.strip()]

    minified_file_path = file_path.parent / f"{file_path.name}.gz"
    with gzip.open(filename=minified_file_path, mode="wt", encoding="utf-8") as file_stream:
        json.dump(obj=file_content, fp=file_stream)


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Minify derivative JSONL files to compressed JSON.")
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `derivatives` directory. Primarily used in tests. "
            "Defaults to the repository root."
        ),
    )
    args = parser.parse_args()

    derivatives_dir = args.base_directory / "derivatives"

    for jsonl_file_path in derivatives_dir.glob("*.jsonl"):
        _minify(jsonl_file_path)
