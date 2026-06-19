import argparse
import gzip
import pathlib
import shutil


def _compress(file_path: pathlib.Path, /) -> None:
    compressed_file_path = file_path.parent / f"{file_path.name}.gz"
    with file_path.open(mode="rb") as source_stream:
        with gzip.open(filename=compressed_file_path, mode="wb") as compressed_stream:
            shutil.copyfileobj(source_stream, compressed_stream)


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Compress derivative JSONL files with gzip.")
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
        _compress(jsonl_file_path)
