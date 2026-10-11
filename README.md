# DANDI Cache: Qualifying AIND Content IDs

For each content ID that qualifies for the [`qualifying-lfp-content-ids`](https://github.com/dandi-cache/qualifying-lfp-content-ids) cache, records whether it also qualifies for the (stricter) AIND ephys pipeline.



## AIND ephys qualification conditions

To qualify for the DANDI Compute AIND ephys pipeline, an asset must meet the following conditions:
1. The asset must qualify for the [`qualifying-lfp-content-ids`](https://github.com/dandi-cache/qualifying-lfp-content-ids) cache.
2. The asset must be an NWB file, either in HDF5 or Zarr format.
3. The NWB file must be valid (openable, satisfying DANDI upload requirements), as determined by the [`content-id-to-valid-nwb-file`](https://github.com/dandi-cache/content-id-to-valid-nwb-file) cache.
4. The NWB file must contain at least one `ElectricalSeries` data stream in the `acquisition` group with a `rate` greater than 10 kHz.

Only acquisition `ElectricalSeries` with a `rate` greater than 10 kHz are assessed further; lower-rate series (e.g. LFP) are ignored.
The pipeline processes *every* such series, so a single non-processable series would cause it to fail.

Each acquisition `ElectricalSeries` above 10 kHz must therefore meet the following conditions:

a.
Its total duration must be more than 2 minutes.

b.
Its channel locations must not contain `NaN` values.

c.
It must survive the pipeline's split-then-aggregate step.
When a series spans more than one channel group, the pipeline splits it by group (as `aind-ephys-job-dispatch` does) and recombines the groups with `spikeinterface.aggregate_channels` (as `aind-ecephys-nwb` does); this requires the relative channel locations to remain unique once the groups are combined.
The qualification check mimics this exactly by performing the same split-and-aggregate and excluding the asset if it raises.
- A common error with this condition involves incorrectly specified indices in the `DynamicTableRegion` for the `electrodes` of an `ElectricalSeries`.



## One-time use

`dist` publishes each output as a [Zarr](https://zarr.dev/) store: the cache itself is `derivatives/qualifying_aind_content_ids.zarr/`, with the cache's other outputs beside it as stores of the same kind (`derivatives/error_ids.zarr/`).
A store is one array of [JSON Lines](https://jsonlines.org/) text cut into chunk files, each holding whole lines and gzip-compressed.
Where a key appears more than once, the last line is its current entry, and the published copy has no repeats.
It can be read with the standard library alone, or opened remotely with any Zarr library.

### Python API (recommended)

Fetch `stream/zarr.json` for the array's length and chunk size, then each chunk in order:

```python
import gzip
import json
import math
import urllib.request

store = "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/dist/derivatives/qualifying_aind_content_ids.zarr"


def fetch(path):
    with urllib.request.urlopen(f"{store}/{path}") as response:
        return response.read()


array = json.loads(fetch("stream/zarr.json"))
length = array["shape"][0]
chunk_bytes = array["chunk_grid"]["configuration"]["chunk_shape"][0]

qualifying_aind_content_ids = {}
for index in range(math.ceil(length / chunk_bytes)):
    for line in gzip.decompress(fetch(f"stream/c/{index}")).decode("utf-8").splitlines():
        if line.strip():
            qualifying_aind_content_ids.update(json.loads(line))
```

An entry is not filed under its key: the chunks hold the entries in the order they were recorded, so looking up one means reading through them.
To look up many, read it once as above, or clone the `derivatives` branch.

### Remote Zarr store

Anything that opens Zarr can open the store over HTTP, reading only the chunks it needs.
The group's `zarr.json` carries the array's metadata, so opening it takes one request:

```bash
pip install "zarr>=3" fsspec aiohttp
```

```python
import json

import zarr

url = "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/dist/derivatives/qualifying_aind_content_ids.zarr"
stream = zarr.open_group(url, mode="r")["stream"]
print(stream.shape, stream.chunks)  # Bytes in all, and bytes per chunk.

# The first chunk, fetched and decompressed on its own: whole lines, each `{"<key>": <value>}`.
first_chunk = stream[: stream.chunks[0]].tobytes().decode("utf-8")
for line in first_chunk.splitlines():
    if line.strip():
        print(json.loads(line))
        break
```

Reading `stream[:]` returns the whole text; to read it in pieces, step through it by `stream.chunks[0]`, as each chunk is fetched whole.

### Save to file

A raw URL cannot be listed, so to fetch every chunk at once clone the `dist` branch, which holds nothing else:

```bash
git clone --branch dist --depth 1 https://github.com/dandi-cache/qualifying-aind-content-ids.git
```



## Repeated use

If you plan on using this cache regularly, clone this repository:

```bash
git clone --branch dist --single-branch https://github.com/dandi-cache/qualifying-aind-content-ids.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/qualifying-aind-content-ids.git --branch derivatives
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/qualifying-aind-content-ids pull
```

This will minimize data overhead by only loading the most recent changes.

Each output is a store in the clone, with every chunk gzip-compressed:

```python
import gzip
import json
import pathlib

chunks = pathlib.Path("qualifying-aind-content-ids/derivatives/qualifying_aind_content_ids.zarr/stream/c")
qualifying_aind_content_ids = {}
for chunk in sorted(chunks.iterdir(), key=lambda path: int(path.name)):
    for line in gzip.decompress(chunk.read_bytes()).decode("utf-8").splitlines():
        if line.strip():
            qualifying_aind_content_ids.update(json.loads(line))
```
