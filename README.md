# DANDI Cache: Qualifying AIND Content IDs

A flat subset of `content-id-to-nwb-files` that has been identified to qualify for the AIND ephys pipeline.



## AIND ephys qualification conditions

To qualify for the DANDI Compute AIND ephys pipeline, an asset must meet the following conditions:
1. The asset must be listed within a public Dandiset.
2. The asset must be an NWB file, either in HDF5 or Zarr format.
3. The NWB file must be valid (openable, satisfying DANDI upload requirements).
4. The asset must contain at least one qualifying `ElectricalSeries` data stream in the `acquisition` group.

For an `ElectricalSeries` to qualify, it must meet the following conditions:
a. Its rate must be greater than 10 kHz.
b. Its relative channel locations must be specified and must be unique across the `ElectricalSeries`.
- A common error with this condition involves incorrectly specified indices in the `DynamicTableRegion` for the `electrodes` of an `ElectricalSeries`.
c. The total duration of the `ElectricalSeries` must be more than 2 minutes.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a gzip-compressed JSON Lines file:

### Python API (recommended)

```python
import gzip
import json

import requests

url = "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/dist/derivatives/qualifying_aind_content_ids.jsonl.gz"
response = requests.get(url)
content = gzip.decompress(data=response.content).decode(encoding="utf-8")
qualifying_aind_content_ids = [json.loads(line) for line in content.splitlines() if line.strip()]
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/dist/derivatives/qualifying_aind_content_ids.jsonl.gz -o qualifying_aind_content_ids.jsonl.gz
```



## Repeated use

If you plan on using this cache regularly, clone this repository:

```bash
git clone --branch dist --single-branch https://github.com/dandi-cache/qualifying-aind-content-ids.git
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/qualifying-aind-content-ids pull
```

This will minimize data overhead by only loading the most recent changes.
