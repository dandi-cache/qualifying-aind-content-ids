# DANDI Cache: Qualifying AIND Content IDs

A flat subset of `content-id-to-nwb-files` that has been identified to qualify for the AIND ephys pipeline.



## AIND ephys qualification conditions

To qualify for the DANDI Compute AIND ephys pipeline, an asset must meet the following conditions:
1. The asset must be listed within a public Dandiset.
2. The asset must be an NWB file, either in HDF5 or Zarr format.
3. The NWB file must be valid (openable, satisfying DANDI upload requirements), as determined by the
   [`content-id-to-valid-nwb-file`](https://github.com/dandi-cache/content-id-to-valid-nwb-file) cache.
4. The NWB file must contain at least one `ElectricalSeries` data stream in the `acquisition` group with a `rate` greater than 10 kHz.

Only acquisition `ElectricalSeries` with a `rate` greater than 10 kHz are assessed further; lower-rate series (e.g. LFP) are ignored.
The pipeline processes *every* such series, so a single non-processable series would cause it to fail.

Each acquisition `ElectricalSeries` above 10 kHz must therefore meet the following conditions:

a. Its total duration must be more than 2 minutes.

b. Its channel locations must not contain `NaN` values.

c. It must survive the pipeline's split-then-aggregate step. When a series spans more than one channel group, the pipeline splits it by group (as `aind-ephys-job-dispatch` does) and recombines the groups with `spikeinterface.aggregate_channels` (as `aind-ecephys-nwb` does); this requires the relative channel locations to remain unique once the groups are combined. The qualification check mimics this exactly by performing the same split-and-aggregate and excluding the asset if it raises.
- A common error with this condition involves incorrectly specified indices in the `DynamicTableRegion` for the `electrodes` of an `ElectricalSeries`.



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
