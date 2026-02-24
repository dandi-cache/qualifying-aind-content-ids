# DANDI Cache: Qualifying AIND Content IDs

A flat subset of `content-id-to-nwb-files` which have been identified to be qualify for the AIND ephys pipeline.

Updated frequently.

Primarily for use by developers.



## AIND ephys qualification conditions

To qualify for the DANDI Compute AIND ephys pipeline, an asset must meet the following conditions:

1) The asset must be listed within a public Dandiset.
2) The asset must be an NWB file, either in HDF5 or Zarr format.
3) The asset must contain at least one `ElectricalSeries` data stream in the acquisition group with a rate greater than 10 kHz.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache as a minified and compressed JSON file:

### Python API (recommended)

```python
import gzip
import json
import requests

url = "https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/derivatives/qualifying_aind_content_ids.min.json.gz"
response = requests.get(url)
qualifying_aind_content_ids = json.loads(gzip.decompress(data=response.content))
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/qualifying-aind-content-ids/refs/heads/min/derivatives/qualifying_aind_content_ids.min.json.gz -o qualifying_aind_content_ids.min.json.gz
```



## Repeated use

If you plan on using this cache regularly, clone this repository:

```bash
git clone https://github.com/dandi-cache/qualifying-aind-content-ids.git
```

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/qualifying-aind-content-ids pull
```

This will minimize data overhead by only loading the most recent changes.
