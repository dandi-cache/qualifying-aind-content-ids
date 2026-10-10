"""This repository's own helpers for SpikeInterface: opening a file's recordings, and the simple checks of one.

`update.py` keeps the rules that carry the reasoning and the order they are applied in, which is
what decides a session; this module has the means. It opens each ElectricalSeries as a recording,
and answers the plain conditions asked of a recording, so `update.py` reads without the
SpikeInterface plumbing.

It is local on purpose, and named so: it stays in this repository, with the `spikeinterface` pin in
`envs/pyproject.toml`, and is not for `dandi_cache_utils`, the shared upstream library, whose base
image does not carry SpikeInterface. See `AGENTS.md`.
"""

import collections.abc

import numpy
import spikeinterface.extractors

# Only series above this rate are spike-sorted by the pipeline; the rest, such as LFP, are ignored.
RATE_THRESHOLD_HZ = 10_000
MINIMUM_DURATION_SECONDS = 120

#: Where an NWB file keeps what an instrument recorded, as opposed to what was derived from it.
ACQUISITION_PREFIX = "acquisition/"


def get_acquisition_recordings(url: str, /) -> collections.abc.Iterator:
    """Each ElectricalSeries in the file's acquisition group, as a SpikeInterface recording.

    The series are listed up front, which reads the file's structure, but each is opened only as the
    caller reaches it, so a caller that stops at the first series that disqualifies the file never
    pays to open the rest.
    """
    series_paths = spikeinterface.extractors.NwbRecordingExtractor.fetch_available_electrical_series_paths(
        file_path=url, stream_mode="remfile"
    )
    for series_path in series_paths:
        if series_path.startswith(ACQUISITION_PREFIX):
            yield spikeinterface.extractors.NwbRecordingExtractor(
                file_path=url, stream_mode="remfile", electrical_series_path=series_path
            )


# Simple checks of one SpikeInterface recording, an ElectricalSeries of the file.


def is_sorted_by_pipeline(recording, /) -> bool:
    """Whether the pipeline spike-sorts this series at all, which only its sampling rate decides."""
    return recording.get_sampling_frequency() > RATE_THRESHOLD_HZ


def lasts_long_enough(recording, /) -> bool:
    """Whether the series runs for longer than the pipeline needs to work with."""
    return recording.get_total_duration() > MINIMUM_DURATION_SECONDS


def has_channel_locations(recording, /) -> bool:
    """Whether every channel has a position.

    A NaN channel location breaks the pipeline's downstream distance and geometry computations just
    as surely as a channel aggregation failure, so it excludes a series the same way.
    """
    return not numpy.isnan(recording.get_channel_locations()).any()
