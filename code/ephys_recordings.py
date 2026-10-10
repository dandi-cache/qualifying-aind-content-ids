"""Reading an NWB file's ElectricalSeries through SpikeInterface.

This is the one place that knows how SpikeInterface streams a remote file. `update.py` asks its
questions of the recordings yielded here and never opens one itself, so the qualification rules
there read without any of the plumbing.
"""

import collections.abc

import spikeinterface.extractors

#: Where an NWB file keeps what an instrument recorded, as opposed to what was derived from it.
ACQUISITION_PREFIX = "acquisition/"


def acquisition_recordings(url: str, /) -> collections.abc.Iterator:
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
