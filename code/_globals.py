"""The constants of this repository's SpikeInterface helpers, in one place."""

# Only series above this rate are spike-sorted by the pipeline; the rest, such as LFP, are ignored.
RATE_THRESHOLD_HZ = 10_000

#: Where an NWB file keeps what an instrument recorded, as opposed to what was derived from it.
ACQUISITION_PREFIX = "acquisition/"
