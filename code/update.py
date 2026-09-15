"""Assess which sessions the AIND ephys pipeline can actually process.

The candidates come from `qualifying-lfp-content-ids`, narrowed to the files
`content-id-to-valid-nwb-file` has confirmed open and pass the NWB Inspector, so nothing here
repeats that work. What remains is this cache's own question, asked of each session's
ElectricalSeries through SpikeInterface.

Everything shared with the other caches -- the argument parsing, the logging, the batch cap, the
stage-routed error logs, the output paths, and testing mode -- comes from `dandi_cache_utils`,
which the runtime image carries.
"""

import dandi_cache_utils as dandi_cache
import numpy
import spikeinterface.extractors

#: The side output: which of the `false` entries are false because the assessment failed, rather
#: than because the session does not qualify.
ERROR_IDS = "error_ids.jsonl"

# Resolving an asset and reading it through SpikeInterface fail for unrelated reasons, and a week
# of failures is only triageable when each kind has its own log.
STAGES = {
    "retrieving asset information from the DANDI API": "dandi_api_errors.txt",
    "validating SpikeInterface metadata": "spikeinterface_errors.txt",
}

# Only series above this rate are spike-sorted by the pipeline; the rest, such as LFP, are ignored.
RATE_THRESHOLD_HZ = 10_000
MINIMUM_DURATION_SECONDS = 120


def session_qualifies(url: str, /) -> bool:
    """Whether the AIND ephys pipeline could process every ElectricalSeries in one NWB file.

    Only ElectricalSeries in the acquisition submodule with a sampling rate above 10 kHz are
    assessed; lower-rate series (e.g. LFP) are ignored. The pipeline processes every such series,
    so a single non-processable one would make it fail: each must have a duration longer than 120
    seconds, have no NaN channel locations, and survive the pipeline's split-then-aggregate step.
    The file qualifies when at least one acquisition ElectricalSeries exceeds 10 kHz and every
    series that does passes those checks.
    """
    acquisition_series_paths = dandi_cache.nwb.electrical_series_paths(url)
    if not acquisition_series_paths:
        return False

    any_above_rate_threshold = False
    for electrical_series_path in acquisition_series_paths:
        extractor = spikeinterface.extractors.NwbRecordingExtractor(
            file_path=url, stream_mode="remfile", electrical_series_path=electrical_series_path
        )

        # The remaining assessments are expensive, so filter on the cheap sampling-rate metadata
        # first and skip every series the pipeline would not sort anyway.
        if extractor.get_sampling_frequency() <= RATE_THRESHOLD_HZ:
            continue
        any_above_rate_threshold = True

        if extractor.get_total_duration() <= MINIMUM_DURATION_SECONDS:
            return False

        # A NaN channel location breaks the pipeline's downstream distance and geometry
        # computations just as surely as the aggregation failure below, so exclude it the same way.
        if numpy.isnan(extractor.get_channel_locations()).any():
            return False

        # Mimic the pipeline as closely as possible. job_dispatch (aind-ephys-job-dispatch) splits
        # a recording with `recording.split_by("group")` when it has more than one channel group,
        # and nwb_ecephys (aind-ecephys-nwb) then recombines those per-group recordings with
        # `spikeinterface.aggregate_channels`. That recombination raises "Locations are not
        # unique!" when the per-group "location" properties collide -- exactly the failure we are
        # trying to predict (channelsaggregationrecording.py). Reproduce the same split-then-
        # aggregate here so that any session that would crash nwb_ecephys is excluded. We catch
        # every exception because any aggregation failure (not just the location assertion) would
        # equally break the pipeline.
        if len(set(extractor.get_channel_groups())) > 1:
            recording_groups = list(extractor.split_by(property="group").values())
            try:
                spikeinterface.aggregate_channels(recording_groups)
            except Exception:
                return False

    return any_above_rate_threshold


def main() -> None:
    dataset, arguments = dandi_cache.open_dataset()
    qualifying_lfp = dataset.read_input("qualifying-lfp-content-ids")
    usage_dandiset_paths = dataset.read_input("content-id-to-usage-dandiset-path")
    validity = dataset.read_input("content-id-to-valid-nwb-file")

    assessed = dataset.read_output_lookup()
    error_ids = dandi_cache.read_ids(dataset.output_file_path(ERROR_IDS))

    candidates = {content_id for content_id, qualifies in qualifying_lfp.items() if qualifies}

    # A file the upstream cache has already found invalid is disqualified without spending any
    # further work on it. That is not an error, just a session that does not qualify.
    for content_id in candidates - assessed.keys():
        if validity.get(content_id) is False:
            assessed[content_id] = False

    def is_assessable(content_id: str, /) -> bool:
        """Whether this content ID can be assessed yet, rather than left for a later run.

        A content ID upstream has not yet resolved to a path, or not yet judged valid, is waiting
        on that cache rather than on this one.
        """
        location = usage_dandiset_paths.get(content_id)
        return (
            location is not None
            and validity.get(content_id) is True
            and dandi_cache.nwb.is_nwb_path(dandi_cache.api.split_location(location)[1])
        )

    resolver = dandi_cache.api.AssetResolver()

    def assess(content_id, item) -> bool:
        dandiset_id, path = dandi_cache.api.split_location(usage_dandiset_paths[content_id])
        # Reported with any failure, so an error log names the asset rather than only its content ID.
        item.context.update({"dandiset ID": dandiset_id, "path": path})

        item.stage = "retrieving asset information from the DANDI API"
        url = resolver.content_url(dandiset_id, path)
        item.context["URL"] = url

        item.stage = "validating SpikeInterface metadata"
        return session_qualifies(url)

    dandi_cache.run_incremental_update(
        dataset,
        candidates=[content_id for content_id in candidates if is_assessable(content_id)],
        process=assess,
        recorded=assessed,
        limit=dandi_cache.effective_limit(testing=dataset.testing, limit=arguments.limit),
        # A session whose assessment failed is recorded as not qualifying, which is what the
        # pipeline would conclude too, and `error_ids` is what keeps the two kinds of `false`
        # apart. Retrying it would only fail the same way.
        on_failure=dandi_cache.RECORD,
        failure_value=False,
        on_error=lambda content_id, _item: error_ids.add(content_id),
        on_write=lambda: dandi_cache.write_ids(dataset.output_file_path(ERROR_IDS), error_ids),
        stages=STAGES,
        describe=lambda qualifies: "qualifies" if qualifies else "does not qualify",
        checkpoint_every=50,
    )


if __name__ == "__main__":
    main()
