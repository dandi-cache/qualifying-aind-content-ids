"""Assess which sessions the AIND ephys pipeline can actually process.

The candidates come from `qualifying-lfp-content-ids`, narrowed to the files
`content-id-to-valid-nwb-file` has confirmed open and pass the NWB Inspector, so nothing here
repeats that work. What remains is this cache's own question, asked of each session's
ElectricalSeries through SpikeInterface.

Everything shared with the other caches -- the argument parsing, the logging, the batch cap, the
stage-routed error logs, the output paths, and testing mode -- comes from `dandi_cache_utils`,
which the runtime image carries.
"""

import _local_utils
import dandi_cache_utils as dandi_cache
import spikeinterface

#: The side output: which of the `false` entries are false because the assessment failed, rather
#: than because the session does not qualify.
ERROR_IDS = "error_ids.jsonl"

# Resolving an asset and reading it through SpikeInterface fail for unrelated reasons, and a week
# of failures is only triageable when each kind has its own log.
STAGES = {
    "retrieving asset information from the DANDI API": "dandi_api_errors.txt",
    "validating SpikeInterface metadata": "spikeinterface_errors.txt",
}


def survives_channel_aggregation(recording, /) -> bool:
    """Whether the pipeline's split-then-aggregate step would work on this series.

    Mimics the pipeline as closely as possible. job_dispatch (aind-ephys-job-dispatch) splits a
    recording with `recording.split_by("group")` when it has more than one channel group, and
    nwb_ecephys (aind-ecephys-nwb) then recombines those per-group recordings with
    `spikeinterface.aggregate_channels`. That recombination raises "Locations are not unique!" when
    the per-group "location" properties collide -- exactly the failure this predicts
    (channelsaggregationrecording.py). Reproducing the same split-then-aggregate here excludes any
    session that would crash nwb_ecephys. Every exception counts, because any aggregation failure
    (not just the location assertion) would equally break the pipeline.
    """
    if len(set(recording.get_channel_groups())) <= 1:
        return True

    recording_groups = list(recording.split_by(property="group").values())
    try:
        spikeinterface.aggregate_channels(recording_groups)
    except Exception:
        return False
    return True


def pipeline_can_process(recording, /) -> bool:
    """Whether the pipeline could process this series, which it must for every one it sorts.

    Ordered cheapest first, and the first that fails ends the check: the duration and the channel
    locations are metadata, and the aggregation builds recordings.
    """
    return (
        _local_utils.lasts_long_enough(recording)
        and _local_utils.has_channel_locations(recording)
        and survives_channel_aggregation(recording)
    )


def session_qualifies(url: str, /) -> bool:
    """Whether the AIND ephys pipeline could process every ElectricalSeries in one NWB file.

    Only ElectricalSeries in the acquisition submodule are assessed, and only those the pipeline
    would sort, which are the ones sampled above 10 kHz: lower-rate series (e.g. LFP) are ignored.
    The pipeline processes every such series, so a single one it could not process would make it
    fail. Each must last longer than 120 seconds, have no NaN channel locations, and survive the
    pipeline's split-then-aggregate step (the plain checks are in `_local_utils.py`).
    The file qualifies when at least one acquisition ElectricalSeries is sorted and every one that
    is can be processed.
    """
    any_sorted = False
    for recording in _local_utils.get_acquisition_recordings(url):
        # The other checks are the expensive ones, so the cheap sampling-rate metadata comes first
        # and skips every series the pipeline would not sort anyway.
        if not _local_utils.is_sorted_by_pipeline(recording):
            continue
        any_sorted = True

        if not pipeline_can_process(recording):
            return False

    return any_sorted


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
        limit=dataset.limit(arguments.limit),
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
