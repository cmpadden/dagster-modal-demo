import re
import urllib.request
from dataclasses import dataclass
from typing import Optional

import dagster as dg
import feedparser

BROWSER_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"


def download_transcript_if_exists(entry) -> Optional[str]:
    """Downloads full text transcript if present on RSS feed entry.

    Args:
        entry (Unknown): entry from feedparser

    Returns:
        Optional[str]: transcript for podcast if present

    """
    if transcript := entry.get("podcast_transcript"):
        if transcript_url := transcript.get("url"):
            with urllib.request.urlopen(transcript_url) as response:
                return response.read().decode("utf-8")


def download_bytes(url: str) -> bytes:
    """Downloads bytes from provided url

    Args:
        url (str): url pointing to download location of bytes

    Returns:
        bytes: Bytes of object being downloaded
    """
    request = urllib.request.Request(
        url,
        headers={"User-Agent": BROWSER_USER_AGENT},
    )
    with urllib.request.urlopen(request) as response:
        return response.read()


def store_bytes(bs: bytes, destination: str) -> None:
    """Stores bytes object to target file destination

    Args:
        bs (bytes): object to store to file-system
        destination (str): location to store binary data
    """
    with open(destination, "wb") as f:
        f.write(bs)


def sanitize(text: str, lower: bool = True) -> str:
    """Prepares text to be used as a file name.

    Args:
        text (str): text to be sanitized
        lower (str): option to enable to disable converting to lower case

    Returns:
        sanitized text

    """
    text = re.sub(r"[^a-zA-Z\d]", "_", text)
    text = re.sub(r"_+", "_", text)
    if lower:
        text = text.lower()
    return text


"""

Task List
- [ ] Limit results of feed using `modified`
- [ ] Create sensor that gets latest podcasts (for podcasts in YAML file?)
- [ ] Create data class for podcast feed

"""

DEFAULT_POLLING_INTERVAL = 10 * 60  # 10 minutes


podcast_partitions_def = dg.DynamicPartitionsDefinition(name="podcast_id")


# @dg.asset(partitions_def=podcast_partitions_def)
# def podcast_metadata(context: dg.AssetExecutionContext):
#     context.log.info("metadata %s", context.partition_key)
#
#
# @dg.asset(partitions_def=podcast_partitions_def, deps=[podcast_metadata])
# def podcast_audio_file(context: dg.AssetExecutionContext):
#     context.log.info("audio %s", context.partition_key)
#
#
# @dg.asset(partitions_def=podcast_partitions_def, deps=[podcast_audio_file])
# def podcast_transcription(context: dg.AssetExecutionContext):
#     context.log.info("transcript %s", context.partition_key)
#
#
# podcast_transcription_job = dg.define_asset_job(
#     name="podcast_transcription_job",
#     selection=dg.AssetSelection.assets(
#         podcast_metadata, podcast_audio_file, podcast_transcription
#     ),
#     partitions_def=podcast_partitions_def,
# )


@dataclass
class RSSFeedDefinition:
    name: str
    url: str
    max_backfill_size: int = 3


def rss_pipeline_factory(feed_definition: RSSFeedDefinition) -> dg.Definitions:
    @dg.asset(
        name=f"{feed_definition.name}_metadata", partitions_def=podcast_partitions_def
    )
    def _podcast_metadata(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        return dg.MaterializeResult(metadata={"size": 0})

    job_name = f"{feed_definition.name}_job"
    _job = dg.define_asset_job(
        name=job_name,
        selection=dg.AssetSelection.assets(_podcast_metadata),
        partitions_def=podcast_partitions_def,
    )

    @dg.sensor(
        name=f"rss_sensor_{feed_definition.name}",
        minimum_interval_seconds=DEFAULT_POLLING_INTERVAL,
        default_status=dg.DefaultSensorStatus.RUNNING,
        job=_job,
    )
    def _sensor(context: dg.SensorEvaluationContext):
        etag = context.cursor
        context.log.info("querying feed with cursor etag: %s", etag)
        feed = feedparser.parse(feed_definition.url, etag=etag)

        num_entries = len(feed.entries)
        context.log.info("total number of entries: %s", num_entries)

        if num_entries > feed_definition.max_backfill_size:
            context.log.info(
                "truncating entries to %s", feed_definition.max_backfill_size
            )
            entries = feed.entries[: feed_definition.max_backfill_size]
        else:
            entries = feed.entries

        partition_keys = [sanitize(entry.id) for entry in entries]

        return dg.SensorResult(
            run_requests=[dg.RunRequest(partition_key=key) for key in partition_keys],
            dynamic_partitions_requests=[
                podcast_partitions_def.build_add_request(partition_keys)
            ],
            cursor=feed.etag,
        )

    return dg.Definitions(assets=[_podcast_metadata], jobs=[_job], sensors=[_sensor])


feeds = [
    RSSFeedDefinition(
        name="practical_ai",
        url="https://changelog.com/practicalai/feed",
    )
]

pipeline_definitions = [rss_pipeline_factory(feed) for feed in feeds]

defs = dg.Definitions.merge(*pipeline_definitions)
