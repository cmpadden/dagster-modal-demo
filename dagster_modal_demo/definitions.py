import os
import re
import urllib.request
from dataclasses import dataclass
from typing import Optional

import dagster as dg
import feedparser
from botocore.exceptions import ClientError
from dagster_aws.s3 import S3Resource

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


def file_size(len_bytes, suffix="B") -> str:
    """Human-readable bytes size

    Args:
        len_bytes (int): number of bytes
        suffix (str): optional suffix

    Returns:
        String representation of bytes size

    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(len_bytes) < 1024.0:
            return "%3.1f%s%s" % (len_bytes, unit, suffix)
        len_bytes /= 1024.0
    return "%.1f%s%s" % (len_bytes, "Yi", suffix)


def _extract_audio_url(entry) -> str:
    """Extracts URL of audio file from RSS entry.

    Args:
        entry: metadata of RSS entry from `feedparser`

    Returns:
        URL of audio file

    """
    audio_hrefs = [
        link.get("href") for link in entry.links if link.get("type") == "audio/mpeg"
    ]
    if audio_hrefs:
        return audio_hrefs[0]
    else:
        raise Exception("No audio file present")


DEFAULT_POLLING_INTERVAL = 10 * 60  # 10 minutes
DATA_PATH = "data"

s3_resource = S3Resource(
    endpoint_url=dg.EnvVar("CLOUDFLARE_R2_API"),
    aws_access_key_id=dg.EnvVar("CLOUDFLARE_R2_ACCESS_KEY_ID"),
    aws_secret_access_key=dg.EnvVar("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
    region_name="auto",
)


def _object_exists(s3, bucket: str, key: str):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


R2_BUCKET_NAME = "dagster-modal-demo"


@dataclass
class RSSFeedDefinition:
    name: str
    url: str
    max_backfill_size: int = 3


def rss_pipeline_factory(feed_definition: RSSFeedDefinition) -> dg.Definitions:
    rss_entry_partition = dg.DynamicPartitionsDefinition(
        name=f"{feed_definition.name}_entry"
    )

    class AudioRunConfig(dg.Config):
        audio_file_url: str

    audio_asset_name = f"{feed_definition.name}_audio"

    @dg.asset(
        name=audio_asset_name,
        partitions_def=rss_entry_partition,
        compute_kind="python",
    )
    def _podcast_audio(
        context: dg.AssetExecutionContext, config: AudioRunConfig, s3: S3Resource
    ):
        context.log.info("downloading audio file %s", config.audio_file_url)
        destination = DATA_PATH + os.sep + context.partition_key + ".mp3"

        metadata = {}

        if _object_exists(s3.get_client(), bucket=R2_BUCKET_NAME, key=destination):
            context.log.info("audio file already exists... skipping")
            metadata["status"] = "cached"
        else:
            bytes = download_bytes(config.audio_file_url)
            s3.get_client().put_object(
                Body=bytes, Bucket=R2_BUCKET_NAME, Key=destination
            )
            metadata["status"] = "uploaded"
            metadata["size"] = file_size(len(bytes))

        return dg.MaterializeResult(metadata=metadata)

    @dg.asset(
        name=f"{feed_definition.name}_transcript",
        partitions_def=rss_entry_partition,
        deps=[_podcast_audio],
    )
    def _podcast_transcription(context: dg.AssetExecutionContext):
        context.log.info("transcript %s", context.partition_key)

    job_name = f"{feed_definition.name}_job"
    _job = dg.define_asset_job(
        name=job_name,
        selection=dg.AssetSelection.assets(_podcast_audio, _podcast_transcription),
        partitions_def=rss_entry_partition,
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

        partition_key_audio_files = [
            (sanitize(entry.id), _extract_audio_url(entry)) for entry in entries
        ]

        return dg.SensorResult(
            run_requests=[
                dg.RunRequest(
                    partition_key=partition_key,
                    run_config=dg.RunConfig(
                        ops={
                            audio_asset_name: AudioRunConfig(
                                audio_file_url=audio_file_url
                            )
                        }
                    ),
                )
                for (partition_key, audio_file_url) in partition_key_audio_files
            ],
            dynamic_partitions_requests=[
                rss_entry_partition.build_add_request(
                    [key for (key, _) in partition_key_audio_files]
                )
            ],
            cursor=feed.etag,
        )

    return dg.Definitions(
        assets=[_podcast_audio, _podcast_transcription],
        jobs=[_job],
        sensors=[_sensor],
        resources={"s3": s3_resource},
    )


feeds = [
    RSSFeedDefinition(
        name="practical_ai",
        url="https://changelog.com/practicalai/feed",
    )
]

pipeline_definitions = [rss_pipeline_factory(feed) for feed in feeds]

defs = dg.Definitions.merge(*pipeline_definitions)
