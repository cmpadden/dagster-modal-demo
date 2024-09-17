import json
import os
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import dagster as dg
import feedparser
import yagmail
from botocore.exceptions import ClientError
from dagster_aws.s3 import S3Resource
from dagster_openai import OpenAIResource

from dagster_modal_demo.dagster_modal.resources import ModalClient
from dagster_modal_demo.utils.summarize import summarize

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
    max_backfill_size: int = 1


def _destination(partition_key: str) -> str:
    return DATA_PATH + os.sep + partition_key + ".mp3"


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
        """Podcast audio file download from RSS feed and uploaded to R2."""
        context.log.info("downloading audio file %s", config.audio_file_url)
        audio_key = _destination(context.partition_key)

        metadata = {}

        if _object_exists(s3.get_client(), bucket=R2_BUCKET_NAME, key=audio_key):
            context.log.info("audio file already exists... skipping")
            metadata["status"] = "cached"
            metadata["key"] = audio_key
        else:
            bytes = download_bytes(config.audio_file_url)
            s3.get_client().put_object(Body=bytes, Bucket=R2_BUCKET_NAME, Key=audio_key)
            metadata["status"] = "uploaded"
            metadata["size"] = file_size(len(bytes))

        return dg.MaterializeResult(metadata=metadata)

    @dg.asset(
        name=f"{feed_definition.name}_transcript",
        partitions_def=rss_entry_partition,
        deps=[_podcast_audio],
        compute_kind="modal",
    )
    def _podcast_transcription(
        context: dg.AssetExecutionContext, modal: ModalClient, s3: S3Resource
    ) -> dg.MaterializeResult:
        """Podcast transcription using OpenAI's Whipser model on Modal."""
        context.log.info("transcript %s", context.partition_key)
        audio_key = _destination(context.partition_key)
        transcription_key = audio_key.replace(".mp3", ".json")

        if _object_exists(
            s3.get_client(), bucket=R2_BUCKET_NAME, key=transcription_key
        ):
            return dg.MaterializeResult(metadata={"status": "cached"})

        # TODO - skip transcription if exists

        vars = [
            "CLOUDFLARE_R2_API",
            "CLOUDFLARE_R2_ACCESS_KEY_ID",
            "CLOUDFLARE_R2_SECRET_ACCESS_KEY",
        ]
        env = {k: v for k, v in os.environ.items() if k in vars}

        return modal.run(
            func_ref="modal_project.transcribe",
            context=context,
            env=env,
            extras={"audio_file_path": audio_key},
        ).get_materialize_result()

    @dg.asset(
        name=f"{feed_definition.name}_summary",
        partitions_def=rss_entry_partition,
        deps=[_podcast_transcription],
        compute_kind="openai",
    )
    def _podcast_summary(
        context: dg.AssetExecutionContext, s3: S3Resource, openai: OpenAIResource
    ) -> dg.MaterializeResult:
        audio_key = _destination(context.partition_key)
        transcription_key = audio_key.replace(".mp3", ".json")
        summary_key = audio_key.replace(".mp3", "-summary.txt")

        if _object_exists(s3.get_client(), bucket=R2_BUCKET_NAME, key=summary_key):
            return dg.MaterializeResult(
                metadata={"summary": "cached", "summary_key": summary_key}
            )

        response = s3.get_client().get_object(
            Bucket=R2_BUCKET_NAME, Key=transcription_key
        )

        data = json.loads(response.get("Body").read())

        with openai.get_client(context) as client:
            summary = summarize(client, data.get("text"))

        s3.get_client().put_object(
            Body=summary.encode("utf-8"), Bucket=R2_BUCKET_NAME, Key=summary_key
        )
        return dg.MaterializeResult(
            metadata={"summary": summary, "summary_key": summary_key}
        )

    @dg.asset(
        name=f"{feed_definition.name}_email",
        partitions_def=rss_entry_partition,
        deps=[_podcast_summary],
        compute_kind="python",
    )
    def _podcast_email(
        context: dg.AssetExecutionContext, s3: S3Resource
    ) -> dg.MaterializeResult:
        audio_key = _destination(context.partition_key)
        summary_key = audio_key.replace(".mp3", "-summary.txt")

        context.log.info("Reading summary %s", summary_key)
        response = s3.get_client().get_object(Bucket=R2_BUCKET_NAME, Key=summary_key)
        summary = response.get("Body").read().decode("utf-8")

        # Expects an application password (see: https://myaccount.google.com/apppasswords)
        yag = yagmail.SMTP(
            os.environ.get("GMAIL_USER"), os.environ.get("GMAIL_APP_PASSWORD")
        )

        recipient = os.environ.get("SUMMARY_RECIPIENT_EMAIL")

        yag.send(
            to=recipient,
            subject=f"Podcast Summary: {context.partition_key}",
            contents=f"""
               <h1>Podcaset Summary</h1>
               <h2>{context.partition_key}</h2>
               <div>{summary}</div>
            """,
        )

        return dg.MaterializeResult(
            metadata={
                "summary": summary,
                "summary_key": summary_key,
                "recipient": recipient,
            }
        )

    job_name = f"{feed_definition.name}_job"
    _job = dg.define_asset_job(
        name=job_name,
        selection=dg.AssetSelection.assets(
            _podcast_audio, _podcast_transcription, _podcast_summary, _podcast_email
        ),
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
        assets=[
            _podcast_audio,
            _podcast_transcription,
            _podcast_summary,
            _podcast_email,
        ],
        jobs=[_job],
        sensors=[_sensor],
        resources={
            "s3": s3_resource,
            "modal": ModalClient(project_directory=Path(__file__).parent.parent),
            "openai": OpenAIResource(api_key=dg.EnvVar("OPENAI_API_KEY")),
        },
    )


feeds = [
    RSSFeedDefinition(
        name="practical_ai",
        url="https://changelog.com/practicalai/feed",
    )
]

pipeline_definitions = [rss_pipeline_factory(feed) for feed in feeds]

defs = dg.Definitions.merge(*pipeline_definitions)
