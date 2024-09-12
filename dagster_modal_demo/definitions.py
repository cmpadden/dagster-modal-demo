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
    text = re.sub(r"[^a-zA-Z\d]", "_", feed.feed.title)
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


@dataclass
class RSSFeedDefinition:
    name: str
    url: str


def rss_sensor_factory(feed_definition: RSSFeedDefinition) -> dg.SensorDefinition:
    @dg.sensor(
        name=f"rss_sensor_{feed_definition.name}",
        minimum_interval_seconds=10 * 60,  # 10-minutes
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def _sensor(context: dg.SensorEvaluationContext):
        etag = context.cursor
        context.log.info("querying feed with cursor etag: %s", etag)
        feed = feedparser.parse(feed_definition.url, etag=etag)

        context.log.info("feed entries: %s", len(feed.entries))
        context.log.info("feed last updated: %s", getattr(feed, "modified", None))

        # TODO - launch run requests

        context.log.info("updating feed cursor etag: %s", feed.etag)
        context.update_cursor(feed.etag)

    return _sensor


feeds = [
    RSSFeedDefinition(
        name="practical_ai",
        url="https://changelog.com/practicalai/feed",
    )
]

feed_sensors = [rss_sensor_factory(feed) for feed in feeds]

defs = dg.Definitions(sensors=[*feed_sensors])
