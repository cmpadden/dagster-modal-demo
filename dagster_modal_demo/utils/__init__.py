import os
import re
import urllib.request
from typing import Optional

from botocore.exceptions import ClientError

from dagster_modal_demo.constants import BROWSER_USER_AGENT, DATA_PATH


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


def _object_exists(s3, bucket: str, key: str):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def _destination(partition_key: str) -> str:
    return DATA_PATH + os.sep + partition_key + ".mp3"
