"""
download_audio.py
=================

This script downloads podcast episodes as MP3 audio files from URLs listed in
a episode-level metadata file, `data/episode_metadata.json`. These audio files
are saved into a separate directory, `episode_audio`.

Usage
-----

To execute this script, run:
    python3 src/download_audio.py

"""

import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import requests

# Thread count for downloading audio in parallel
MAX_WORKERS = 10


def load_metadata_from_json(path: str) -> List[Dict[str, Any]]:
    """Loads episode metadata from a JSON into a list of dicts.

    Parameters
    ----------
    path : str
        The path to the JSON metadata file.

    Returns
    -------
    episode_metadata : list of dict
        A list of dictionaries, each representing metadata for an episode
        published in the `target_year` (defined in extract_metadata.py).
    """
    with open(path, "r", encoding="utf-8") as f:
        episode_metadata = json.load(f)
        return episode_metadata


def download_audio(episode: Dict[str, Any], download_dir: str) -> str:
    """Downloads the MP3 for a single podcast episode.

    Starts out by using the requests library to retrieve audio. If that fails,
    the function uses curl as a fallback.

    Parameters
    ----------
    episode : dict
        A dictionary containing metadata for a single episode published in the
        `target_year` (defined in extract_metadata.py).
    download_dir : str
        The directory where the downloaded MP3 file should be saved.
    """

    episode_id = episode.get("id")

    # Get the audio URL from links (if available)
    links = episode.get("links", [])
    audio_url = None

    for link in links:
        if link.get("type") == "audio/mpeg":
            audio_url = link.get("href")
            break

    if audio_url:
        filepath = os.path.join(download_dir, f"{episode_id}.mp3")

        # Don't re-download an existing MP3
        if os.path.exists(filepath):
            return f"Skipping existing file: {filepath}"

        try:
            # Attempt to download audio using the requests library
            response = requests.get(audio_url, stream=True, timeout=30)
            # Check for 200 status code
            response.raise_for_status()

            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return f"Saved to: {filepath}\n"

        except requests.RequestException as e:
            # If that fails, use curl as a fallback
            try:
                print(f"requests failed: {e}. Trying with curl for id: {episode_id}")

                subprocess.run(
                    ["curl", "-L", "-k", "-o", filepath, audio_url], check=True
                )
                return f"Saved with curl to: {filepath}\n"

            except subprocess.CalledProcessError as curl_err:
                return f"Failed to download {episode_id} with both requests and curl: {curl_err}\n"

    else:
        return f"No audio found for episode id: {episode_id}\n"


def download_audio_parallel(episode_metadata: List[Dict[str, Any]]):
    """Downloads podcast episodes in parallel using threads.

    Parameters
    ----------
    episode_metadata : list of dict
        A list of dictionaries, each representing metadata for an episode
        published in the `target_year` (defined in extract_metadata.py).
    """
    # Define a directory to save MP3s into
    download_dir = "episode_audio"
    os.makedirs(download_dir, exist_ok=True)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(download_audio, episode, download_dir)
            for episode in episode_metadata
        ]

        for future in as_completed(futures):
            print(future.result())


def main():
    # Deserialize JSON to a list
    print("\nLoading episode metadata from JSON...")
    episode_metadata = load_metadata_from_json("data/episode_metadata.json")

    # Download audio files in parallel to a directory
    print("\nDownloading MP3 files...\n")
    download_audio_parallel(episode_metadata)

    print("\nAudio downloads complete!")


if __name__ == "__main__":
    main()
