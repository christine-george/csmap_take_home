"""
extract_metadata.py
===================

This script extracts show- and episode-level metadata from podcasts RSS URLs
listed in a separate CSV file. It only extracts this metadata for one specified
filtering year. After extraction, it writes all collected metadata to two JSON
files in a separate directory, `data/show_metadata.json` and
`data/episode_metadata.json`.

Usage
-----

To execute this script, run:
    python3 src/extract_metadata.py --path YOUR_CSV_PATH --year FILTER_YEAR

"""

import argparse
import csv
import json
from typing import Any, Dict, List, Tuple

import feedparser


def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments needed for RSS feed processing.

    Returns
    -------
    argparse.Namespace
        An object containing the CSV file path and the desired episode
        publication year.
    """
    parser = argparse.ArgumentParser()

    # Dynamically pass in CSV file paths and the year to filter by
    parser.add_argument(
        "--path",
        required=True,
        help="The path to the CSV file with RSS feed URLs.",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="The episode publication year to filter by.",
    )

    return parser.parse_args()


def load_rss_urls(path: str) -> List[str]:
    """Loads RSS URLs from a CSV into a list.

    Parameters
    ----------
    path : str
        The path to the CSV file.

    Returns
    -------
    list of str
        A list of all the RSS URLS contained in the CSV.

    Raises
    ------
    ValueError
        If the "rss_url" column is missing in the CSV.
    FileNotFoundError
        If the CSV does not exist.
    """
    try:
        with open(path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)

            if "rss_url" not in reader.fieldnames:
                raise ValueError("CSV does not contain a 'rss_url' column.")

            # Save valid RSS URLs
            rss_urls = []
            for row in reader:
                if row.get("rss_url"):
                    rss_urls.append(row["rss_url"])

            return rss_urls

    except FileNotFoundError as e:
        raise FileNotFoundError(f"CSV not found: {path}") from e


def extract_metadata(
    rss_url: str, target_year: int
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Extracts show- and episode-level metadata for a podcast.

    Parameters
    ----------
    rss_url : str
        The URL of the podcast RSS feed.
    target_year : int
        The episode publication year to filter by.

    Returns
    -------
    show_metadata : dict
        A dictionary containing the top-level metadata about the podcast.
    episode_metadata : list of dict
        A list of dictionaries, each representing metadata for an episode
        published in the `target_year`.
    """
    try:
        feed = feedparser.parse(rss_url)

        # Extract show metadata
        show_metadata = dict(feed.feed)

        # Extract episode metadata
        episode_metadata = []

        for entry in feed.entries:
            pub_date = entry.get("published_parsed")

            # Only save metadata for episodes published in the desired year
            if pub_date and pub_date.tm_year == target_year:
                # Convert the entry to a plain dict
                episode_data = dict(entry)
                episode_metadata.append(episode_data)

        return show_metadata, episode_metadata

    except Exception as e:
        print(f"Error while processing {rss_url}: {e}")
        return {}, []


def save_metadata_to_json(metadata: List[Dict[str, Any]], filename: str):
    """Serializes metadata into JSON to save into a file.

    Parameters
    ----------
    metadata : list of dict
        The metadata to serialize into JSON.
    filename : str
        The file to write the metadata to.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)


def main():
    # Parse CSV path and desired year from command line
    args = parse_arguments()

    # Save RSS URLs into a list
    print("\nLoading RSS URLs...")
    rss_urls = load_rss_urls(args.path)

    # Initialize lists to store metadata in
    all_show_metadata = []
    all_episode_metadata = []

    # Gather show and metadata for all RSS URLs; save them into their
    # respective lists
    for url in rss_urls:
        print(f"\nProcessing feed: {url}")
        show_metadata, episode_metadata = extract_metadata(url, target_year=args.year)
        all_show_metadata.append(show_metadata)
        all_episode_metadata.extend(episode_metadata)  # 782

    # Serialize the metadata lists to JSON and save to files
    print("\nSaving metadata to JSON...")
    save_metadata_to_json(all_show_metadata, "data/show_metadata.json")
    save_metadata_to_json(all_episode_metadata, "data/episode_metadata.json")

    print("\nMetadata extraction complete!")


if __name__ == "__main__":
    main()
