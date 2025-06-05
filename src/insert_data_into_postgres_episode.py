"""
insert_data_into_postgres_episode.py
====================================

This script loads episode metdata from `data/episode_metadata.json` into the
GCP-hosted PostgreSQL table, `csmap.information.episode`.

Usage
-----

To execute this script, run:
    python3 src/insert_data_into_postgres_episode.py

"""

import json
import os
from typing import Any, Dict, List

import psycopg
from dotenv import load_dotenv

import utils as utils

# Path to the JSON file
json_file_path = "data/episode_metadata.json"

# All top-level keys in the data
expected_keys = [
    "id",
    "title",
    "link",
    "summary",
    "published",
    "itunes_episode",
    "itunes_episodetype",
    "itunes_duration",
    "author",
    "subtitle",
    "image",
    "title_detail",
    "summary_detail",
    "subtitle_detail",
    "author_detail",
    "published_parsed",
    "links",
    "authors",
    "content",
    "guidislink",
    "ppg_enclosurelegacy",
    "ppg_enclosuresecure",
    "ppg_canonical",
    "media_content",
]

# All JSONB fields in the data
json_fields = [
    "title_detail",
    "links",
    "summary_detail",
    "published_parsed",
    "authors",
    "author_detail",
    "image",
    "subtitle_detail",
    "content",
    "ppg_enclosurelegacy",
    "ppg_enclosuresecure",
    "media_content",
]


def prepare_json_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare nested dictionary fields into serialized JSON strings.

    Parameters
    ----------
    row : dict
        A dictionary for a single episode's data.

    Returns
    -------
    row : dict
        A dictionary for a single episode's data with nested dictionaries
        serialized into JSON strings.
    """
    for field in json_fields:
        if field in row and row[field] is not None:
            row[field] = json.dumps(row[field])
        else:
            row[field] = None
    return row


def write_to_postgres(dsn: str, data: List[Dict[str, Any]]):
    """Write each episode to Postgres.

    Parameters
    ----------
    dsn : str
        A formatted string containing variables to make the Postgres
        table connection.
    data : list of dict
        The episode data to write.
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO csmap.information.episode (
                id, title, title_detail, links, link, summary,
                summary_detail, published, published_parsed,
                itunes_episodetype, itunes_episode, authors,
                author, author_detail, image, subtitle, subtitle_detail,
                content, itunes_duration, guidislink, ppg_enclosurelegacy,
                ppg_enclosuresecure, ppg_canonical, media_content
            )
            VALUES (
                %(id)s, %(title)s, %(title_detail)s, %(links)s, %(link)s,
                %(summary)s, %(summary_detail)s, %(published)s,
                %(published_parsed)s, %(itunes_episodetype)s, %(itunes_episode)s,
                %(authors)s, %(author)s, %(author_detail)s, %(image)s,
                %(subtitle)s, %(subtitle_detail)s, %(content)s,
                %(itunes_duration)s, %(guidislink)s, %(ppg_enclosurelegacy)s,
                %(ppg_enclosuresecure)s, %(ppg_canonical)s, %(media_content)s
            )
            ON CONFLICT (id) DO UPDATE SET
                id = EXCLUDED.id,
                title = EXCLUDED.title,
                title_detail = EXCLUDED.title_detail,
                links = EXCLUDED.links,
                link = EXCLUDED.link,
                summary = EXCLUDED.summary,
                summary_detail = EXCLUDED.summary_detail,
                published = EXCLUDED.published,
                published_parsed = EXCLUDED.published_parsed,
                itunes_episodetype = EXCLUDED.itunes_episodetype,
                itunes_episode = EXCLUDED.itunes_episode,
                authors = EXCLUDED.authors,
                author = EXCLUDED.author,
                author_detail = EXCLUDED.author_detail,
                image = EXCLUDED.image,
                itunes_duration = EXCLUDED.itunes_duration,
                guidislink = EXCLUDED.guidislink,
                ppg_enclosurelegacy = EXCLUDED.ppg_enclosurelegacy,
                ppg_enclosuresecure = EXCLUDED.ppg_enclosuresecure,
                ppg_canonical = EXCLUDED.ppg_canonical,
                media_content = EXCLUDED.media_content;
            """

            for row in data:
                # Serialize all nested dictionaries
                prepared_row = prepare_json_fields(row)

                # Fill in missing keys in episodes with None
                for key in expected_keys:
                    if key not in prepared_row:
                        prepared_row[key] = None

                # Load the row for table insertion
                cur.execute(insert_query, prepared_row)
                print(f"\n{prepared_row.get("id")} inserted or updated")

            conn.commit()


def main():
    # Load JSON data from file path
    data = utils.read_data_from_json(json_file_path)

    # Structure connection variables for Postgres table (defined in .env)
    load_dotenv()
    dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

    # Write data to Postgres table
    print("\nWriting data to Postgres...")
    write_to_postgres(dsn, data)

    print(f"\n{len(data)} episodes inserted or updated successfully.")


if __name__ == "__main__":
    main()
