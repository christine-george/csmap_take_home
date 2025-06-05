"""
insert_data_into_postgres_show.py
=================================
This script loads episode metdata from `data/show_metadata.json` into the
GCP-hosted PostgreSQL table, `csmap.information.show`.

Usage
-----

To execute this script, run:
    python3 src/insert_data_into_postgres_show.py

"""

import json
import os
from typing import Any, Dict, List

import psycopg
from dotenv import load_dotenv

import utils as utils

# Path to your the file
json_file_path = "data/show_metadata.json"

# All top-level keys in the data
expected_keys = [
    "title",
    "title_detail",
    "links",
    "link",
    "subtitle",
    "subtitle_detail",
    "rights",
    "rights_detail",
    "generator",
    "generator_detail",
    "language",
    "authors",
    "author",
    "author_detail",
    "itunes_block",
    "publisher_detail",
    "tags",
    "media_thumbnail",
    "href",
    "image",
    "itunes_type",
    "updated",
    "updated_parsed",
    "media_restriction",
    "restriction",
]

# All JSONB fields in the data
json_fields = [
    "title_detail",
    "links",
    "subtitle_detail",
    "rights_detail",
    "generator_detail",
    "authors",
    "author_detail",
    "publisher_detail",
    "tags",
    "media_thumbnail",
    "image",
    "updated_parsed",
    "media_restriction",
]


def prepare_json_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """Prepare nested dictionary fields into serialized JSON strings.

    Parameters
    ----------
    row : dict
        A dictionary for a single shows's data.

    Returns
    -------
    row : dict
        A dictionary for a single show's data with nested dictionaries
        serialized into JSON strings.
    """
    for field in json_fields:
        if field in row and row[field] is not None:
            row[field] = json.dumps(row[field])
        else:
            row[field] = None
    return row


def write_to_postgres(dsn: str, data: List[Dict[str, Any]]):
    """Write each show to Postgres.

    Parameters
    ----------
    dsn : str
        A formatted string containing variables to make the Postgres
        table connection.
    data : list of dict
        The show data to write.
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO csmap.information.show (
                title, title_detail, links, link, subtitle,
                subtitle_detail, rights, rights_detail,
                generator, generator_detail, language, authors,
                author, author_detail, itunes_block,
                publisher_detail, tags, media_thumbnail, href,
                image, itunes_type, updated, updated_parsed, media_restriction,
                restriction
            )
            VALUES (
                %(title)s, %(title_detail)s, %(links)s, %(link)s, %(subtitle)s,
                %(subtitle_detail)s, %(rights)s, %(rights_detail)s,
                %(generator)s, %(generator_detail)s, %(language)s, %(authors)s,
                %(author)s, %(author_detail)s, %(itunes_block)s,
                %(publisher_detail)s, %(tags)s, %(media_thumbnail)s, %(href)s,
                %(image)s, %(itunes_type)s, %(updated)s, %(updated_parsed)s,
                %(media_restriction)s, %(restriction)s
            )
            ON CONFLICT (title) DO UPDATE SET
                title = EXCLUDED.title,
                title_detail = EXCLUDED.title_detail,
                links = EXCLUDED.links,
                link = EXCLUDED.link,
                subtitle = EXCLUDED.subtitle,
                subtitle_detail = EXCLUDED.subtitle_detail,
                rights = EXCLUDED.rights,
                rights_detail = EXCLUDED.rights_detail,
                generator = EXCLUDED.generator,
                generator_detail = EXCLUDED.generator_detail,
                language = EXCLUDED.language,
                authors = EXCLUDED.authors,
                author = EXCLUDED.author,
                author_detail = EXCLUDED.author_detail,
                itunes_block = EXCLUDED.itunes_block,
                publisher_detail = EXCLUDED.publisher_detail,
                tags = EXCLUDED.tags,
                media_thumbnail = EXCLUDED.media_thumbnail,
                href = EXCLUDED.href,
                image = EXCLUDED.image,
                itunes_type = EXCLUDED.itunes_type,
                updated = EXCLUDED.updated,
                updated_parsed = EXCLUDED.updated_parsed,
                media_restriction = EXCLUDED.media_restriction,
                restriction = EXCLUDED.restriction;
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
                print(f"\n{prepared_row.get("title")} inserted or updated")

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

    print(f"\n{len(data)} shows inserted or updated successfully.")


if __name__ == "__main__":
    main()
