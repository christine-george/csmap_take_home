"""
insert_data_into_postgres_full_text.py
======================================

This script loads episode metdata from `data/full_text_transcriptions.json`
into the GCP-hosted PostgreSQL table, `csmap.transcript.full`.

Usage
-----

To execute this script, run:
    python3 src/insert_data_into_postgres_full_text.py

"""

import os
from typing import Any, Dict, List

import psycopg
from dotenv import load_dotenv

import utils as utils

# Path to the JSON file
json_file_path = "data/full_text_transcriptions.json"

# All the top-level keys in the data
expected_keys = ["id", "full_text"]


def write_to_postgres(dsn: str, data: List[Dict[str, Any]]):
    """Write each full transcript to Postgres.

    Parameters
    ----------
    dsn : str
        A formatted string containing variables to make the Postgres
        table connection.
    data : list of dict
        The full text data to write.
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO csmap.transcript.full (
                id, full_text
            )
            VALUES (
                %(id)s, %(full_text)s
            )
            ON CONFLICT (id) DO UPDATE SET
                id = EXCLUDED.id,
                full_text = EXCLUDED.full_text;
            """

            for row in data:
                # Fill in missing keys in transcripts with None
                for key in expected_keys:
                    if key not in row:
                        row[key] = None

                # Load the row for table insertion
                cur.execute(insert_query, row)
                print(f"\n{row.get("id")} inserted or updated")

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

    print(f"\n{len(data)} transcripts inserted or updated successfully.")


if __name__ == "__main__":
    main()
