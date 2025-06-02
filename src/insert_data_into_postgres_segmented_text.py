"""
insert_data_into_postgres_segmented_text.py
===========================================
This script loads episode metdata from
`data/segmented_text_transcriptions.json` into the GCP-hosted PostgreSQL
table, `csmap.transcript.segmented`.

Usage
-----

To execute this script, run:
    python3 src/insert_data_into_postgres_segmented_text.py

"""

import json
import os
from typing import Any, Dict, List

import psycopg
from dotenv import load_dotenv

# Path to the JSON file
json_file_path = "data/segmented_text_transcriptions.json"

# All the top-level keys in the data
expected_keys = ["id", "segmented_text"]


def write_to_postgres(dsn: str, data: List[Dict[str, Any]]):
    """Write each segmented transcript to Postgres.

    Parameters
    ----------
    dsn : str
        A formatted string containing variables to make the Postgres
        table connection.
    data : list of dict
        The segmented text data to write.
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            insert_query = """
            INSERT INTO csmap.transcript.segmented (
                id, segment_index, text, start_time, end_time
            )
            VALUES (
                %(id)s, %(segment_index)s, %(text)s, %(start_time)s, %(end_time)s
            )
            ON CONFLICT (id, segment_index) DO UPDATE SET
                text = EXCLUDED.text,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time;
            """

            for row in data:
                # Fill in missing keys in transcripts with None
                for key in expected_keys:
                    if key not in row:
                        row[key] = None

                # Flatten the segments into individual rows
                if "segmented_text" in row and row["segmented_text"]:
                    # Loop through each segment and insert as a row
                    for index, segment in enumerate(row["segmented_text"], start=1):
                        segment_data = {
                            "id": row["id"],
                            "segment_index": index,
                            "text": segment.get("text", None),
                            "start_time": segment.get("start", None),
                            "end_time": segment.get("end", None),
                        }

                        # Load the row for table insertion
                        cur.execute(insert_query, segment_data)
                        print(
                            f"Segment {index} for {row.get('id')} inserted or updated"
                        )

            conn.commit()


def main():
    # Load JSON data from file path
    with open(json_file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Structure connection variables for Postgres table (defined in .env)
    load_dotenv()
    dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

    # Write data to Postgres table
    print("\nWriting data to Postgres...")
    write_to_postgres(dsn, data)

    print(f"\n{len(data)} transcripts inserted or updated successfully.")


if __name__ == "__main__":
    main()
