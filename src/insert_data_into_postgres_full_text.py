import psycopg
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Path to your JSON file
json_file_path = "data/full_text_transcriptions.json"

# Load JSON data from file
with open(json_file_path, "r", encoding="utf-8") as f:
    data = json.load(f)


expected_keys = ["id", "full_text"]


print("\ndata loaded")

dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

with psycopg.connect(dsn) as conn:
    print("\nconnection made")
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
            # Ensure all keys exist in prepared_show dict
            for key in expected_keys:
                if key not in row:
                    row[key] = None  # or some appropriate default

            for key, val in row.items():
                print(key, type(val))

            cur.execute(insert_query, row)
            print(f"\n{row.get("id")} inserted or updated")
        conn.commit()
        print(f"\n{len(data)} transcripts inserted or updated successfully.")
