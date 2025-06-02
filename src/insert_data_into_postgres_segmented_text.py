import psycopg
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Path to your JSON file
json_file_path = "data/segmented_text_transcriptions.json"

# Load JSON data from file
with open(json_file_path, "r", encoding="utf-8") as f:
    data = json.load(f)


expected_keys = ["id", "segmented_text"]


print("\ndata loaded")

dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

with psycopg.connect(dsn) as conn:
    print("\nconnection made")
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
            # Ensure all keys exist in the row dict
            for key in expected_keys:
                if key not in row:
                    row[key] = None  # or some appropriate default

            # Flatten the segmented_text field into individual rows
            if "segmented_text" in row and row["segmented_text"]:
                # Loop through each segment and insert as a row
                for index, segment in enumerate(row["segmented_text"], start=1):
                    segment_data = {
                        "id": row[
                            "id"
                        ],  # Assuming the `id` is the same for all segments in a row
                        "segment_index": index,
                        "text": segment.get("text", None),
                        "start_time": segment.get("start", None),
                        "end_time": segment.get("end", None),
                    }
                    cur.execute(insert_query, segment_data)
                    print(f"Segment {index} for {row.get('id')} inserted or updated")

        conn.commit()
        print(f"\n{len(data)} segments inserted or updated successfully.")
