import psycopg
import json
from dotenv import load_dotenv
import os
from datetime import datetime
from exploration.insert_queries import insert_values

load_dotenv()


def prepare_json_fields(row, json_fields):
    for field in json_fields:
        if field in row and row[field] is not None:
            row[field] = json.dumps(row[field])
        else:
            row[field] = None
    return row


def prepare_row(row, all_fields, json_fields):
    # Convert published to datetime if exists
    if row.get("published"):
        row["published"] = datetime.strptime(row["published"], "%a, %d %b %Y %H:%M:%S %z")

    prepared_row = prepare_json_fields(row, json_fields)

    # Ensure all keys exist in prepared_show dict
    for key in all_fields:
        if key not in prepared_row:
            prepared_row[key] = None  # or some appropriate default

    for key, val in row.items():
        print(key, type(val))


def main():
    paths = ["data/episode_metadata.json"]
    dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

    for path in paths:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        insert_query = insert_values[path]["insert_query"]
        all_values = insert_values[path]["all_fields"]
        json_values = insert_values[path]["json_fields"]

    with psycopg.connect(dsn) as conn:
        print("\nconnection made")
        with conn.cursor() as cur:
            for row in data:
                prepared_row = prepare_row(row, all_values, json_values)

                cur.execute(insert_query, prepared_row)
                print(f"\n{row.get("title")} inserted or updated")
            conn.commit()
            print(f"\n{len(data)} shows inserted or updated successfully.")


if __name__ == "__main__":
    main()
