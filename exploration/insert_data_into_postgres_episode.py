import psycopg
import json
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()


# Path to your JSON file
json_file_path = "data/episode_metadata.json"

# Load JSON data from file
with open(json_file_path, "r", encoding="utf-8") as f:
    data = json.load(f)


all_keys = ["id", "title", "link", "summary", "published", "itunes_episode",
            "itunes_episodetype", "itunes_duration", "author", "subtitle",
            "image", "title_detail", "summary_detail", "subtitle_detail",
            "author_detail", "published_parsed", "links", "authors",
            "content", "guidislink", "ppg_enclosurelegacy",
            "ppg_enclosuresecure", "media_content"]

json_fields = ["title_detail", "links", "summary_detail", "published_parsed",
               "authors", "author_detail", "image", "subtitle_detail",
               "content", "ppg_enclosurelegacy", "ppg_enclosuresecure",
               "media_content"]


def prepare_json_fields(row):
    for field in json_fields:
        if field in row and row[field] is not None:
            row[field] = json.dumps(row[field])
        else:
            row[field] = None
    return row


dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        insert_query = """
        INSERT INTO csmap.metadata.episode (
            id, title, link, summary, published,
            itunes_episode, itunes_episodetype, itunes_duration,
            author, subtitle, image, title_detail, summary_detail,
            subtitle_detail, author_detail, published_parsed,
            links, authors, content, guidislink, ppg_enclosurelegacy,
            ppg_enclosuresecure, media_content
        )
        VALUES (
            %(id)s, %(title)s, %(link)s, %(summary)s, %(published)s,
            %(itunes_episode)s, %(itunes_episodetype)s, %(itunes_duration)s,
            %(author)s, %(subtitle)s, %(image)s, %(title_detail)s,
            %(summary_detail)s,
            %(subtitle_detail)s, %(author_detail)s, %(published_parsed)s,
            %(links)s, %(authors)s, %(content)s, %(guidislink)s,
            %(ppg_enclosurelegacy)s, %(ppg_enclosuresecure)s, %(media_content)s
        )
        ON CONFLICT (id) DO UPDATE SET
            title = EXCLUDED.title,
            link = EXCLUDED.link,
            summary = EXCLUDED.summary,
            published = EXCLUDED.published,
            itunes_episode = EXCLUDED.itunes_episode,
            itunes_episodetype = EXCLUDED.itunes_episodetype,
            itunes_duration = EXCLUDED.itunes_duration,
            author = EXCLUDED.author,
            subtitle = EXCLUDED.subtitle,
            image = EXCLUDED.image,
            title_detail = EXCLUDED.title_detail,
            summary_detail = EXCLUDED.summary_detail,
            subtitle_detail = EXCLUDED.subtitle_detail,
            author_detail = EXCLUDED.author_detail,
            published_parsed = EXCLUDED.published_parsed,
            links = EXCLUDED.links,
            authors = EXCLUDED.authors,
            content = EXCLUDED.content,
            guidislink = EXCLUDED.guidislink,
            ppg_enclosurelegacy = EXCLUDED.ppg_enclosurelegacy,
            ppg_enclosuresecure = EXCLUDED.ppg_enclosuresecure,
            media_content = EXCLUDED.media_content;
        """
        for row in data:
            # Convert published to datetime if exists
            if row.get("published"):
                row["published"] = datetime.strptime(row["published"], "%a, %d %b %Y %H:%M:%S %z")

            prepared_row = prepare_json_fields(row)

            # Ensure all keys exist in prepared_show dict
            for key in all_keys:
                if key not in prepared_row:
                    prepared_row[key] = None  # or some appropriate default

            for key, val in row.items():
                print(key, type(val))

            cur.execute(insert_query, prepared_row)
            print(f"\n{row.get("id")} inserted or updated")
        conn.commit()
        print(f"\n{len(data)} shows inserted or updated successfully.")
