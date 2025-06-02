import psycopg
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Path to your JSON file
json_file_path = "data/episode_metadata.json"

# Load JSON data from file
with open(json_file_path, "r", encoding="utf-8") as f:
    data = json.load(f)


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


def prepare_json_fields(row):
    for field in json_fields:
        if field in row and row[field] is not None:
            row[field] = json.dumps(row[field])
        else:
            row[field] = None
    return row


print("\ndata loaded")

dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"

with psycopg.connect(dsn) as conn:
    print("\nconnection made")
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
            prepared_row = prepare_json_fields(row)

            # Ensure all keys exist in prepared_show dict
            for key in expected_keys:
                if key not in prepared_row:
                    prepared_row[key] = None  # or some appropriate default

            for key, val in prepared_row.items():
                print(key, type(val))

            cur.execute(insert_query, prepared_row)
            print(f"\n{prepared_row.get("id")} inserted or updated")
        conn.commit()
        print(f"\n{len(data)} episodes inserted or updated successfully.")
