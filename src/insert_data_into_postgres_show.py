import psycopg
from dotenv import load_dotenv
import os
import json

load_dotenv()

# Path to your JSON file
json_file_path = "data/show_metadata.json"

# Load JSON data from file
with open(json_file_path, "r", encoding="utf-8") as f:
    data = json.load(f)


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
            prepared_row = prepare_json_fields(row)

            # Ensure all keys exist in prepared_show dict
            for key in expected_keys:
                if key not in prepared_row:
                    prepared_row[key] = None  # or some appropriate default

            for key, val in prepared_row.items():
                print(key, type(val))

            cur.execute(insert_query, prepared_row)
            print(f"\n{prepared_row.get("title")} inserted or updated")
        conn.commit()
        print(f"\n{len(data)} shows inserted or updated successfully.")
