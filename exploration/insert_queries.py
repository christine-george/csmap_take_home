insert_values = {
    "data/episode_metadata.json": {
        "all_fields": [
            "id", "title", "link", "summary", "published", "itunes_episode",
            "itunes_episodetype", "itunes_duration", "author", "subtitle",
            "image", "title_detail", "summary_detail", "subtitle_detail",
            "author_detail", "published_parsed", "links", "authors", "content",
            "guidislink", "ppg_enclosurelegacy", "ppg_enclosuresecure",
            "media_content"
        ],
        "json_fields": [
            "title_detail", "links", "summary_detail", "published_parsed",
            "authors", "author_detail", "image", "subtitle_detail", "content",
            "ppg_enclosurelegacy", "ppg_enclosuresecure", "media_content"
        ],
        "insert_query":
"""
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
    },
    "data/show_metadata.json": {
        "all_fields": [],
        "json_fields": [],
        "insert_query": """
        INSERT INTO csmap.metadata.show (
            title, link, language, rights, subtitle,
            itunes_explicit, itunes_type, author,
            summary, content, links, title_detail, rights_detail,
            subtitle_detail, image, authors,
            author_detail, summary_detail, content_detail, publisher_detail,
            tags
        )
        VALUES (
            %(title)s, %(link)s, %(language)s, %(rights)s, %(subtitle)s,
            %(itunes_explicit)s, %(itunes_type)s, %(author)s,
            %(summary)s, %(content)s, %(links)s, %(title_detail)s,
            %(rights_detail)s,
            %(subtitle_detail)s, %(image)s, %(authors)s,
            %(author_detail)s, %(summary_detail)s, %(content_detail)s,
            %(publisher_detail)s,
            %(tags)s
        )
        ON CONFLICT (title) DO UPDATE SET
            title = EXCLUDED.title,
            link = EXCLUDED.link,
            language = EXCLUDED.language,
            rights = EXCLUDED.rights,
            subtitle = EXCLUDED.subtitle,
            itunes_explicit = EXCLUDED.itunes_explicit,
            itunes_type = EXCLUDED.itunes_type,
            author = EXCLUDED.author,
            summary = EXCLUDED.summary,
            content = EXCLUDED.content,
            links = EXCLUDED.links,
            title_detail = EXCLUDED.title_detail,
            rights_detail = EXCLUDED.rights_detail,
            subtitle_detail = EXCLUDED.subtitle_detail,
            image = EXCLUDED.image,
            authors = EXCLUDED.authors,
            author_detail = EXCLUDED.author_detail,
            summary_detail = EXCLUDED.summary_detail,
            content_detail = EXCLUDED.content_detail,
            publisher_detail = EXCLUDED.publisher_detail,
            tags = EXCLUDED.tags;
        """
    }
}
