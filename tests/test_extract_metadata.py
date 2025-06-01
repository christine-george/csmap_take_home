import csv

import pytest

from src.extract_metadata import load_rss_urls


def test_load_rss_urls(tmp_path):
    """Test that a CSV of URLS gets properly turned into a list."""
    # Create a CSV file with rss_url column
    csv_path = tmp_path / "rss_urls.csv"

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rss_url"])
        writer.writerow(["http://example.com/feed"])

    urls = load_rss_urls(str(csv_path))

    assert urls == ["http://example.com/feed"]


def test_load_rss_urls_missing_column(tmp_path):
    """Test that an error is raised if the CSV isn't properly labeled."""
    csv_path = tmp_path / "rss_urls.csv"

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["not_rss_url"])
        writer.writerow(["not_a_feed_url"])

    with pytest.raises(ValueError):
        load_rss_urls(str(csv_path))
