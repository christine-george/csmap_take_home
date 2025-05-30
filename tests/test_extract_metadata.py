import json
import os
import pytest
import csv
from src.extract_metadata import (
    load_rss_urls,
    save_metadata_to_json,
)


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


def test_save_metadata_to_json(tmp_path):
    """Test that metadata in list form properly loads into a CSV."""
    data = [{"id": 123, "title": "Test"}]
    filepath = tmp_path / "metadata.json"

    save_metadata_to_json(data, str(filepath))

    assert os.path.exists(filepath)
    with open(filepath, "r") as f:
        loaded = json.load(f)
    assert loaded == data


if __name__ == "__main__":
    pytest.main()
