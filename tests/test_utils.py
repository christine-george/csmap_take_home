import json
import os

from src.utils import save_data_to_json, read_data_from_json


def test_save_data_to_json(tmp_path):
    """Test that data in list form properly loads into a JSON."""
    data = [{"id": 123, "title": "Test"}]
    filepath = tmp_path / "data.json"

    save_data_to_json(data, str(filepath))

    assert os.path.exists(filepath)
    with open(filepath, "r") as f:
        loaded = json.load(f)
    assert loaded == data


def test_read_data_from_json(tmp_path):
    """Test that data is read into a list properly from JSON."""
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    filepath = tmp_path / "test.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f)

    result = read_data_from_json(str(filepath))

    assert result == data
