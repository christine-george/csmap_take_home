import json
import os

from src.utils import save_data_to_json


def test_save_metadata_to_json(tmp_path):
    """Test that data in list form properly loads into a CSV."""
    data = [{"id": 123, "title": "Test"}]
    filepath = tmp_path / "data.json"

    save_data_to_json(data, str(filepath))

    assert os.path.exists(filepath)
    with open(filepath, "r") as f:
        loaded = json.load(f)
    assert loaded == data
