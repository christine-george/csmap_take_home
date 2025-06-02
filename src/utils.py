"""
utils.py
========

This script contains shared utility functions.

"""

import json
from typing import Any, Dict, List


def save_data_to_json(data: List[Dict[str, Any]], filename: str):
    """Serializes data into JSON to save into a file.

    Parameters
    ----------
    data : list of dict
        The data to serialize into JSON.
    filename : str
        The file name to write the data to.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
