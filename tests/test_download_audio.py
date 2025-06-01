import json

from src.download_audio import download_audio, load_metadata_from_json


def test_load_metadata_from_json(tmp_path):
    """Test that metadata in JSON form properly loads into a list."""
    # Create a test JSON file
    test_data = [
        {
            "id": "123",
            "links": [{"href": "http://example.com/audio.mp3"}],
        }
    ]
    filepath = tmp_path / "episode_metadata.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(test_data, f)

    test_metadata = load_metadata_from_json(str(filepath))

    assert isinstance(test_metadata, list)
    assert test_metadata[0]["id"] == "123"


def test_download_audio_no_links(tmp_path):
    """Test that the correct string is returned if there's no MP3 link."""
    episode = {"id": "123", "links": []}
    output = download_audio(episode, str(tmp_path))

    assert "No audio found" in output


def test_download_audio_existing_file(tmp_path):
    """Test that a file doesn't get redownloaded if it's already saved."""
    episode = {
        "id": "123",
        "links": [{"href": "http://example.com/audio.mp3", "type": "audio/mpeg"}],
    }
    filepath = tmp_path / "123.mp3"

    # Create a test existing file
    filepath.write_text("test")
    result = download_audio(episode, str(tmp_path))

    assert "Skipping existing file" in result
