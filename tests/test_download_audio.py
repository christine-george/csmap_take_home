from src.download_audio import download_audio


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
