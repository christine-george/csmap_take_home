from src.transcribe_audio import (create_full_text_dict,
                                  create_segmented_text_dict)


def test_create_full_text_dict():
    """Test that text gets properly joined for the full text dictionary."""
    segments = [{"text": "hi "}, {"text": "friend!"}]
    result = create_full_text_dict(segments, "test_id")
    assert result == {"id": "test_id", "full_text": "hi friend!"}


def test_create_segmented_text_dict():
    """Test that the segmented text dictionary is properly made."""
    segments = [{"start": 0.0, "end": 1.0, "text": "hi friend!"}]
    result = create_segmented_text_dict(segments, "test_id")
    assert result == {"id": "test_id", "segmented_text": segments}
