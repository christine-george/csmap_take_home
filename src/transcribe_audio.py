"""
transcribe_audio.py
===================

This script transcribes the downloaded podcast episode MP3s and transcribes
them into text. This transcribed text is returned in two JSONs:
`data/full_text_transcriptions.json`, which contains the entire text of a
podcast episode in one string, and `data/segmented_text_transcriptions.json`,
whcih segments the entire text of the podcast episode into timestamped
sections.

Usage
-----

To execute this script, run:
    python3 src/transcribe_audio.py

"""

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple, Set
import src.utils as utils

# The path where the MP3s are
AUDIO_DIR = "episode_audio"
# Configuration variables for the transcription model
MODEL_SIZE = "tiny"
DEVICE = "cpu"
# Process count for transcribing audio in parallel
NUM_WORKERS = 4

# Initializing global transcription model
model = None


def init_worker():
    """Initializes the transcription model in each process pool worker."""
    global model
    import whisperx

    model = whisperx.load_model(MODEL_SIZE, DEVICE, compute_type="int8")


def read_in_json(path: str) -> List[Set[str]]:
    """Reads in the existing transcription data from JSON to a list.

    To avoid re-transcribing the same audio twice if it ran in two different
    script executions, this function looks at the episode ID of what's already
    been transcribed. If during transcription, this ID is encountered again, it
    will be skipped.

    Parameters
    ----------
    path : str
        The path where the transcript text JSON file lives.

    Returns
    -------
    tuple of list and set
        A tuple containing the deserialized JSON of existing transcription data
        and a set of unique episode IDs from the JSON.
    """
    if os.path.exists(path):
        data = utils.read_json_from_file(path)

        existing_ids = {item["id"] for item in data if "id" in item}
        print(f"\nLoaded {path} JSON.")

        return data, existing_ids

    return [], {}


def transcribe_audio(audio_file: str) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Transcribes an episode of podcast from MP3 to text.

    Parameters
    ----------
    audio_file : str
        The path to the audio file to transcribe.

    Returns
    -------
    tuple of str and dict
        The episode ID, full transcription text dictionary, and segmented text
        dictionary.

    """
    global model
    # Load the WhisperX transcription model and transcribe the audio
    # model = whisperx.load_model(MODEL_SIZE, DEVICE, compute_type="int8")
    transcription = model.transcribe(audio_file, language="en")

    # Extract the episode ID from the file name and the timestamped text
    # segments from the transcription model
    episode_id = os.path.basename(audio_file).replace(".mp3", "")
    segments = transcription["segments"]

    # Create two dictionaries, one with the full text mapped to the episode ID,
    # and one with the timestamped segments mapped to the episode ID
    full_text_dict = create_full_text_dict(segments, episode_id)
    segmented_text_dict = create_segmented_text_dict(segments, episode_id)

    return episode_id, full_text_dict, segmented_text_dict


def create_full_text_dict(
    segments: List[Dict[str, Any]], episode_id: str
) -> Dict[str, Any]:
    """Creates a dictionary with the full transcript in one string.

    Parameters
    ----------
    segments : list of dict
        Dictionaries containing sections of texts and their start and end
        timestamps.
    episode_id : str
        The unique episode ID from the RSS feed.

    Returns
    -------
    full_text_dict : dict
        A dictionary containing the episode ID and the full transcription as a
        single string.
    """
    # Since the segments dictionaries are loaded into the segment list in
    # order, loop through and join the text together
    full_transcription_text = "".join(segment["text"] for segment in segments)
    full_text_dict = {"id": episode_id, "full_text": full_transcription_text}

    return full_text_dict


def create_segmented_text_dict(
    segments: List[Dict[str, Any]], episode_id: str
) -> Dict[str, Any]:
    """Creates a dictionary with the transcript split into timestamped parts.

    Parameters
    ----------
    segments : list of dict
        Dictionaries containing sections of texts and their start and end
        timestamps.
    episode_id : str
        The unique episode ID from the RSS feed.

    Returns
    -------
    segmented_text_dict : dict
        A dictionary containing the episode ID and the original timestamped
        transcript dictionary.
    """
    # Map the segments dictionary directly to the associated episode ID
    segmented_text_dict = {"id": episode_id, "segmented_text": segments}

    return segmented_text_dict


def transcribe_audio_parallel(
    audio_dir: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Transcribes podcast episodes in parallel using using a process pool.

    Parameters
    ----------
    audio_dir : str
        The path containing all of the podcast episode MP3s.

    Returns
    -------
    tuple of dicts
        A tuple containg the dictionaries of full text transcription and
        segmented text transcription.
    """
    # Check if audio has already been transcribed to avoid rewrites
    full_text_dicts, existing_ids = read_in_json("data/full_text_transcriptions.json")
    segmented_text_dicts, existing_ids = read_in_json(
        "data/segmented_text_transcriptions.json"
    )

    # Filter out IDs that are already in the JSON
    audio_files = [
        os.path.join(audio_dir, f)
        for f in os.listdir(audio_dir)
        if f.endswith(".mp3") and os.path.splitext(f)[0] not in existing_ids
    ]

    with ProcessPoolExecutor(
        max_workers=NUM_WORKERS, initializer=init_worker
    ) as executor:
        future_to_file = {
            executor.submit(transcribe_audio, audio): audio for audio in audio_files
        }
        for future in as_completed(future_to_file):
            try:
                episode_id, full_text_dict, segmented_text_dict = future.result()

                # For each completed transcription, add them to the lists of
                # finished full and segmented text dictionaries
                full_text_dicts.append(full_text_dict)
                segmented_text_dicts.append(segmented_text_dict)

                print(f"Processed: {episode_id}")

            except Exception as e:
                print(f"Error processing file: {future_to_file[future]} - {e}")

    return full_text_dicts, segmented_text_dicts


def main():

    # Transcribe audio files in parallel
    print("\nStarting audio transcription...")
    full_text_dicts, segmented_text_dicts = transcribe_audio_parallel(AUDIO_DIR)

    print("\nSaving transcriptions to JSONs...")

    # Serialize final dictionaries to JSON and save files in the `data`
    # directory
    utils.save_data_to_json(full_text_dicts, "data/full_text_transcriptions.json")
    utils.save_metadata_to_json(
        segmented_text_dicts, "data/segmented_text_transcriptions.json"
    )

    print("\nTranscription complete!")


if __name__ == "__main__":
    main()
