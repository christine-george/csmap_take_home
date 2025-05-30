"""
transcribe_audio.py
===================

This script creates text transcripts of podcast episodes in the form of MP3
files.

Usage
-----

To execute this script, run:
    python3 src/transcribe_audio.py

"""
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import whisperx

AUDIO_DIR = "episode_audio2"
OUTPUT_DIR = "episode_transcripts/"
MODEL_SIZE = "small"
MAX_WORKERS = 3
DEVICE = "cpu"

# Global variable for model
model = None


def init_worker():
    global model
    model = whisperx.load_model(MODEL_SIZE, DEVICE, compute_type="float32")


def transcribe_file(filename: str) -> str:
    global model

    audio_path = os.path.join(AUDIO_DIR, filename)
    print(f"Transcribing {filename}...")

    result = model.transcribe(audio_path, language="en")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, filename + ".txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(result["text"])

    print(f"Finished {filename}")
    return filename


def transcribe_parallel(filenames):
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, initializer=init_worker) as executor:
        futures = [executor.submit(transcribe_file, fn) for fn in filenames]

        for future in as_completed(futures):
            print(future.result())


def main():
    files = [
        f for f in os.listdir(AUDIO_DIR)
        if f.lower().endswith(".mp3")
    ]
    print("Transcribing audio...")
    transcribe_parallel(files)
    print("All transcriptions completed.")


if __name__ == "__main__":
    main()
