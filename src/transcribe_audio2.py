import whisperx
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import torch

#AUDIO_DIR = "episode_audio2/"
#OUTPUT_DIR = "episode_transcripts/"
MODEL_SIZE = "tiny"

# Set batch size (how many chunks to process at a time)
BATCH_SIZE = 10

# Load WhisperX model

# Check if CUDA is available
if torch.cuda.is_available():
    DEVICE = "cuda"
    print("CUDA is available. Using GPU for transcription.")
else:
    DEVICE = "cpu"
    print("CUDA not available. Falling back to CPU.")

model = whisperx.load_model(MODEL_SIZE, DEVICE)


# Path to the audio file
audio_file = "episode_audio/0a132212-a4c2-4a52-895a-1be0a25a45c4.mp3.mp3"

# Run transcription with batch_size
result = model.transcribe(audio_file, batch_size=BATCH_SIZE)

# Output full text
print("Full Transcription:")
print(result['text'])

# Output timestamped transcription
print("\nTimestamped Transcription:")
for segment in result['segments']:
    print(f"Start: {segment['start']}s - End: {segment['end']}s")
    print(f"Text: {segment['text']}\n")

    