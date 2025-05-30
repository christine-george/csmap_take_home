from concurrent.futures import ProcessPoolExecutor
import os
import whisperx

# Load WhisperX model (using GPU)
model = whisperx.load_model("tiny", device="cuda")

# Function to transcribe a single audio file
def transcribe_audio(filename):
    audio_path = os.path.join("path/to/audio_files", filename)
    print(f"Processing {filename}...")

    result = model.transcribe(audio_path, batch_size=10)

    # Save result to a file
    output_path = os.path.join("path/to/output", filename + ".txt")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(result["text"])

    print(f"Finished {filename}")
    return filename

# Test different number of workers (based on CPU cores)
def transcribe_parallel(num_workers):
    files = [f for f in os.listdir("path/to/audio_files") if f.endswith(".mp3")]

    # Use ProcessPoolExecutor to parallelize transcription
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(transcribe_audio, filename) for filename in files]
        for future in futures:
            print(f"Processed: {future.result()}")

# Test with different number of parallel workers
if __name__ == "__main__":
    num_workers = 8  # Start with 8 workers (equal to your CPU cores)
    transcribe_parallel(num_workers)
