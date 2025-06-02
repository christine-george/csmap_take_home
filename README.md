# CSMaP Take Home Assignment

## Background

### High-level Goal

This data pipelines scrapes podcast episode- and show-level metadata from RSS feeds, downloads
the relevant episode audio, transcribes the audio to text, and writes the metadata and text to
a GCP-hosted PostgresSQL database.

These tables can then be used to query mentions of "Trump" or "Biden" around the 2024 general election date.
You can use the query [found here](/query.sql) to do so.

### Assumptions

I established a few assumptions at the beginning to establish an idea of the project scope and target audience:

* This pipeline is meant for an audience who is comfortable with Python and SQL. Leave good comments and docstrings,
but don't overexplain every line in the code.
* This pipeline runs on manual script triggers rather than automated orchestration like Airflow or a cronjob.
* Raw data does not need to be stored in a data lake--the final table output is all that's necessary. Also,
the MP3 files don't need to be stored in GitHub to avoid bloating the repo.

## Data Pipeline Architecture

![Data pipeline architecture](/docs/arch_diagram.png)

### Architecture Walkthrough + Design Decisions

#### Extract the metadata

The steps in [`extract_metadata.py`](src/extract_metadata.py) are:

1. Grab the path to the RSS CSV and filtering year from the command line.
2. Read the URLs in the CSV into a list.
3. Loop through the URL list to grab show- and episode-level metadata. Keep
a running record of these two lists, one for show metadata and one for episode metadata.
4. Once all the URLs are looped through, write the final lists to JSON files in the [`data`](/data/)
directory: [`show_metadata.json`](data/show_metadata.json) and [`episode_metadata.json`](data/episode_metadata.json).

Design decisions were:

1. Keeping the path and year flexible from the command line avoids hardcoding values into the script.
Since the first script is the only one that needs a user to provide inputs, I made this one flexible and
kept the rest less customizable since they all cascade from the first script.
2. I kept all of the shows metadata together and all of the episodes metadata together to make it easy
to create well-defined tables for the metadata.
3. Creating separate [`data`](/data/) and [`src`](/src/) directories in the repo will keep everything organized
and easier to find.

#### Download the audio

The steps in [`download_audio.py`](src/download_audio.py) are:

1. Load the episode metadata from [`episode_metadata.json`](data/episode_metadata.json) into the script.
2. For each episode, find the audio file URL and download it using the `requests` library. If this fails,
use curl as a fallback method.
3. Save these MP3s into a separate directory, `episode_audio/`.
4. Parallelize steps 2 and 3 using a thread pool.

Design decisions were:

1. I saved the MP3 files by ID rather than title--I started with title, but caught one instance where
two podcast episodes had the same exact title, but differing metadata otherwise ("Connie Chung Regrets
Being a Good Girl"). Since I had logic to skip downloading a file if the name is already present in the
directory, I used ID since each episode was guaranteed to have a unique one.
2. I used curl with the `-k` flag as a fallback since the requests library had issues with one domain name
(www.stitcher.com) since the SSL certificate had expired. The episode wasn't hosted there, but it was one
of the redirect URLs. If it were a production project or sensitive data, I wouldn't do that, but I
explored the audio manually beforehand and made sure it was safe.
3. Parallelizing the downloads made it a lot faster to grab the audio rather than having to wait for each I/O
operation sequentially.

#### Transcribe the audio

The steps in [`transcribe_audio.py`](src/transcribe_audio.py) are:

1. Read in [`full_text_transcriptions.json`](data/full_text_transcriptions.json) and
[`segmented_text_transcriptions.json`](data/segmented_text_transcriptions.json) to see what's already been
transcribed.
2. With a parallel process pool, initialize each worker with the WhisperX transcription model.
3. For each audio file in the `episode_audio/` directory, call the model's `.transcribe()` function to
get the text transcript.
4. Using the output, create two dictionaries: one mapping the ID to the full text version of the transcript,
and another mapping the ID to the segmented text version of the transcript. For each completed audio file,
append each dict to a running list of segmented and full text dictionaries.
5. Once all audio files have been transcribed, write the running list of dictionaries back into two JSONs in the
[`data`](/data/) directory, [`full_text_transcriptions.json`](data/full_text_transcriptions.json) and
[`segmented_text_transcriptions.json`](data/segmented_text_transcriptions.json).

Design decisions were:

1. I initially attempted to transcribe all of the audio files, but was limited by my laptop's hardware in terms
of the time it would take to complete. As a tradeoff, I only transcribed a portion of the audio files to make sure
that I had successful output that I could write to a database. To maximize the number of audio files I could transcribe
in a reasonable amount of time, I did it for any file that was 50MB and under, so 331 files.
2. Similiarly, I used the `tiny` model, `int8` compute type, 4 process workers, and `cpu` as the device to match what
my laptop could handle and to try to maximize the number of files I could transcribe.
3. I parallelized the transcription to speed up the process and to maximize the CPU capacity I had.
4. I mapped the transcription outputs to episode ID to make it easy to have a join condition later on when
this data was stored in a Postgres table.

#### Write the data to Postgres

Before writing to Postgres, I created four tables for all the data. The tables (and their linked DDLs) are:

* [`csmap.information.episode`](ddl/episode.ddl)
* [`csmap.information.show`](ddl/show.ddl)
* [`csmap.transcript.full`](ddl/full.ddl)
* [`csmap.transcript.segmented`](ddl/segmented.ddl)

To write to each table, there were a few different scripts, one custom for each table:

* [`insert_data_into_postgres_episode.py`](/src/insert_data_into_postgres_episode.py)
* [`insert_data_into_postgres_show.py`](/src/insert_data_into_postgres_show.py)
* [`insert_data_into_postgres_full_text.py`](/src/insert_data_into_postgres_full_text.py)
* [`insert_data_into_postgres_segmented_text.py`](/src/insert_data_into_postgres_segmented_text.py)

Generally, each file had the same steps:

1. Read in the relevant JSON file for the table.
2. Create a connection to the Postgres table using `psycopg` and values defined in a .env file.
3. Create an insert query specific to the table and its columns.
4. If there are nested dictionaries, serialize them into JSON strings.
5. Make sure each row has all the necessary keys to insert. If there's no associated value in the
original data for the row, fill the row with `None`.
6. (Specifically for the segmented text table) Flatted the segments so that each one is its own row.
7. Insert or update the data into the Postgres table.

Design decisions were:

1. I tried generalizing these scripts into one to reduce code redundancy, but I ran into a lot of syntax issues
with the triply-quoted insert query. In the interest of time, I went with custom scripts for each table, but I kept
a record of my attempt to streamline in the [`exploration`](/exploration/) folder.
2. Keeping the metadata in a separate schema from the transcript data improves logical organization as well
as security and access control, if this were a production environment.
3. Aside from the segments dictionary, I kept the nested detail fields as JSONBs rather than flattening them
since the values were still easily accessible in that format.
4. I added a segment index column to make is easier to track the order of segments for an ID without having to
rely on start and end times.
5. In the interest of time, I didn't get around to adding unit tests for these scripts.

#### <u>Other</u>

Design decisions were:

1. I created a [`utils.py`](src/utils.py) to reduce redunancy and try to share code amongst scripts.
2. I added `pytest` unit tests in the [tests](/tests/) directory to give myself basic checks for code stability.
3. I used `black` and `isort` for code formatting, and I used numpy-style docstrings for code documentation.

## Future Considerations

* I would orchestrate this pipeline using a tool like Airflow to automatically trigger the sequence
of tasks. This could also be transformed into a batch pipeline that makes regular, incremental updates
to an existing table rather than a full refresh load on each run.
* If resource constraints weren't an issue, I'd store the MP3 files in a raw data layer since it's best
practice to save data at each step in the pipeline. Then, there would be a source of truth to work off of
if the end table is somehow corrupted. This would also open the door to more easily exploring Google's
Speech-to-Text API for transcription.
* I would streamline the scripts for inserting data into Postgres into one script rather than four.
* I would increase the flexibility of inputting the filtering year to be capable of handing a range of
dates or years on top of a single year filter.
* I would include more robust unit testing as well as data quality checks throughout the pipeline to make
sure data isn't being lost or corrupted.
* I would see how more common functions could be added to the [`utils`](src/utils.py) file to reduce
redundant code.
* **(Bonus)** With more time, I would use WhisperX's diarization capabilities to further categorize each
transcript segment by detected speaker.
