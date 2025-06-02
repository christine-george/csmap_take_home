CREATE TABLE csmap.transcript.segmented (
    id TEXT,
    segment_index INT,
    text TEXT,
    start_time FLOAT8,
    end_time FLOAT8,
    PRIMARY KEY (id, segment_index)
);