DROP TABLE IF EXISTS jobs;

CREATE TABLE jobs
(
    id                  VARCHAR(40) PRIMARY KEY,
    filenames           TEXT ARRAY,
    github_status_url   TEXT,
    job_queue_url       TEXT
);
