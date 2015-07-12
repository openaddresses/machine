DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS sets;
DROP TABLE IF EXISTS jobs;

CREATE TABLE jobs
(
    id                  VARCHAR(40) PRIMARY KEY,
    status              BOOLEAN,
    task_files          JSON,
    file_states         JSON,
    file_results        JSON,
    github_status_url   TEXT
);

CREATE TABLE sets
(
    id                  SERIAL PRIMARY KEY,
    commit_sha          VARCHAR(40) NULL,
    datetime_start      TIMESTAMP WITH TIME ZONE,
    datetime_end        TIMESTAMP WITH TIME ZONE
);

CREATE TABLE runs
(
    id                  SERIAL PRIMARY KEY,
    source_path         TEXT,
    source_id           VARCHAR(40),
    source_data         BYTEA,
    datetime_tz         TIMESTAMP WITH TIME ZONE,
    -- commit_id?
    state               JSON,
    status              BOOLEAN,
    copy_of             INTEGER REFERENCES runs(id) NULL,

    code_version        VARCHAR(8) NULL,
    worker_id           VARCHAR(16) NULL,
    job_id              VARCHAR(40) REFERENCES jobs(id) NULL,
    commit_sha          VARCHAR(40) NULL
);
