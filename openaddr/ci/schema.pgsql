DROP TABLE IF EXISTS runs;

CREATE TABLE runs
(
    id                  SERIAL PRIMARY KEY,
    source_path         TEXT,
    source_id           VARCHAR(40),
    source_data         BYTEA,
    datetime            TIMESTAMP,
    -- commit_id?
    state               JSON
);

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
