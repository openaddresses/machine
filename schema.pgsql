DROP TABLE IF EXISTS jobs;

CREATE TABLE jobs
(
    id                  VARCHAR(40) PRIMARY KEY,
    task_files          JSON,
    file_states         JSON,
    github_status_url   TEXT
);
