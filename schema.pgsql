DROP TABLE IF EXISTS jobs;

CREATE TABLE jobs
(
    id                  VARCHAR(40) PRIMARY KEY,
    task_files          JSON,
    file_states         JSON,
    file_results        JSON,
    github_status_url   TEXT
);
