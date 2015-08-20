DROP VIEW IF EXISTS dashboard_runs;
DROP VIEW IF EXISTS dashboard_stats;

DROP TABLE IF EXISTS runs;
DROP TABLE IF EXISTS sets;
DROP TABLE IF EXISTS jobs;
DROP SEQUENCE IF EXISTS ints;

CREATE SEQUENCE ints;

CREATE TABLE jobs
(
    id                  VARCHAR(40) PRIMARY KEY,
    status              BOOLEAN,
    task_files          JSON,
    file_states         JSON,
    file_results        JSON,
    github_status_url   TEXT,
    sequence            INTEGER NULL DEFAULT NEXTVAL('ints')
);

CREATE INDEX jobs_sequence_reverse ON jobs (sequence DESC);

CREATE TABLE sets
(
    id                  INTEGER NOT NULL DEFAULT NEXTVAL('ints') PRIMARY KEY,
    owner               TEXT,
    repository          TEXT,
    commit_sha          VARCHAR(40) NULL,
    datetime_start      TIMESTAMP WITH TIME ZONE,
    datetime_end        TIMESTAMP WITH TIME ZONE,
    
    render_world        TEXT,
    render_europe       TEXT,
    render_usa          TEXT
);

CREATE TABLE runs
(
    id                  INTEGER NOT NULL DEFAULT NEXTVAL('ints') PRIMARY KEY,
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
    set_id              INTEGER REFERENCES sets(id) NULL,
    commit_sha          VARCHAR(40) NULL
);

CREATE INDEX runs_set_ids ON runs (set_id);
CREATE INDEX runs_source_paths ON runs (source_path);

--
-- Two views mimicking Nelson's dashboard tables that were
-- previously populated by scraping from data.openaddresses.io.
--

CREATE VIEW dashboard_runs AS
    SELECT round(extract(epoch from datetime_start)::numeric, 3)::text AS tsname
    FROM sets;

GRANT SELECT ON dashboard_runs TO dashboard;

CREATE VIEW dashboard_stats AS
    SELECT round(extract(epoch from s.datetime_start)::numeric, 3)::text AS tsname,
           r.source_path AS source,
           r.state->>'version' AS version,
           extract(epoch from (r.state->>'process time')::interval) AS process_time,
           extract(epoch from (r.state->>'cache time')::interval) AS cache_time,
           (r.state->>'address count')::integer AS address_count,
           r.state->>'geometry type' AS geometry_type,
           r.state->>'processed' AS processed_url,
           r.state->>'cached' AS cache_url,
           r.state->>'sample' AS sample_url,
           r.state->>'output' AS output_url,
           r.state->>'fingerprint' AS fingerprint
    FROM runs AS r
    LEFT JOIN sets AS s ON s.id = r.set_id
    WHERE r.set_id IS NOT NULL
      AND s.datetime_end IS NOT NULL
      AND r.state::text != 'null';

GRANT SELECT ON dashboard_stats TO dashboard;
