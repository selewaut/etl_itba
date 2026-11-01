CREATE TABLE IF NOT EXISTS Substance (
    SubstanceID serial NOT NULL PRIMARY KEY,
    Name        text NOT NULL UNIQUE,
    Unit        text NOT NULL,
    UNIQUE (Name)
);

-- guarda las corridas correctas
CREATE TABLE IF NOT EXISTS etl_run (
    etl_run_id  bigserial PRIMARY KEY,
    job_name    text NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status      text NOT NULL DEFAULT 'RUNNING'
);

-- guarda las corridas rechazadas
CREATE TABLE IF NOT EXISTS etl_rejects (
    etl_run_id  bigint NOT NULL REFERENCES etl_run (etl_run_id),
    stage       text NOT NULL,
    myid        integer,
    raw_payload jsonb,
    reason      text NOT NULL,
    ts          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_etl_rejects_run ON etl_rejects (etl_run_id);

INSERT INTO Substance (Name, Unit) VALUES ('Carbon monoxide', 'PPM') ON CONFLICT (Name) DO NOTHING;
