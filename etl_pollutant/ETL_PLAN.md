# ETL Plan — EPA Air Pollutants Hourly Dataset

**Source:** `hourly_42101_2012_1.zip` → `hourly_42101_2012_1.csv`
**Target DB:** PostgreSQL 18 — database `etl3`
**Source table:** `public.hourly_42101_2012_1` (raw, 1,000,000 rows, all `VARCHAR`)
**Target staging:** `public.hourly_staging` (typed, 999,999 rows)
**Tooling:** Pentaho Data Integration (Spoon) — `spoon.sh`
**Reference:** "Entregable 03 Santiago Elewaut.pdf"

---

## 0. Data Profile Summary (baseline)

| Fact | Value |
|---|---|
| Raw rows | 1,000,000 |
| Duplicate-header rows (row `myid=1`) | 1 (contains column-name strings) |
| Rows with ≥1 NULL (after header removal) | 349,217 |
| Latitude NULLs | 349,217 (same rows as longitude) |
| Longitude NULLs | 349,217 |
| Sample NULLs | 0 |
| Latitude range | 25.621667 … 64.84569 |
| Longitude range | -149.93493 … -72.631667 |
| Distinct `units` values | 4 (`Parts per million`, `parts  per million`, `PPM`, `Units of Measure`[header]) |
| Distinct `parametername` values | 6 (incl. typos: `Carbno monoxide`, `Cabron monoxide`, abbrev `CO`, spacing `Carbon  monoxide`) |
| Distinct `datum` values | WGS84 (409,441), NULL/empty (349,217), NAD83 (197,584), NAD27 (38,382), `UNKNOWN` (5,375) |
| `datelocal` conforms to `YYYY-MM-DD` | 100% (excl. header) |
| `timelocal` conforms to `HH:MM` | 100% (excl. header) |

---

## Step 0 — Schema Creation JOB (pre-ETL)

**Type:** Spoon **Job** (`.kjb`), not a transformation. Modeled after `etl_itba/CreateTables.kjb`.
**Purpose:** Create the `Substance` pollutant dimension, its auxiliaries, and the PSM helper used by the populate step — so downstream ETLs never fail on a missing object.

**Target DB:** OLAP (same as `CreateTables.kjb` — Substance is a DW dimension alongside `geolocation0-3`). The populate step reads normalized data from `etl3.hourly_staging` and writes here via the PSM (cross-DB is fine in Kettle with two connections).

### 0.1 Job structure (mirror of CreateTables.kjb)

```
Start ──▶ [Entry 1: SQL from file] polluant_schema.sql  ──▶ [Entry 2: inline SQL] inserta_substance PSM
```

| Entry | Type | Content | Connection |
|---|---|---|---|
| 1. `SQL CREATE TABLES` | SQL (from file) | DDL: `Substance` + auxiliaries + seeds → external file `pollutant_schema.sql` (keeps DDL version-controlled, same as `olap.sql`) | OLAP |
| 2. `create PSM` | SQL (inline) | `DO $$` guard for `rec_user_type` + `CREATE OR REPLACE FUNCTION inserta_substance(...)` | OLAP |

Use `rec_user_type` — **already exists in OLAP** (created by `CreateTables.kjb`), so the `DO $$ IF NOT EXISTS` guard is a no-op safety net. (It does NOT exist in `etl3`; if the populate ETL ever runs against `etl3` instead, this guard creates it there.)

### 0.2 Entry 1 — DDL (external file `pollutant_schema.sql`)

```sql
-- ===== Substance pollutant dimension =====
CREATE TABLE IF NOT EXISTS Substance (
    SubstanceID serial       NOT NULL PRIMARY KEY,  -- autonumerico
    Name        text         NOT NULL UNIQUE,       -- Ej: 'Sulfato de Nitrogeno'
    Unit        text         NOT NULL,              -- Ej: 'PPM'
    UNIQUE (Name)
);

-- ===== Auxiliaries required by the main ETL =====
CREATE TABLE IF NOT EXISTS etl_run (
    etl_run_id  bigserial    PRIMARY KEY,
    job_name    text         NOT NULL,
    started_at  timestamptz  NOT NULL DEFAULT now(),
    finished_at timestamptz,
    status      text         NOT NULL DEFAULT 'RUNNING'  -- RUNNING | SUCCESS | FAILED
);

CREATE TABLE IF NOT EXISTS etl_rejects (
    etl_run_id  bigint       NOT NULL REFERENCES etl_run (etl_run_id),
    stage       text         NOT NULL,   -- 'header' | 'latlon_format' | 'units' | 'datelocal' | 'timelocal' | 'parametername' | 'substance'
    myid        integer,
    raw_payload jsonb,
    reason      text         NOT NULL,
    ts          timestamptz  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_etl_rejects_run ON etl_rejects (etl_run_id);

INSERT INTO Substance (Name, Unit)
VALUES ('Carbon monoxide', 'PPM')
ON CONFLICT (Name) DO NOTHING;
```

> Note: trailing `UNIQUE (Name)` is redundant with the inline `UNIQUE` on `Name` — Postgres creates one unique index either way. Kept verbatim per spec; harmless.
>
> T3/T6 value mappings (units, parametername aliases) are **inline in the transformation** (`Value Mapper` steps) — no `dim_unit`/`dim_parameter` lookup tables.

### 0.3 Entry 2 — PSM `inserta_substance` (inline, mirrors `inserta_dimgeolocation`)

```sql
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'rec_user_type') THEN
    CREATE TYPE rec_user_type AS (id integer, message text);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION public.inserta_substance(
  aName text,
  aUnit text
)
RETURNS rec_user_type
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
  aSubstanceID integer;
  rec rec_user_type;
BEGIN
  SELECT SubstanceID INTO aSubstanceID
  FROM Substance
  WHERE Name = aName;

  IF NOT found THEN
    INSERT INTO Substance (Name, Unit)
    VALUES (aName, aUnit)
    RETURNING SubstanceID INTO aSubstanceID;
  END IF;

  rec.id := aSubstanceID;
  rec.message := NULL;
  RETURN rec;

EXCEPTION
  WHEN others THEN
    rec.id := NULL;
    rec.message := format('name=%s unit=%s: %s', aName, aUnit, SQLERRM);
    RETURN rec;
END
$BODY$;
```

Semantics (identical to `inserta_dimgeolocation`, plus enriched error context):
- Lookup by natural key (`Name`) → return existing `SubstanceID` (idempotent re-runs, no duplicates).
- Not found → insert, return new `SubstanceID`.
- Any exception → return `(NULL, error_message)` instead of failing the whole load — caller routes to the reject sink. The message embeds `name`/`unit` because the Kettle Table Input step's output is **only the SQL result columns** (no input pass-through — verified in `TableInput.java`: `putRow(data.rowMeta=getReturnRowMeta())`), so the reject row itself can't carry the offending values.

### 0.4 Why these auxiliaries + PSM (answer to "anything similar?")

| Object | Needed by | If missing |
|---|---|---|
| `Substance` | substance populate step (next ETL) | populate fails |
| `inserta_substance` PSM | substance populate step | populate must hand-roll upsert logic in Kettle (error-prone, no atomicity) |
| `etl_run` | every stage (audit `etl_run_id`) | no run auditability; FK on rejects broken |
| `etl_rejects` | T1–T6 + substance populate | invalid rows silently dropped |

**Recommendation — yes, mirror the PSM pattern.** The populate ETL becomes: read distinct `(Name, Unit)` pairs from normalized `hourly_staging` → call `SELECT * FROM inserta_substance(?, ?)` per row → log `rec.id`/`rec.message`. No manual upsert logic in the Spoon canvas, same battle-tested shape as `inserta_dimgeolocation`.

### 0.5 Spoon JOB implementation (`create_substance_schema.kjb`)

1. **Start** entry.
2. **Execute SQL script** — *SQL from file* = `pollutant_schema.sql`, connection **OLAP**, `sendOneStatement=N`.
3. **Execute SQL script** — inline SQL = the `inserta_substance` PSM block from 0.3, connection **OLAP**, `sendOneStatement=T`.
4. Hop: `Start → Entry 1 (unconditional) → Entry 2 (evaluation=success)`.
5. **Abort job on error** enabled (fail fast).

### 0.6 Step 0 acceptance criteria

1. `Substance` exists in OLAP with `SubstanceID serial PK`, `Name text UNIQUE NOT NULL`, `Unit text NOT NULL`; seeded with `('Carbon monoxide','PPM')`.
2. `inserta_substance('Carbon monoxide','PPM')` returns existing id; `inserta_substance('Nuevo Gas','PPM')` inserts and returns new id; error path returns `(NULL, msg)`.
3. `etl_run` and `etl_rejects` exist.
4. Re-running the JOB changes nothing (idempotent) and exits 0.

---

## Step 0.5 — Substance Populate ETL (`Substance01.ktr`) — ✅ DRAFT BUILT

**File:** `etl_pollutant/Substance01.ktr`
**Reference transformation:** `etl_itba/Geo08.ktr` (populates the geolocation dimension in the same DW) — the PSM is invoked from inside the transformation via a **Table Input** step with `execute_each_row=Y` and `<lookup>` naming the info step.

### Built flow (9 steps)

```
source substances (Table Input, etl3 — RAW table, 16 distinct pairs incl. header)
   → drop header (Filter rows: name <> 'Parameter Name')        [Geo08 in-canvas header pattern]
        ├─ true  → normalize unit (Janino, replace in place)
        │            → normalize name (Janino, replace in place)
        │            → inserta substance (Table Input, OLAP, ☑ execute_each_row, lookup=normalize name)
        │            → result ok? (Filter rows: message IS NULL)
        │                 ├─ true  → done (Dummy)
        │                 └─ false → psm rejects (Text file output → logs/substance_psm_rejects_*.csv)
        └─ false → header rejects (Text file output → logs/substance_header_rejects_*.csv)
```

| Step | Detail |
|---|---|
| **source substances** | `SELECT DISTINCT parametername AS name, units AS unit FROM "public".hourly_42101_2012_1 WHERE ... IS NOT NULL AND <> ''` — field order `name, unit` matters (positional `?` binding) |
| **drop header** | Filter rows (Geo08 pattern): `name <> 'Parameter Name'` → false → `header rejects`. Single header row, name alone identifies it |
| **header rejects** | Text file output, `logs/substance_header_rejects` + date — Geo08's reject-file approach (its `WHERE myid <> 1` is commented out; its Filter does the work) |
| **normalize unit** | Janino, replace `unit` in place: collapse whitespace + trim, then `parts per million`/`ppm` (case-insensitive) → `PPM`; unknown values kept as-is |
| **normalize name** | Janino, replace `name` in place: collapse + trim, then `carbon monoxide`/`co`/`carbno monoxide`/`cabron monoxide` → `Carbon monoxide`; unknown values kept as-is (future substances flow through) |
| **inserta substance** | `SELECT id, message FROM inserta_substance(?,?)`, `execute_each_row=Y`, `<lookup>normalize name</lookup>` (info step). `?` binds positionally: `name`→aName, `unit`→aUnit |
| **result ok?** | `message IS NULL` → `done`; else → `psm rejects` |
| **psm rejects** | Text file output (Geo08 pattern), `${Internal.Entry.Current.Directory}/logs/substance_psm_rejects` + date, `;`-separated, header |

### Design decisions / findings

1. **Source = RAW table** (`hourly_42101_2012_1`), not staging — matches Geo08, which sources raw and drops the header **in-canvas** (Filter rows → Text file output; its `WHERE myid <> 1` is commented out). Substance01 does the same: `drop header` filter rejects `('Parameter Name','Units of Measure')` to a reject file.
2. **Raw has 16 distinct pairs** (5 name × 3 unit variants + 1 header pair, e.g. `parts    per million   ` with 4+3 spaces) — normalization runs inline (T3/T6-style Janino mappings) so it works today and stays harmless post-T1–T6. Post-normalize: 15 rows → **1 distinct pair**. All unit bytes are plain ASCII spaces; the messy variant normalizes only because the Janino formula trims after collapsing.
3. **No pass-through on the PSM step** — verified in Kettle source: `TableInput` output = SQL result columns only (`putRow(getReturnRowMeta())`). Reject rows carry only `(id, message)` → the PSM's exception message embeds `name=`/`unit=` context (enriched in Step 0's KJB).
4. **v1 rejects → text files** (Geo08's own pattern), not `etl_rejects`: reject rows lack `etl_run_id` (FK to `etl_run`), which will be supplied later by the master job. Rewiring reject sink to `etl_rejects` = follow-up when `etl_master.kjb` exists.
5. **Empty/null pairs** excluded by the source SQL `WHERE` (defensive); PSM's exception path is the second safety net behind the header filter.
6. **15 PSM calls, all idempotent** — every normalized row is `('Carbon monoxide','PPM')`; all hit the lookup path (seed id=1). Optional future optimization: dedupe post-normalization (no local example of a Unique Rows step found — skip for draft).

### Expected result (today, simulated)

- Source: **16** rows → `drop header` rejects **1** → **15** kept → after Janino: 1 distinct pair `('Carbon monoxide','PPM')` → PSM returns `(id=1, message=NULL)` ×15 → all rows to `done`; **0 psm rejects**; **1 row** in `substance_header_rejects`.
- Future substances (new name/unit combos) normalize-if-known / pass-through-if-new and flow automatically — no transformation edit needed.

Depends on: Step 0 complete (PSM + `rec_user_type` exist in OLAP — both verified live). Independent of T1–T6 (self-normalizing).

---

## Step 0.6 — In-ETL reject persistence (`Substance02.ktr`) — ✅ BUILT

**File:** `etl_pollutant/Substance02.ktr` (19 steps; starts from Substance01 + reject persistence; **Substance01 kept untouched** as the simple text-file-sink reference version).

Rejects are written to `etl_rejects` **by the transformation itself** — no master job required. The run row is opened in-transformation and left `RUNNING`; a future master job closes it (`status='DONE'`, `finished_at`).

### Flow (19 steps)

```
open run (Table Input, OLAP)
  INSERT INTO etl_run (job_name) VALUES ('Substance02') RETURNING etl_run_id   ← 1 row, runs once
       ├───────────────────────┐                              ┌────────────────────┤
source substances (raw, NO WHERE)                            │                    │
  → check source (Janino: source_reason diagnostic)           │                    │
  → source ok? (source_reason IS NULL)                        │                    │
       │true                          │false                  │                    │
       │                    join source reject ◄──────────────┘                    │
       │                      → shape source reject (stage=substance_source)       │
       │                      → write source reject (ExecSQL → etl_rejects)        │
  → drop header ─false─► join header reject ◄──────────────────────────────────────┘
       │true                       → shape header reject (stage=substance_header)
       │                       → write header reject (ExecSQL → etl_rejects)
  → normalize unit → normalize name → strip source reason (Select Values: remove source_reason)
  → inserta substance → result ok?
       │true→ done
       └false─► join psm reject (JoinRows, main=open run, cond TRUE)
                → shape psm reject (stage=substance_psm) → write psm reject (ExecSQL → etl_rejects)
```

| New step | Type | Detail |
|---|---|---|
| **open run** | Table Input (OLAP) | `INSERT INTO etl_run (job_name) VALUES ('Substance02') RETURNING etl_run_id` — once, emits 1 row |
| **check source** | Janino | Adds `source_reason`: `'null or empty name'` / `'null or empty unit'` / `null` when usable — null-safe (`name == null \|\| name.trim().isEmpty()`) |
| **source ok?** | Filter Rows | `source_reason IS NULL` → true → `drop header`; false → source-reject branch. **No WHERE on the source SQL** — nulls/empties are audited, never silently filtered |
| **join source reject** / **join header reject** / **join psm reject** | Join Rows | `main=open run`, condition `TRUE` (cartesian 1×N). Verified vs `JoinRowsMeta.java`: tags `directory/prefix/cache_size/main/compare/condition`; **main step's fields come first** in the output row. Sync-safe: Join Rows blocks until the 1-row open-run stream arrives |
| **shape source/header/psm reject** | Janino (3 formulas) | Add `stage`, `reason`, `raw_payload` (JSON string). source: `substance_source`/carries `source_reason`/`{name,unit}` with null-guards. header: `substance_header`/`header row detected`/`{name,unit}`. psm: `substance_psm`/`reason=message` (embeds `name=/unit=`)/`{id,message}` with quote-escaping and null-id guard |
| **write source/header/psm reject** | ExecSQL (OLAP) | `INSERT INTO etl_rejects (etl_run_id, stage, raw_payload, reason) VALUES (?,?,?::jsonb,?)`, `execute_each_row=Y`, `set_params=Y` with explicit `<arguments>` (binds by name — order/extra-fields safe; pattern cribbed from `music04.ktr`). `ts` → DB default; `myid` unmapped → NULL (column nullable) |

### Design notes

1. **No WHERE on the source query** (vs Substance01) — the audit principle is "never silently drop". Today's raw data has 0 nulls/empties in `parametername`/`units` (profiling: their counts sum to the full 1M), but if any appear they now land in `etl_rejects` with `stage='substance_source'` instead of vanishing. Empty strings specifically must die before the PSM — `inserta_substance('','')` would otherwise **insert** an empty-named substance (the PSM only traps SQL errors, not emptiness).
2. **Janino guard instead of a compound 4-part Filter condition** — `check source` computes one diagnostic field and `source ok?` uses an atomic `IS NULL` test (same serialization already proven in `result ok?`), avoiding unverified nested-condition XML. Bonus: `reason` for the reject comes free from the same field.
3. **Why Join Rows on the reject branches only** — the PSM step's `?` placeholders bind **all** fields of its info row positionally; attaching `etl_run_id` to the main stream would break the 2-`?` binding. Attaching only on reject branches keeps the main path clean. **Same trap hit by `source_reason`** (added by `check source` on the main path): first Spoon run failed with `Error setting value #3 … column index is out of range: 3, number of columns: 2`. Fix: **`strip source reason`** (Select Values, remove `source_reason`) between `normalize name` and `inserta substance`, with the PSM's info-step (`lookup`) repointed to it. Lesson: nothing extra may ride the info row into a positional `?` call.
4. **Why ExecSQL over Table Output** — ExecSQL binds by explicit argument names (local precedent: `music04.ktr`), avoiding both the unverified `TableOutputMeta` XML and any field-order coupling.
5. **Race-free** — Join Rows synchronizes on both inputs; `open run` always completes before any reject can be written. Run row left `RUNNING` by design (decision: close in future master job).
6. **Future main pipeline (T1–T6 ktr)** — same prologue (`job_name='etl_pollutant'`); each stage's false branch gets the same shape→ExecSQL pair (`stage='T1'..'T6'`, map `myid`, payload = stage-relevant fields). The per-stage Filter Rows **is** the split-before-insert; both branches then end in DB sinks.

### Acceptance (verified by SQL simulation, ROLLBACK — DB clean)

- `etl_run` gets 1 row per execution (`job_name='Substance02'`, `status='RUNNING'`).
- **Source reject rows** (synthetic null/empty inputs): `stage='substance_source'`, `reason='null or empty name'` with payload `{"name": null, "unit": "Parts per million"}`; `reason='null or empty unit'` with `{"name": "CO", "unit": ""}`.
- **Header reject row:** `stage='substance_header'`, `reason='header row detected'`, payload `{"name": "Parameter Name", "unit": "Units of Measure"}`. *(Confirmed in a real Spoon run of Substance01: the row landed in `logs/substance_header_rejects_20261028..csv`.)*
- **PSM error path:** `inserta_substance('Carbon monoxide', NULL)` → `(NULL, enriched message)` → reject row with `"id": null` payload; happy path unaffected (`id=1`).
- Audit query: `SELECT j.job_name, j.status, r.stage, r.reason, r.raw_payload FROM etl_run j LEFT JOIN etl_rejects r USING (etl_run_id)`.

---

## 1. Transformation Order (pipeline stages)

The ETL runs as a single Spoon transformation in this exact order:

```
[PG raw read] → [T1 Header dedup] → [T2 Lat/Lon cast + regex validate]
   → [T3 Units normalize] → [T4 DateLocal cast] → [T5 TimeLocal cast]
   → [T6 ParameterName normalize] → [PG staging write] + [reject/error sinks]
```

Each stage passes rows downstream only if valid; invalid rows are routed to a per-stage **reject stream** so they can be logged/loaded into an errors table instead of silently dropped.

---

## 2. T1 — Header Duplication Removal

**Objective:** Remove the embedded header row that was loaded as data.

**Findings:**
- Raw table contains one row where `myid=1` and every column holds the column *name* (e.g. `latitude='Latitude'`, `parametername='Parameter Name'`, `units='Units of Measure'`).
- Confirmed: `myid=1 → latitude='Latitude', longitude='Longitude', parametername='Parameter Name', datelocal='Date Local', timelocal='Time Local'`.

**Transformation / Correction:**
- Filter out the header row.

**Validation:**
- Post-filter count must equal 999,999.
- Assert no row where `latitude ILIKE 'Latitude'` or `units = 'Units of Measure'`.

**Spoon implementation:**
1. **Table Input** (`PG raw read`): `SELECT myid, latitude, longitude, datum, parametername, datelocal, timelocal, dategtm, timegtm, sample, units FROM hourly_42101_2012_1`.
2. **Filter Rows** step — condition:
   ```
   myid <> 1 AND latitude <> 'Latitude'
   ```
   (Defensive: block on either condition.)
   - **True** → main stream → T2.
   - **False** → *reject stream* (header). Send to a **Trash** step or an `etl_rejects` log table (recommended: log with `stage='header'` for auditability).

**Acceptance:** 999,999 rows continue to T2; 1 row rejected.

---

## 3. T2 — Latitude & Longitude (cast + regex validation)

**Objective:** Convert raw `VARCHAR` coords to numeric, validate world-range format via regex from the document, and flag coordinate-system (`datum`) inconsistencies.

**Findings (from doc + DB):**
- 349,217 NULLs in both lat & lon (same rows — the sensor block has no coords).
- No non-numeric values besides NULLs (casting does not increase null count).
- Observed ranges fit North America; all within WGS84/NAD83/NAD27 valid bounds.
- `datum` is a *coordinate-system group*, not a sensor id. Mixed systems (WGS84 / NAD83 / NAD27) → needs a single canonical system for the OLAP cube.
- 5,375 rows with `UNKNOWN` datum → route to a separate flow for root-cause analysis.

**Regex validation (from document — applied to raw VARCHAR before cast):**

- **Latitude** (valid −90…90):
  ```
  ^[+-]?((90(\.0+)?)|([0-8]?\d)(\.\d+)?)$
  ```
  Must match: `90, -90, 90.0, -90.000, 0, -0.0, 45.6789, -33.4489, +12.5, 89.999999, 9.9, 64.84569`
  Must reject: `91, -90.0001, 90.5, 100, 180, 360, 45,5`(comma), `NAN, 45°, N45, ""`(empty)

- **Longitude** (valid −180…180):
  ```
  ^[+-]?((180(\.0+)?)|((1[0-7]\d|[0-9]?\d)(\.\d+)?))$
  ```
  Must match: `180, -180, 180.0, 0, -0.0, -149.93493, -72.631667, 122.4, 90.5, 100, +179.999999`
  Must reject: `181, -180.0001, 180.5, -181, 360, 1000, 12,34`(comma), `Longitude`(header), `""`(empty)

**Transformation / Correction:**
1. Apply regex on raw string. Note: NULL coordinates should pass as "NULL-allowed" (they are legitimately missing, not invalid-format).
2. Replace comma decimal separators (`45,5` → `45.5`) *before* regex if present (doc assumes pre-replacement).
3. Cast valid strings → `double precision`.
4. Handle `datum`:
   - Convert all to one canonical system (recommended: **WGS84**, the most populated — 409,441 rows). Use a Kettle coordinate conversion (e.g. GeoTools/`Calculate coordinates` with target CRS) or a lookup-based transform.
   - `datum = 'UNKNOWN'` (5,375 rows) → separate reject/reporting branch.
   - `datum` empty/NULL (349,217 rows) → these are the rows with NULL lat/lon; they inherit the NULL-coord path.

**Validation:**
- Regex must reject out-of-range / malformed values (unit tests for the must-match/must-reject lists above).
- Assert post-cast null count for lat == null count for lon == 349,217 (no new nulls introduced by cast).
- Assert lat ∈ [−90, 90], lon ∈ [−180, 180] post-cast.

**Spoon implementation:**
1. **`Replace`** step (pre-clean): `,` → `.` in `latitude`/`longitude` (only if comma decimal detected).
2. **` regexp` / `Filter Rows` with Regular Expression** on `latitude` → boolean `lat_ok`; same for `longitude` → `lon_ok`.
   - NULL coords → set `lat_ok/lon_ok = TRUE` with a separate `coords_null` flag (legitimate missing vs invalid).
3. **` Filter Rows`**:
   - `lat_ok AND lon_ok AND NOT coords_null` → **` Number`** step / **` Select Values`** type-cast to `double precision` → downstream.
   - `lat_ok AND lon_ok AND coords_null` → main stream with NULLs preserved (flagged).
   - `NOT lat_ok OR NOT lon_ok` → **reject sink** (`etl_rejects`, `stage='latlon_format'`).
4. **` Group By` / ` Unique Rows`** check or **` Filter`** on `datum='UNKNOWN'` → separate reporting stream.
5. **Coordinate conversion:** follow the `Geo08.ktr` approach — a **` User Defined Java Class`** step using WorldWind `DatumTransformation` to reproject NAD27/NAD83 → WGS84 in-transformation (no lookup table).

**Acceptance:** 999,999 rows continue (650,782 with coords + 349,217 NULL-coord rows); 0 rows rejected for format (raw data is clean); `UNKNOWN` datum rows isolated for review.

---

## 4. T3 — Units Normalization

**Objective:** Map all unit spelling variants to a single canonical unit so OLAP grouping is correct.

**Findings:**
- 4 distinct values: `Parts per million` (735,453), `parts  per million` (261,336 — lowercase + double space), `PPM` (3,210), `Units of Measure` (1 — the header row, already removed in T1).
- All three real variants mean **PPM** (parts per million).

**Transformation / Correction:**
- Map raw → standard **inline in the transformation**:
  | Raw | Standard |
  |---|---|
  | `Parts per million` | `PPM` |
  | `parts  per million` | `PPM` |
  | `PPM` | `PPM` |
- Normalize whitespace (collapse double spaces) and case first, so future variants are caught.

**Validation:**
- Post-transform, exactly **1** distinct value in `units`: `PPM`.
- Row count unchanged.
- Any raw value not in the map → reject/log (do not silently pass).

**Spoon implementation:**
1. **` Select Values` → `Modified Formula` / `Calculator`**: TRIM + REGEXP_REPLACE to collapse whitespace for a normalized key.
2. **` Value Mapper`**: hardcoded source→target pairs from the table above (inline, no lookup table).
3. **` Filter Rows`**: mapped rows → downstream; unmapped → **reject sink** (`etl_rejects`, `stage='units'`).

**Acceptance:** All rows have `units = 'PPM'`.

---

## 5. T4 — DateLocal Cast

**Objective:** Cast `DateLocal` string → proper `DATE`, validate ISO-8601.

**Findings:**
- 100% of rows (excl. header) match ISO `YYYY-MM-DD` (verified against doc's Reference Data Matcher + my DB check).
- Year coverage: all 2012. Monthly distribution roughly uniform (75k–91k/month).

**Transformation / Correction:**
- Cast `datelocal VARCHAR` → `date`.
- No correction needed (format already valid).

**Validation:**
- ISO-date pattern check: `^\d{4}-\d{2}-\d{2}$` → 0 failures.
- Cast must not introduce NULLs (null count before == after).
- Year must equal 2012 (or fall in expected ingestion window).
- Referential check (optional): `datelocal <= NOW()` (no future dates).

**Spoon implementation:**
1. **` Filter Rows`** with regex `^\d{4}-\d{2}-\d{2}$` on `datelocal` (defensive) → failures to reject sink.
2. **` Select Values`** → Data type = `Date`.
3. **` Date/Time`** step (optional): extract year/month for completeness monitoring into audit columns.

**Acceptance:** `datelocal` is type `date`; 0 rows lost; year/month distributions match baseline.

---

## 6. T5 — TimeLocal Cast

**Objective:** Cast `TimeLocal` string → proper `TIME`, validate `HH:MM`.

**Findings:**
- 100% of rows (excl. header) match `##:##` (doc's Pattern Finder + my DB check).
- Range: 00:00:00 … 23:00:00 (hourly granularity, all 24 hours present).

**Transformation / Correction:**
- Cast `timelocal VARCHAR` → `time`.
- No correction needed (format already valid).

**Validation:**
- Pattern check `^\d{2}:\d{2}$` → 0 failures.
- Cast must not introduce NULLs.
- Range check: hour ∈ [00, 23].
- Cross-field check (recommended): `dategtm/timegtm` offset from `datelocal/timelocal` should be consistent (staging already has `utc_offset_hours`).

**Spoon implementation:**
1. **` Filter Rows`** with regex `^\d{2}:\d{2}$` on `timelocal` → failures to reject sink.
2. **` Select Values`** → Data type = `Time`.
3. Optional **` Calculator`**: compute `utc_offset_hours = (dategtm+timegtm) − (datelocal+timelocal)` in hours for TZ audit (matches existing staging column).

**Acceptance:** `timelocal` is type `time`; 0 rows lost; offset audit consistent with staging.

---

## 7. T6 — ParameterName Normalization

**Objective:** Resolve spelling variants/typos of the pollutant name to a single canonical value.

**Findings:**
- 6 distinct values (incl. 1 header): `Carbon monoxide` (786,268), `Carbon  monoxide` (84,753 — double space), `CO` (43,392 — abbreviation), `Carbno monoxide` (43,294 — typo), `Cabron monoxide` (42,292 — typo).
- Doc recommends a staged approach (deterministic first, fuzzy only as escalation).

**Transformation / Correction (layered, per document):**
1. **Text normalization:** TRIM, collapse internal whitespace, strip punctuation, case-fold.
2. **Alias mapping (inline):** hardcoded aliases → canonical via `Value Mapper`:
   | Raw (normalized) | Canonical |
   |---|---|
   | `carbon monoxide` | `Carbon monoxide` |
   | `co` | `Carbon monoxide` |
   | `carbno monoxide` | `Carbon monoxide` |
   | `cabron monoxide` | `Carbon monoxide` |
3. **Fuzzy matching (escalation only):** Levenshtein / trigram for typos not in the mapper → propose candidates, **human approves**, then add to the mapper.
4. **Manual monitoring:** instances with no match → reviewed offline; mapper updated.
5. **Semantic embeddings (last resort, offline only):** run embedding pipeline out-of-band to propose alias candidates; **do not** put embeddings inline in the ETL (too slow/costly). Only add if deterministic methods show high unmatched rate.

**Validation:**
- Post-transform, exactly **1** distinct `parametername`: `Carbon monoxide`.
- Row count unchanged.
- Unmapped values → reject/log with the fuzzy-match score for human review.

**Spoon implementation:**
1. **` Select Values` → `Modified Formula`/`Calculator`**: TRIM + REGEXP_REPLACE (collapse space) + LOWER → `pn_norm`.
2. **` Value Mapper`**: hardcoded alias → canonical pairs (inline, no lookup table).
3. **` Fuzzy Matching`** step (PDI built-in) for unmatched rows → attach best candidate + score; threshold e.g. distance ≤ 2.
4. **` Filter Rows`**:
   - Mapper match → downstream (canonical).
   - Fuzzy match ≥ threshold → downstream flagged `needs_review` (or routed to review queue).
   - No match → **reject sink** (`etl_rejects`, `stage='parametername'`).

**Acceptance:** All rows map to `Carbon monoxide`; unmatched rows are surfaced, not dropped.

---

## 8. Pipeline Architecture (Spoon)

**Orchestration Job** (`etl_master.kjb`):
1. **Step 0** — `create_schema.kjb` (or inline **Execute SQL**): create `Substance` + auxiliaries (idempotent).
2. Run main transformation `etl_pollutant.ktr` (T1→T6).
3. Post-load SQL assertions (row counts, distinct checks).

**One master transformation** (`etl_pollutant.ktr`) chaining T1→T6, plus:

- **Reject sinks:** each stage routes invalid rows to a shared `etl_rejects` table with columns `(etl_run_id, stage, myid, raw_payload, reason, ts)`. Never silently drop.
- **Value mappings:** inline in the transformation via `Value Mapper` steps (T3 units, T6 parametername aliases) — no lookup tables.
- **Audit / metrics:** row counts in/out per stage, null deltas, distinct-value checks → logged per `etl_run_id`.
- **Orchestration:** optional Spoon **Job** (`.kjb`) to run the transformation, then run post-load SQL assertions.

**Data flow:**
```
hourly_42101_2012_1 (raw, VARCHAR)
        │  Table Input
        ▼
   T1 Filter (myid<>1 AND latitude<>'Latitude') ──reject──▶ Trash/log
        │ 999,999 rows
        ▼
   T2 Replace(,→.) → regex lat/lon → cast double → datum handling
        ├── format-fail ──▶ etl_rejects(stage='latlon_format')
        ├── UNKNOWN datum ──▶ separate reporting stream
        ▼
   T3 normalize units → Value Mapper → cast/standardize
        ├── unmapped ──▶ etl_rejects(stage='units')
        ▼
   T4 regex ISO date → cast date
        ├── fail ──▶ etl_rejects(stage='datelocal')
        ▼
   T5 regex HH:MM → cast time (+ utc_offset audit)
        ├── fail ──▶ etl_rejects(stage='timelocal')
        ▼
   T6 normalize parametername → Value Mapper → fuzzy escalate
        ├── unmapped ──▶ etl_rejects(stage='parametername')
        ▼
hourly_staging (typed, canonical, WGS84)
```

---

## 9. Target Schema (staging) — alignment

Existing `hourly_staging` already matches the intended typed shape:

| Column | Type | Produced by |
|---|---|---|
| myid | integer | passthrough |
| latitude | double precision | T2 cast |
| longitude | double precision | T2 cast |
| datum | varchar(30) | T2 (canonical CRS) |
| parametername | varchar(30) | T6 canonical |
| datelocal | date | T4 cast |
| timelocal | time | T5 cast |
| dategtm | date | T4-style cast (mirror) |
| timegtm | time | T5-style cast (mirror) |
| sample | double precision | passthrough cast |
| units | varchar(30) | T3 standard (`PPM`) |
| utc_offset_hours | double precision | T5 audit calc |

Note: existing staging has 999,999 rows (header already excluded) — consistent with T1 goal.

---

## 10. Acceptance Criteria (end-to-end)

0. Step 0 JOB creates `Substance` + auxiliaries idempotently (re-run = no-op, exit 0).
1. Header row (`myid=1`) excluded → 999,999 rows land in staging.
2. `latitude`/`longitude` are `double precision`, regex-validated, within world range; null count unchanged (349,217).
3. `units` has exactly one value: `PPM`.
4. `datelocal` is `date`, `timelocal` is `time`, zero cast-induced nulls.
5. `parametername` has exactly one value: `Carbon monoxide`.
6. All invalid/rejected rows captured in `etl_rejects` with stage + reason (auditable, not dropped).
7. T3/T6 value mappings are inline (`Value Mapper` steps) — no lookup tables required.
8. Per-stage row-count + null-delta audit logged against `etl_run_id`.
9. `Substance` dimension is populated/ready for the next ETL step (substance load).

---

## 11. Environment Notes

- **DB:** PostgreSQL 18 (Homebrew), DB `etl3`, user `selewaut`.
- **Raw data:** already loaded into `hourly_42101_2012_1` (1M rows).
- **Tool:** Pentaho Data Integration (Spoon) — `spoon.sh` located at `~/Downloads/data-integration/`.
  - Note: that path is currently sandbox-restricted for direct execution. Either grant terminal Full Disk Access for Downloads, or copy `data-integration/` to a permitted workspace path (e.g. `etl_itba/etl_pollutant/` or `~/opt/`) before building the `.ktr`/`.kjb` there.
- **Python (for offline fuzzy/embedding prep only):** use `uv` (v0.11.9) for any scripting, not pip.

---

## 12. Suggested Build Order in Spoon

1. **Step 0 — `create_substance_schema.kjb`:** create `Substance`, `etl_run`, `etl_rejects` + `inserta_substance` PSM; verify idempotent re-run. *(Done — job + SQL built and verified in OLAP.)*
2. **Step 0.5 — `Substance01.ktr`:** substance populate draft — raw source + in-canvas header drop (Geo08 pattern). *(Done — built + simulated end-to-end: 16→15→1 pair→id=1; pending an actual Spoon run. Rejects → text files.)*
2b. **Step 0.6 — `Substance02.ktr`:** Substance01 + source-audit guard (`check source`/`source ok?`, no WHERE) + in-ETL reject persistence (`open run` → Join Rows → Janino shape → ExecSQL into `etl_rejects`) across all 3 stages (source/header/psm). *(Done — built, 19 steps, SQL-simulated with ROLLBACK; pending a Spoon run. Follow-ups: master job closes `etl_run`; wire T1–T6 reject sinks the same way; optional post-normalize dedupe.)*
3. Build T1 (header filter) + verify 999,999 rows.
4. Add T2 (regex lat/lon + cast + datum handling) + unit-test regex lists.
5. Add T3 (units normalize + inline Value Mapper).
6. Add T4/T5 (date/time casts + audit offset).
7. Add T6 (parametername normalize + inline Value Mapper + fuzzy escalation).
8. Wire reject sinks + audit logging.
9. Wrap in master Job with Step 0 + post-load SQL assertions.
