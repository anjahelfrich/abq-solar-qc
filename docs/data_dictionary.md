# Data dictionary

Every table the pipeline writes, plus the overrides CSV it reads. The DuckDB file (`data/abq_qc.duckdb`) and the committed Parquet snapshot (`snapshot/<table>.parquet`) have the same columns.

- **Time:** all timestamps are UTC (`TIMESTAMPTZ`). The dashboard converts them to the chosen time zone for display only.
- **Days:** a "day" is a UTC calendar day, the same unit as NOAA's daily files.
- **Irradiance** is in W/m². UVB is in mW/m², as NOAA reports it.
- **Variables:** `ghi` (global horizontal), `dni` (direct normal), `dhi` (diffuse horizontal). `all` means the whole minute rather than one sensor.
- **Daytime:** solar zenith angle < 90°.
- **Lineage columns** (`run_id`, `source_sha256`, `code_version`) link a row to the Dagster run that wrote it, the raw file it came from, and the git commit that ran.

`tests/test_data_dictionary.py` fails if a column is added to or removed from the schema without updating this file.

## `source_files`

One row per downloaded file version. Re-downloading identical content adds nothing. A re-download with different content adds a row, so both versions stay traceable. The row with the newest `downloaded_at` is the current version for that day.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `sha256` | VARCHAR | | SHA-256 of the file's bytes. The raw file is kept at `data/raw/<file_date>/<sha256>.dat` |
| `file_date` | DATE | | UTC day the file covers |
| `url` | VARCHAR | | Where it was downloaded from |
| `size_bytes` | BIGINT | bytes | File size |
| `downloaded_at` | TIMESTAMPTZ | | When this version was downloaded |
| `run_id` | VARCHAR | | Dagster run that downloaded it |

Primary key: (`file_date`, `downloaded_at`).

## `measurements`

One row per minute, as received from NOAA. No row means NOAA didn't send that minute. NOAA's missing-value code (`-9999.9`) is stored as NULL.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `ts_utc` | TIMESTAMPTZ | | End of the 1-minute averaging period |
| `zenith_deg` | DOUBLE | degrees | Solar zenith angle at the middle of the minute, from NOAA's file. 0 = sun overhead, 90 = horizon |
| `ghi` | DOUBLE | W/m² | Global horizontal irradiance: all sunlight on a flat surface |
| `dni` | DOUBLE | W/m² | Direct normal irradiance: the sun's beam, measured facing the sun |
| `dhi` | DOUBLE | W/m² | Diffuse horizontal irradiance: sky light with the sun's disk shaded |
| `uvb` | DOUBLE | mW/m² | Global UVB. Stored, not checked by our QC |
| `std_ghi` | DOUBLE | W/m² | Standard deviation of the 1-second GHI samples within the minute |
| `std_dni` | DOUBLE | W/m² | Same, for DNI |
| `std_dhi` | DOUBLE | W/m² | Same, for DHI |
| `std_uvb` | DOUBLE | mW/m² | Same, for UVB |
| `noaa_qc_ghi` | TINYINT | | NOAA's QC flag for GHI. 0 = good |
| `noaa_qc_dni` | TINYINT | | NOAA's QC flag for DNI |
| `noaa_qc_dhi` | TINYINT | | NOAA's QC flag for DHI |
| `noaa_qc_uvb` | TINYINT | | NOAA's QC flag for UVB |
| `source_sha256` | VARCHAR | | Hash of the file this row came from (→ `source_files.sha256`) |
| `source_line` | INTEGER | | 1-based line number in that file |
| `run_id` | VARCHAR | | Dagster run that loaded the row |
| `code_version` | VARCHAR | | Git commit (12 characters) that loaded the row, with `-dirty` if there were uncommitted edits |

No primary key on `ts_utc` on purpose: a file with duplicate minutes is loaded as-is and caught by the `no_duplicate_minutes` asset check.

## `qc_rules`

One row per rule version, synced from `src/abq_qc/qc_rules.py`. Rows are added, never changed, so an old flag always points to the thresholds that produced it.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `rule_id` | VARCHAR | | `limits`, `closure`, `gap`, `stuck` or `drift` |
| `version` | INTEGER | | Bumped whenever the description, thresholds or severity change. The pipeline refuses to run if they change without a bump |
| `description` | VARCHAR | | What the rule checks, in words |
| `thresholds` | JSON | | The rule's parameters, e.g. `{"min_run_minutes": 10, ...}` |
| `severity` | VARCHAR | | `bad`, `suspect`, `missing` or `warning` |
| `registered_at` | TIMESTAMPTZ | | When this version was first written to the database |

Primary key: (`rule_id`, `version`).

## `qc_flags`

One row per rule failure, per variable, per minute. Only failures are stored: a minute with no rows here passed every rule. Drift is per day, so it lives in `drift_trend` instead.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `ts_utc` | TIMESTAMPTZ | | The flagged minute |
| `variable` | VARCHAR | | `ghi`, `dni`, `dhi`, or `all` for a closure failure (the test can't tell which sensor is wrong) or a missing minute |
| `rule_id` | VARCHAR | | Rule that failed (→ `qc_rules`) |
| `rule_version` | INTEGER | | Version of that rule (→ `qc_rules.version`) |
| `severity` | VARCHAR | | Copied from the rule: `bad` (limits), `suspect` (closure, stuck), `missing` (gap) |
| `value` | DOUBLE | varies | `limits` and `stuck`: the measured value in W/m². `closure`: the ratio GHI ÷ (DNI·cos Z + DHI), no unit. `gap`: NULL |
| `run_id` | VARCHAR | | Dagster run that wrote the flag |

## `overrides`

Manual decisions, loaded from `overrides/manual_overrides.csv` (see below). The CSV in git is the source of truth; this table is a copy.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `override_id` | VARCHAR | | Unique id, e.g. `OVR-0001` |
| `start_utc` | TIMESTAMPTZ | | First minute covered (included) |
| `end_utc` | TIMESTAMPTZ | | Last minute covered (included) |
| `variable` | VARCHAR | | `ghi`, `dni`, `dhi` or `all` |
| `rule_id` | VARCHAR | | For `accept`: the rule whose flags are accepted (`limits`, `closure` or `stuck`). NULL for `flag_bad` |
| `action` | VARCHAR | | `accept`: the automatic flag is a false alarm, final status good. `flag_bad`: the data is known to be bad |
| `reason` | VARCHAR | | Why, in words |
| `author` | VARCHAR | | Who made the decision |
| `source_sha256` | VARCHAR | | Hash of the CSV version this row was loaded from |
| `run_id` | VARCHAR | | Dagster run that loaded it |

## `qc_status` (view)

Final status per sensor value that has an automatic flag or an override. Computed when queried, so it always matches the current flags and overrides. A value not listed here is good. In the snapshot it is written out as a table.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `ts_utc` | TIMESTAMPTZ | | The minute |
| `variable` | VARCHAR | | `ghi`, `dni` or `dhi`. Flags on `all` appear once for each sensor |
| `auto_rules` | VARCHAR[] | | Automatic rules that fired. Accepted flags stay listed here |
| `override_ids` | VARCHAR[] | | Overrides that apply |
| `final_status` | VARCHAR | | Worst of: `missing` > `bad` > `suspect` > `good`. An accepted flag counts as `good`; `flag_bad` counts as `bad` |

## `audit_log`

Add-only event log. Written only through `append_audit()` in `db.py`, in the same transaction as the data change it describes. Each row's hash covers its content and the previous row's hash, so editing or deleting an old row breaks the chain.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `event_id` | BIGINT | | 1, 2, 3, … with no gaps |
| `event_time` | TIMESTAMPTZ | | When the event was written |
| `event_type` | VARCHAR | | See the table below |
| `actor` | VARCHAR | | `pipeline`, or the override's author for `override_applied` |
| `details` | JSON | | Event-specific fields, see below |
| `run_id` | VARCHAR | | Dagster run that wrote the event |
| `prev_hash` | VARCHAR | | `event_hash` of the event before (64 zeros for the first event) |
| `event_hash` | VARCHAR | | SHA-256 of this event's fields plus `prev_hash` |

| `event_type` | Written when | `details` keys |
| --- | --- | --- |
| `file_ingested` | a day's file is downloaded for the first time | `file_date`, `url`, `sha256`, `data_rows` |
| `file_changed` | a re-download's content differs from the current version | `file_date`, `url`, `sha256`, `previous_sha256` |
| `overrides_loaded` | the overrides CSV changed since the last load | `sha256`, `previous_sha256`, `override_count`, `added`, `removed`, `changed` |
| `qc_run` | QC runs for a day | `day`, `rule_versions`, `flag_counts`, `overrides_sha256` |
| `override_applied` | an override matches data in a QC run | `day`, `override_id`, `action`, `reason`, `minutes` |

## `daily_summary`

One row per UTC day, rebuilt whenever that day is re-run.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `day` | DATE | | UTC day |
| `received_minutes` | INTEGER | minutes | Distinct minutes in NOAA's file (out of 1440) |
| `availability_pct` | DOUBLE | % | `received_minutes` ÷ 1440. Counts rows, so a row holding NOAA's missing value still counts. The dashboard's availability counts values instead |
| `daytime_minutes` | INTEGER | minutes | Received minutes with zenith < 90° |
| `flagged_daytime_minutes` | INTEGER | minutes | Daytime minutes with at least one automatic flag, gaps not counted. Overrides are ignored: this measures the automatic QC |
| `flags_limits` | INTEGER | flags | `qc_flags` rows for `limits` |
| `flags_closure` | INTEGER | flags | `qc_flags` rows for `closure` |
| `flags_gap` | INTEGER | flags | `qc_flags` rows for `gap` |
| `flags_stuck` | INTEGER | flags | `qc_flags` rows for `stuck` |
| `clear_sky_minutes` | INTEGER | minutes | Minutes with zenith < 60°, GHI > 50 W/m² and DHI/GHI < 0.2 |
| `clear_sky_closure_ratio` | DOUBLE | ratio | Median closure ratio over those minutes. NULL if there were none. Input to drift |
| `noaa_both` | INTEGER | sensor values | GHI/DNI/DHI values flagged by both our automatic QC and NOAA |
| `noaa_ours_only` | INTEGER | sensor values | Flagged by us, not by NOAA |
| `noaa_theirs_only` | INTEGER | sensor values | Flagged by NOAA, not by us |
| `run_id` | VARCHAR | | Dagster run that wrote the row |

## `drift_trend`

One row per day in `daily_summary`. Rebuilt in full on every run, because the rolling window and the baseline look across days.

| Column | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `day` | DATE | | UTC day |
| `daily_ratio` | DOUBLE | ratio | That day's `clear_sky_closure_ratio` |
| `rolling_median` | DOUBLE | ratio | Median of `daily_ratio` over the 7 days ending on `day`. NULL if fewer than 3 of those days have a ratio |
| `baseline` | DOUBLE | ratio | Median `daily_ratio` over Aug 1–31, 2026 |
| `deviation_pct` | DOUBLE | % | (`rolling_median` ÷ `baseline` − 1) × 100 |
| `warn` | BOOLEAN | | `deviation_pct` is more than ±2% |
| `run_id` | VARCHAR | | Dagster run that wrote the row |

## Input: `overrides/manual_overrides.csv`

One row per manual decision, changed through git commits. Loaded into `overrides` by the `manual_overrides` asset; a row that breaks the rules below stops QC.

| Column | Meaning |
| --- | --- |
| `override_id` | Unique, e.g. `OVR-0001` |
| `start_utc` | UTC ending in `Z`, e.g. `2026-08-01T21:35:00Z`. Included |
| `end_utc` | Same format. Included |
| `variable` | `ghi`, `dni`, `dhi` or `all` |
| `rule_id` | Required for `accept` (`limits`, `closure` or `stuck`). Empty for `flag_bad` |
| `action` | `accept` or `flag_bad` |
| `reason` | Required |
| `author` | Required |

Gaps can't be accepted, since there is no value to keep. A flag on `all` (a closure failure) is only accepted by an override on `all`.
