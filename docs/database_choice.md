# Database choice: DuckDB + Parquet

## The workload

- One station, 1-minute data, Aug 1–Oct 29, 2026: about 130,000 measurement rows. The DuckDB file is about 10 MB, and the Parquet snapshot is 1.8 MB.
- **One writer:** the Dagster pipeline, one run at a time.
- **Readers:** the dashboard, read-only, and the pipeline's own checks.
- **Queries are analytical:** daily aggregates, rolling medians, comparing GHI, DNI and DHI within each minute, and joining flags to overrides.

## Why DuckDB here

- **No server.** The database is one file. `make setup` is all a reviewer needs, and CI tests run against an in-memory database.
- **Columnar.** Aggregates over a few columns of many rows (most of what QC and the dashboard do) are fast.
- **Reads and writes Parquet natively.** The pipeline exports a Parquet snapshot that's committed to git. The hosted dashboard queries those files with the same SQL, so it doesn't need a database server, and hosting stays free.
- **Still a real database.** Transactions let each data change commit together with its audit event. Primary and foreign keys apply (e.g. `qc_flags` → `qc_rules`), and replacing one day's rows is a single `DELETE` + `INSERT`.

Why not the alternatives:

- **Parquet files only:** no transactions or constraints, so an audit event could be written without its data change, or the other way around.
- **SQLite:** also serverless, but it stores rows, so aggregates are slower, and it can't read or write Parquet.
- **PostgreSQL:** the right tool at scale (see below), but here it would need a server for the demo and a hosted database for the dashboard.

## What it costs, and how the project handles it

| Limitation | How it's handled |
| --- | --- |
| Only one process can write at a time | Dagster runs one job at a time (`.dagster/dagster.yaml`). `make backfill` needs `make dagster` stopped |
| No roles or permissions, so nothing in the database itself stops an `UPDATE` on `audit_log` | All audit writes go through one insert-only function (`append_audit`). The hash chain makes any edit or deletion detectable, and the `audit_chain_intact` check looks for one on every QC run |
| No triggers | Same as above: the rule lives in code, and a test covers it |
| No users or access control | The hosted dashboard is read-only and reads a public snapshot. Overrides go through git, where every change has an author and a review |

## What I'd use in production

At production scale (many stations, years of data, maybe 1-second data, several services writing, many users, plus ISO 17025 record-keeping), I'd use **PostgreSQL with time partitioning**, or **TimescaleDB**, which partitions and compresses automatically. I've run a ~2 TB time-series PostgreSQL database partitioned by time, and the same patterns apply here:

- **Partition `measurements` and `qc_flags` by month.** Re-running a day touches one partition, and old partitions can be compressed or moved to cheaper storage.
- **Enforce add-only in the database:** `REVOKE UPDATE, DELETE ON audit_log` from every role the pipeline uses, plus a trigger that rejects them. The hash chain stays as a second check.
- **Per-user roles:** lab staff can read, only the pipeline role writes, and overrides get their own role (or a reviewed table with a UI instead of a CSV).
- **Backups with point-in-time recovery,** which a lab's quality system would require.
- **DuckDB stays useful next to it** for ad-hoc analysis: it can query PostgreSQL directly or read Parquet exports.

Moving the code over would be small. The schema in `src/abq_qc/db.py` is plain SQL. The DuckDB-specific parts are the list aggregates in the `qc_status` view (`list()` → `array_agg()`), `INSERT ... BY NAME`, and the Parquet export.
