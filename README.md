# Maven Market Capstone

Databricks Asset Bundle for the Maven Market Lakehouse: Bronze ingestion,
the Silver/Gold Lakeflow Declarative Pipeline, and the DQ summary — with
CI/CD that deploys (never runs) on every push and PR into `main`.

## Repo structure

```
.
├── databricks.yml              bundle root: targets, catalog variable
├── resources/
│   ├── pipeline.yml             maven_silver_pipeline (Silver + Gold)
│   ├── bronze_job.yml           Bronze_Ingestion job (new - scheduled 10 PM IST)
│   └── silver_gold_job.yml      Silver_and_Gold_Pipeline job (unscheduled, triggered by Bronze_Ingestion)
├── src/
│   ├── bronze/                  01_bronze_ingestion, 02_bronze_mongo, 03_bronze_kafka
│   ├── silver/                  6 Silver notebooks incl. dq_summary_report
│   ├── gold/                    3 Gold notebooks
│   └── governance/              groups_and_permissions.py (manual admin script, not scheduled)
└── .github/workflows/deploy.yml
```

## CI/CD authentication setup (required before CI/CD will work at all)

GitHub Actions authenticates as a Databricks **service principal**, not a
personal login. Set this up once:

1. Databricks workspace → **Settings → Identity and access → Service
   principals → Add service principal**. Name it e.g. `maven-capstone-ci`.
2. Open it → **Secrets** tab → **Generate secret**. Copy the **Client ID**
   and **Secret** immediately — the secret is shown once only.
3. Grant it access: **Can Manage** on `maven_silver_pipeline`, on
   `Bronze_Ingestion`, and on `Silver_and_Gold_Pipeline` (or make it a
   workspace admin for now while you're still building, tighten later),
   plus Unity Catalog grants on `maven_dev` matching `data_engineers` —
   it becomes the identity your scheduled runs actually execute as.
4. GitHub repo → **Settings → Secrets and variables → Actions → New
   repository secret**, add all three:
   - `DATABRICKS_HOST` = `https://adb-7405616945565645.5.azuredatabricks.net`
   - `DATABRICKS_CLIENT_ID` = the Client ID from step 2
   - `DATABRICKS_CLIENT_SECRET` = the Secret from step 2
5. Put that same Client ID into `databricks.yml`'s `prod` target under
   `run_as.service_principal_name` (replace the
   `<ci-service-principal-client-id>` placeholder). It's not sensitive —
   only the secret is — so it's fine committed in plain text.

If you see `cannot configure default credentials` during
`bundle validate`/`bundle deploy` in the Action logs, it means one of the
three secrets above is missing or empty — GitHub prints a real secret as
`***`; a blank value after the colon in the log means it was never added.

## REQUIRED one-time step before your first push

You already have a live pipeline (`b9a24975-2af3-480c-864d-dfb2aa523fd4`) and
a live job (`Silver_and_Gold_Pipeline`) in the workspace, created by hand.
If you just push this repo and let CI/CD deploy, Databricks Asset Bundles
will create **brand-new** pipeline/job objects instead of updating the ones
you already have — you'd end up with duplicates, and the old ones would sit
there doing nothing but still technically existing. To avoid that, bind this
bundle to the existing objects **once**, locally, before your first push:

```bash
databricks auth login   # or configure your profile however you normally do

# adopt the existing pipeline
databricks bundle deployment bind maven_silver_pipeline \
  b9a24975-2af3-480c-864d-dfb2aa523fd4 -t prod

# adopt the existing job - get its numeric id first:
databricks jobs list | grep Silver_and_Gold_Pipeline
databricks bundle deployment bind Silver_and_Gold_Pipeline <that-job-id> -t prod
```

After this, every `databricks bundle deploy -t prod` (including the one CI/CD
runs) updates those same two objects in place instead of creating new ones.
`Bronze_Ingestion` is new, so it needs no binding — its first deploy creates
it fresh.

## How the `catalog` variable resolves

| How you deploy | Target | `catalog` | When you'd use it |
|---|---|---|---|
| `databricks bundle deploy` (no `-t`) | `dev` | `maven_test` | your own local testing |
| CI/CD — every push to `main`, every PR | `prod` | `maven_dev` | the only thing that actually matters day to day |

Every Silver/Gold notebook already reads this via
`spark.conf.get("catalog", "maven_test")`, and the pipeline passes it
through its own `configuration.catalog` field (that field was missing from
the pipeline you'd built by hand, so it was always silently falling back
to `maven_test` regardless of catalog — this bundle fixes that). Bronze
notebooks read it from a job parameter via `dbutils.widgets.get("catalog")`.

## What runs on a push or PR — and what doesn't

`.github/workflows/deploy.yml` fires on **both** a PR into `main` and a push
to `main`, and does the exact same two things either way:

1. `databricks bundle validate -t prod`
2. `databricks bundle deploy -t prod`

That's it. `deploy` only pushes the resource *definitions* — job tasks,
pipeline config, the schedule — into the workspace. It never calls
`databricks bundle run`, so no cluster starts and no credits are spent just
because you pushed or opened a PR.

One thing worth knowing: because PRs deploy too, the live `prod` job/pipeline
definitions reflect whichever PR branch pushed most recently — if you have
two PRs open at once, the second push wins. Fine for a two-person team if
you're aware of it; worth remembering if that ever causes confusion.

## The only thing that spends compute on its own: the 10 PM schedule

`Bronze_Ingestion` is deployed with:

```yaml
schedule:
  quartz_cron_expression: "0 0 22 * * ?"
  timezone_id: "Asia/Kolkata"
  pause_status: PAUSED
```

It's deployed **paused** on purpose, so pushing this repo for the first time
doesn't suddenly start a 10 PM job tonight. Flip it on in the Jobs UI
(Bronze_Ingestion → Schedule → toggle on) whenever you're ready, or change
`pause_status` to `UNPAUSED` here and let CI/CD redeploy it.

When it does run: `bronze_csv`, `bronze_mongo`, and `bronze_kafka` run in
parallel on one ephemeral job cluster; once all three succeed, the job's
last task (`trigger_silver_gold`, a `run_job_task`) calls
`Silver_and_Gold_Pipeline`, which runs the DLT pipeline (Silver then Gold,
resolved automatically by Lakeflow) and then `dq_summary_report`.
`Silver_and_Gold_Pipeline` has no schedule of its own — it only ever runs
because `Bronze_Ingestion` calls it, so there's exactly one clock for the
whole platform and one place to pause everything.

## Compute

Every job cluster and the pipeline's own cluster use the same shape you'd
already tuned by hand: single-node `Standard_D4ds_v4`, `num_workers: 0`,
`autotermination_minutes: 20` on the job clusters. Nothing here is
`continuous`, nothing stays warm between runs — `dq_summary`'s old
`existing_cluster_id` (pinned to one specific always-on interactive
cluster) was replaced with its own ephemeral job cluster of the same shape,
so the whole thing no longer depends on that one cluster still existing.

`data_security_mode: USER_ISOLATION` (Shared) is set explicitly on the
Bronze job clusters, matching the fix already documented in your Setup
Runbook (Step 13) — the Mongo Spark connector was dropped specifically so
this compute could stay Shared-mode, not Single User.

`spark_version: 15.4.x-scala2.12` is a placeholder — check it against your
workspace's current LTS runtime before your first deploy and adjust if
needed; it wasn't pinned anywhere in what you gave me.

## What changed in the notebooks vs. your `.dbc` export

- `01_bronze_ingestion.py`, `03_bronze_kafka.py`: `catalog = "maven_test"`
  replaced with `dbutils.widgets.text("catalog", "maven_test")` +
  `dbutils.widgets.get("catalog")`, matching the pattern `02_bronze_mongo.py`
  already used — this is what lets the job's `base_parameters.catalog`
  actually reach them.
- `01_bronze_ingestion.py`: removed a leftover debug line
  (`print("Sanjay is chutiya")`) from the last cell.
- `03_bronze_kafka.py`: the inventory-events row-peek cell was sitting
  first in the notebook (before `catalog` was even defined — it would have
  errored top-to-bottom); moved to the end, after the online-orders check
  it was clearly meant to sit beside.
- The stale top-level duplicate `silver_dimensions_basic.python` (hardcoded
  catalog, superseded by the one in `silver/`) was dropped — not copied in.
- `governance/groups_and_permissions.py` is included for reference but is
  **not** wired into either job — it's a manual admin script that grants
  against both catalogs by name on purpose, not something that should run
  on a schedule.

## Still open

- Secrets referenced by the notebooks (`mavenScope/mongodb-conn-string`,
  `kafka-bootstrap-server`, `kafka-api-key`, `kafka-api-secret`) must exist
  in the target workspace already — this bundle doesn't create a secret
  scope.
- The quarantine-coverage gap in the dimension/master-data Silver notebooks
  (Regions/Calendar/Promotions/Customers/Products/Store-country all
  currently drop failing rows with no `_quarantine` table) is unchanged
  here — that's still queued up from earlier in this conversation.
