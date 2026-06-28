# TPC-H — Unity Catalog Tagging Stress Test

A Lakehouse Plumber (LHP) example project that builds a full **medallion pipeline**
on top of the read-only `samples.tpch` dataset (present in every Databricks
metastore) and decorates **every table and many columns** with Unity Catalog tags.
Its purpose is to stress-test LHP's UC-tagging event hook in a real Spark
Declarative Pipeline.

## What it builds

A single SDP/DLT pipeline (`tpch_pipeline`) with **17 tables**:

| Layer | Tables |
|-------|--------|
| Bronze (streaming passthrough of `samples.tpch`) | `customer`, `orders`, `lineitem`, `part`, `supplier`, `partsupp`, `nation`, `region` |
| Silver (cleansed/conformed, incl. a stream-static join) | `customer_dim`, `supplier_dim`, `part_dim`, `nation_dim`, `orders_fct`, `lineitem_fct` |
| Gold (materialized views) | `revenue_by_nation_mv`, `customer_order_summary_mv`, `monthly_revenue_mv` |

## Tagging

- Every tag key uses an **`lhp_` prefix** (e.g. `lhp_data_layer`) so the demo never
  collides with *governed* tags — UC tag policies that constrain a key's allowed
  values — which would otherwise fail the tag write.
- **Table-level tags** live on each `write_target.tags` (e.g. `lhp_data_layer`,
  `lhp_domain`, `lhp_owner`, `lhp_cost_center`, `lhp_sensitivity`, key-only
  `lhp_contains_pii` / `lhp_certified`).
- **Column-level tags** live on columns of the schema files in `schemas/` referenced
  by `table_schema` (e.g. `lhp_classification: pii`, `lhp_semantic_type`,
  `lhp_masking_policy`, key-only `lhp_contains_pii`). Honored only for YAML/JSON schema files.
- Tagging is **on by default** — declaring `tags` opts in. The optional `uc_tagging`
  block in `lhp.yaml` tunes it (this project turns on reconcile mode); set
  `uc_tagging.enabled: false` to disable.

LHP does **not** put tags in the table DDL (SDP can't set them at create time).
Instead it generates `generated/<env>/tpch_pipeline/_uc_tagging_hook.py` — a
`@dp.on_event_hook` that applies declared tags via the Unity Catalog
*Entity Tag Assignments* REST API **during the run**: on `update_progress` `RUNNING`
(streaming tables) and on the terminal state (the gold materialized views, which
materialize later), each entity tagged at most once, fanning across
`tag_update_concurrency` threads (default 16). Tagging on `RUNNING` means tag-write
failures show up as event-log warnings while the run is live; tagging never fails the
pipeline.

## Prerequisites

- A Unity Catalog you can create schemas in. Defaults to `liam_perritt` with schemas
  `tpch_bronze` / `tpch_silver` / `tpch_gold` — edit `substitutions/dev.yaml` and
  `databricks.yml` if you want a different catalog.
- The pipeline's run-as identity needs **`APPLY TAG`** permission to assign UC tags.

## Generate

```bash
cd Example_Projects/tpch
lhp generate --env dev
```

This writes the pipeline Python under `generated/dev/tpch_pipeline/` (including
`_uc_tagging_hook.py`) and the Asset Bundle resource under
`resources/lhp/tpch_pipeline.pipeline.yml`.

## Deploy & run (Databricks Asset Bundle)

```bash
# Set your workspace host in databricks.yml first.
databricks bundle deploy -t dev
databricks bundle run tpch_pipeline_pipeline -t dev
```

After the run completes, inspect tags in Unity Catalog — every table and tagged
column should carry its declared tags. A second run is a no-op for unchanged tags
(additive upsert). Set `uc_tagging.remove_undeclared_tags: true` in `lhp.yaml` to
reconcile (delete tags not declared here) instead.
