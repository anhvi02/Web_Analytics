# dbt Lakehouse Pipeline: SQL Server → Databricks (Free Edition) → Power BI

## Context

This project holds real business reporting requirements (gross-profit-by-campaign, funnel drop-off, refund rate, most-viewed URL, sales by device, conversion by UTM source, return-customer rate, cohort LTV, rolling 28-day sales, net revenue by product) but no working pipeline yet — just a bare `dbt init` scaffold with dummy example models. The source is a live OLTP-style schema on SQL Server (Maven-Fuzzy-Factory-style web analytics data: sessions, pageviews, orders, order items, refunds, products). The target is Databricks Free Edition (Unity Catalog + serverless SQL warehouses, no cost), and Power BI connects to the resulting gold-layer tables as its star-schema source.

Goal: a clean, incrementally-buildable dbt project whose gold layer maps directly onto the 10 reporting requirements without needing a rebuild later, and that plugs into the existing Power BI/DAX standards (star schema, `DIVIDE()`, branching measures, empty measure tables, fxCalendar date table built separately in Power Query).

## Decisions locked in

- **No true ROI** is possible — there's no ad-spend/cost table in the source. Build **Gross Profit by Campaign** (revenue − COGS, via `order_items.cogs_usd`) as the profitability proxy instead. Labeled explicitly as a proxy, not ROI.
- **Attribution model**: build both `first_touch_campaign_key` and `last_touch_campaign_key` on the user dimension; default the Power BI measure to first-touch (acquisition-quality question), swappable. Not fully settled — revisit once real data is in.
- **Target**: Databricks Free Edition. Flow is SQL Server → Databricks → Power BI.
- **EL mechanism** (SQL Server → Databricks bronze) is unverified — Free Edition serverless compute may or may not allow outbound JDBC to a public SQL Server. Spike first before committing.
- Both SQL Server and Databricks credentials are currently plaintext in `~/.dbt/profiles.yml` — move to `env_var()`.
- **"Return customer"**: build both repeat-*session* and repeat-*order* flags; let the Power BI measure choose.

## Progress log

- [x] Confirmed `dbt debug` connects to live SQL Server source (`xomdata_dataset.web_analytics`).
- [x] Confirmed live row counts: `website_pageviews` 1,188,124 · `website_sessions` 472,871 · `orders` 32,313 · `order_items` 40,025 · `order_item_refunds` 1,731 · `products` 4.
  - Confirms the materialization split: pageviews/sessions are large enough that incremental merge matters; orders/order_items/refunds/products are cheap enough for full-refresh tables.
- [x] Confirmed `website_sessions.device_type` distinct values: `mobile` (145,844), `desktop` (327,027) — matches ERD assumption, only two values, no nulls seen.
- [ ] Confirm `utm_source`, `utm_campaign`, `is_repeat_session` distinct values and null patterns for direct/organic traffic.
- [ ] Spike EL step: serverless Databricks notebook + JDBC read from SQL Server → bronze Delta table in Unity Catalog. Fallback if network egress is blocked: local script (pyodbc/pymssql → Databricks SQL Connector).
- [ ] Move credentials in `~/.dbt/profiles.yml` to `env_var()`.
- [ ] Install `dbt-databricks` adapter; activate the (currently commented-out) Databricks target profile.
- [ ] Scaffold `packages.yml` (dbt_utils) and staging/intermediate/marts folder structure.
- [ ] Build staging models (1:1 source-conformed views).
- [ ] Build intermediate models (funnel classification, attribution, rollups).
- [ ] Build mart dim/fact models.
- [ ] Add dbt tests across all layers.
- [ ] `dbt build` end-to-end and spot-check against source row counts.
- [ ] Connect Power BI to the Databricks SQL warehouse; verify relationships resolve cleanly.

## Phase 0 — Verify before building

1. **Confirm live schema** via `database_queries/database_explore.sql` — distinct values for `device_type` (done), `utm_source`, `is_repeat_session`, and nullability of `utm_*` for direct/organic traffic. Don't hardcode `accepted_values` dbt tests until fully confirmed.
2. **Spike the EL step**: serverless Databricks notebook doing `spark.read.jdbc(...)` against SQL Server, writing to a `bronze` schema in Unity Catalog. If blocked, fall back to a local Python script (pyodbc/pymssql to read, Databricks SQL Connector/REST API to write), run manually or on a local schedule. Land one raw Delta table per source table, stamped with `_ingested_at` (no source table has a native `updated_at`, so this becomes the freshness watermark).
3. Move credentials to `env_var('SQLSERVER_PASSWORD')` / `env_var('DATABRICKS_TOKEN')`. Activate the Databricks target profile once catalog/schema names for bronze/staging/marts are decided.
4. Add `dbt-databricks` (not installed yet — only `dbt-sqlserver` is).

## Target model structure

```
models/
  staging/web_analytics/
    _web_analytics__sources.yml           -- sources.yml pointing at bronze schema, freshness on _ingested_at
    _web_analytics__staging_models.yml
    stg_web_analytics__website_sessions.sql
    stg_web_analytics__website_pageviews.sql   -- also produces pageview_url_clean (strip query string/trailing slash)
    stg_web_analytics__orders.sql
    stg_web_analytics__order_items.sql
    stg_web_analytics__order_item_refunds.sql
    stg_web_analytics__products.sql
  intermediate/
    int_pageviews__funnel_step.sql        -- incremental; joins seed funnel_step_url_map.csv
    int_sessions__funnel_flags.sql        -- collapses per-session pageviews into reached_homepage/product/cart/checkout/thank_you booleans
    int_sessions__order_rollup.sql
    int_order_items__refund_rollup.sql
    int_users__first_last_touch.sql       -- first_touch_campaign_key / last_touch_campaign_key via window functions
    int_users__activity_rollup.sql        -- repeat-session and repeat-order flags, cohort_month
  marts/core/
    dim_products.sql
    dim_campaign.sql                       -- surrogate key via dbt_utils.generate_surrogate_key(utm_source, utm_campaign, utm_content); sentinel row '(direct/none)' for nulls
    dim_users.sql                          -- first/last touch campaign keys, repeat flags, cohort_month
    dim_date.sql                           -- dbt_utils.date_spine; gap-fill only, NOT the Power BI calendar table
  marts/web_analytics/
    fct_pageviews.sql                      -- incremental merge
    fct_sessions.sql                       -- incremental merge; carries funnel-flag booleans + campaign_key
    fct_orders.sql                         -- full-refresh table; denormalizes device_type + campaign_key from originating session
    fct_order_items.sql                    -- full-refresh table (refunds are late-arriving children — see below)
    fct_daily_sales.sql                    -- full-refresh table, joined against dim_date for gap-free daily grain
```

Add `packages.yml` with `dbt-labs/dbt_utils` (needed for `generate_surrogate_key`, `date_spine`, `expression_is_true`).

## Key design decisions to implement

- **Materialization split** (not "incremental everywhere"): staging = views. `fct_pageviews`/`fct_sessions` = incremental merge (high volume, effectively append-only, immutable once written). `fct_orders`/`fct_order_items` = **full-refresh table**, not incremental — refunds can land on an order_item weeks after creation, so filtering by the row's own `created_at` watermark would silently miss them. `dim_*` = full-refresh table (small, and aggregate flags like repeat-customer status change retroactively on old rows). Revisit `fct_order_items` to a bounded-lookback incremental merge only if full-refresh cost becomes real.
- **Funnel classification**: seed-driven (`seeds/funnel_step_url_map.csv`: `step_order, step_name, url_pattern, match_type`), not inline regex/CASE — testable and maintainable. `int_pageviews__funnel_step` classifies each pageview; `int_sessions__funnel_flags` collapses to per-session booleans; these land directly on `fct_sessions` at session grain (not pre-aggregated) so Power BI can compute step-over-step conversion under any slicer via `DIVIDE()`.
- **Star-schema hygiene for Power BI import**: `dim_campaign` needs the `'(direct/none)'` sentinel so null-UTM sessions don't create unmatched relationship keys. Facts denormalize what they need (`device_type`, `campaign_key`) directly rather than chaining fact→fact→dim in the Power BI model.
- **`dim_date` in the lakehouse**: built to guarantee a gap-free daily grain for `fct_daily_sales` (a day with zero sessions must appear as zero, not be absent) — matters for cohort bucketing and the rolling-28-day sales measure. It is explicitly **not** a replacement for Power BI's own fxCalendar-based Date Table, which still owns all DAX time intelligence.
- **Rolling 28-day sales and cohort LTV cumulative math**: computed in Power BI DAX (`CALCULATE` + `DATESINPERIOD` over fxCalendar) against the clean `fct_daily_sales`/`fct_orders` grain — not pre-materialized in dbt.

## Requirement → model mapping

| # | Requirement | Model(s) |
|---|---|---|
| 1 | Gross-profit-by-campaign (ROI proxy) | `fct_sessions`/`fct_orders` (revenue − cogs_usd) × `dim_campaign`, explicitly labeled as profitability proxy, not ROI |
| 2 | Funnel drop-off | `int_pageviews__funnel_step` → `int_sessions__funnel_flags` → `fct_sessions` booleans |
| 3 | Refund rate by product | `int_order_items__refund_rollup` → `fct_order_items` × `dim_products` |
| 4 | Most viewed URL | `fct_pageviews.pageview_url_clean` |
| 5 | Sales by device | `fct_orders.device_type` (denormalized) |
| 6 | Conversion rate by utm_source | `fct_sessions` × `dim_campaign` |
| 7 | Return customer rate by campaign | `dim_users` (first/last touch key + repeat flags) × `dim_campaign` |
| 8 | Cohort LTV by first-session month | `dim_users.cohort_month` + `fct_orders`, cumulative math in DAX |
| 9 | Rolling 28-day sales | `fct_daily_sales` + `dim_date` (gap-fill), rolling window in DAX |
| 10 | Net revenue by product | `fct_order_items` (price_usd − refund_amount_usd) × `dim_products` |

## Testing

- Staging: `unique`+`not_null` on PKs, `relationships` child→parent, `accepted_values` on categorical columns (only after Phase 0 confirms real distinct values).
- Intermediate: singular test asserting % of pageviews with null `step_name` in `int_pageviews__funnel_step` stays under a threshold (catches URL pattern drift).
- Marts: `unique`+`not_null` on fact/dim grain keys, `relationships` fact→dim, `dbt_utils.expression_is_true` for `refund_amount_usd <= price_usd`, and a row/sum reconciliation test between `stg_order_items` and `fct_order_items` to catch join fan-out. Turn on `store_failures: true` for reconciliation tests.

## Verification

1. `dbt debug` against both profiles (SQL Server source, Databricks target) after credentials move to env vars.
2. Phase 0 EL spike: confirm a bronze Delta table lands in Unity Catalog with correct row counts matching a `SELECT COUNT(*)` on the SQL Server source table.
3. `dbt build` (runs models + seeds + tests together) on the full project; all tests green, `store_failures` tables empty.
4. Spot-check each of the 10 gold-layer requirement models with a manual query against expected values (e.g. total sessions/orders reconcile against source row counts; funnel flags monotonically decrease step-over-step for a sample of sessions).
5. Connect Power BI to the Databricks SQL warehouse, import the mart-layer tables, confirm relationships resolve with no blank/unmatched members (checks the `dim_campaign` sentinel row worked), and build one measure per requirement to confirm the grain supports it.
