# Databricks resources

`nyc_mobility_job.yml` is a deliberately minimal Asset Bundle scaffold. It
references the existing pipeline test runner at
`src/shared/monitoring/03_run_pipeline_tests.py` and passes `src` as the
pipeline root. Set `cluster_id` to an existing Databricks cluster before
deployment.

The SQL pipeline is intentionally not expanded into production job tasks here:
task sequencing, catalog/schema targets, secrets, and compute policy are not
validated in this repository. Add those tasks only after the corresponding
Databricks environment contracts are agreed.
