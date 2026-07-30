Baseline reports for `nx-store` load benchmarks.

These files use `report_schema_version = 1` and are the inputs future
regression checks compare against. They are intentionally separate from
`reports/load/`, which remains historical benchmark output.

Initial baselines are normalized from existing committed load reports, so
`resources.rss_bytes` is `null` until fresh v1 reports replace them.
