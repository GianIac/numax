Baseline reports for `nx-core` load benchmarks.

These files use `report_schema_version = 1` and are the inputs future
regression checks compare against. They are intentionally separate from
`reports/load/`, which remains historical benchmark output.

Naming convention:

- `<scenario-profile>.json` for committed baselines;
- `*-smoke.json` for short PR or shadow-CI scenarios;
- longer release-gate scenarios can be added beside them once they are stable.

Initial baselines are normalized from existing committed load reports, so
`resources.rss_bytes` is `null` until fresh v1 reports replace them.
