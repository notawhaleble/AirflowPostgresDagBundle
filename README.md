# Airflow Postgres DAG Bundle

Airflow 3 DAG bundle backend that reads versioned DAG zip archives from the
`versioned_dagcode` table in the same PostgreSQL database used by Airflow's
metadata database.

The Airflow bundle `name` is treated as `versioned_dagcode.project_name`.
`versioned_dagcode.commit_hash` is treated as the bundle version.

Example `airflow.cfg`:

```ini
[dag_processor]
dag_bundle_config_list = [
  {
    "name": "my_project",
    "classpath": "airflow_postgres_dag_bundle.PostgresDagBundle",
    "kwargs": {}
  }
]
```

For each configured bundle, refresh checks the latest `commit_hash` by
`uploaded` for that `project_name`. When the version changes, all rows for that
project/version are read and their `source_code` zip payloads are extracted into
the installed bundle directory.
