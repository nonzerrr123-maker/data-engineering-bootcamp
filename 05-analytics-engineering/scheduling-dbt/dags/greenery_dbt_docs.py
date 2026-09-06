from pathlib import Path
import shutil

from airflow import DAG
from airflow.utils import timezone
from cosmos import ProfileConfig
from cosmos.operators import DbtDocsOperator
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

DBT_PROJECT_DIR = "/opt/airflow/dbt/greenery"

with DAG(
    dag_id="greenery_dbt_docs",
    schedule=None,
    start_date=timezone.datetime(2023, 3, 17),
    catchup=False,
    tags=["DEB", "Skooldio", "greenery", "docs"],
):
    profile_config = ProfileConfig(
        profile_name="greenery",
        target_name="dev",
        profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
            conn_id="bigquery_dbt",
            profile_args={
                "dataset": "dbt_suntisuk",
                "location": "asia-southeast1",
            },
        ),
    )

    def persist_docs(project_dir, **kwargs):
        source = Path(project_dir) / "target"
        destination = Path(DBT_PROJECT_DIR) / "target"
        shutil.rmtree(destination, ignore_errors=True)
        shutil.copytree(source, destination)

    DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir=DBT_PROJECT_DIR,
        profile_config=profile_config,
        callback=persist_docs,
    )
