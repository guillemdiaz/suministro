import json
import time
from pathlib import Path

import pendulum
import requests
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import ObjectStoragePath, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from requests.adapters import HTTPAdapter
from schemas.cima import MEDICAMENTOS_SCHEMA, PRESENTACIONES_SCHEMA, PSUMINISTRO_SCHEMA
from urllib3.util.retry import Retry

BASE_URL = "https://cima.aemps.es/cima/rest"
BASE_PATH = ObjectStoragePath("gs://guillemdiaz-suministro/")

# BigQuery configuration
BQ_DATASET = "suministro_bronze"
BUCKET = "guillemdiaz-suministro"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}


def make_session() -> requests.Session:
    """
    Creates a requests Session with connection pooling and
    automatic retries for transient server errors and timeouts.
    """
    session = requests.Session()
    session.headers.update(HEADERS)
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=2),
}


@dag(
    dag_id="cima_bronze_ingestion_daily",
    start_date=pendulum.datetime(2026, 9, 25, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 6 * * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
)
def cima_pipeline():

    @task
    def extract_psuministro(**context) -> list[str]:
        """
        Fetch the full daily supply-shortage list from CIMA's psuministro endpoint,
        paginating until no more results are returned.

        Writes the raw records to a date-partitioned bronze path in
        GCS (psuministro/<ds>/data.json).

        Returns:
            list[str]: distinct 'cn' (código nacional) values found in today's
            shortage list, to be enriched by extract_presentaciones.
        """
        all_records = []
        current_page = 1

        with make_session() as session:
            while True:
                response = session.get(
                    f"{BASE_URL}/psuministro",
                    params={"pagina": current_page},
                    timeout=30,
                )
                response.raise_for_status()

                try:
                    data = response.json()
                except ValueError:
                    break

                results = data.get("resultados", [])
                if not results:
                    break

                all_records.extend(results)
                current_page += 1

        ds = context["ds"]
        target = BASE_PATH / "psuministro" / ds / "data.json"
        with target.open("w") as f:
            for item in all_records:
                f.write(json.dumps(item) + "\n")

        distinct_cns = list({str(record["cn"]) for record in all_records if "cn" in record})
        print(f"Successfully extracted {len(distinct_cns)} distinct CNs.")
        return distinct_cns

    @task
    def extract_presentaciones(cn_list: list[str], **context) -> list[str]:
        """
        Look up commercial-presentation details for each distinct 'cn' found in
        today's shortage list, one call per cn via GET presentacion/{cn}.

        A 204 response means the cn has no matching presentation and is skipped
        as expected. Any other non-200 status or network error is logged and skipped
        without failing the task.

        Writes the collected presentation records to a date-partitioned bronze path
        in GCS (presentaciones/<ds>/data.json).

        Returns:
            list[str]: distinct 'nregistro' values pulled from the
            presentation records, to be enriched by extract_medicamentos.
        """
        presentaciones_data = []
        distinct_nregistros = set()

        with make_session() as session:
            for cn in cn_list:
                try:
                    response = session.get(f"{BASE_URL}/presentacion/{cn}", timeout=30)

                    if response.status_code == 200:
                        try:
                            data = response.json()
                            presentaciones_data.append(data)
                            if "nregistro" in data:
                                distinct_nregistros.add(str(data["nregistro"]))
                        except ValueError:
                            pass
                    elif response.status_code == 204:
                        pass  # Expected behavior for missing items
                    else:
                        print(f"WARNING: Skipped CN {cn} - Status {response.status_code}")

                except requests.exceptions.RequestException as e:
                    print(f"ERROR: Network failure skipping CN {cn} - {str(e)}")

                time.sleep(0.1)

        ds = context["ds"]
        target = BASE_PATH / "presentaciones" / ds / "data.json"
        with target.open("w") as f:
            for item in presentaciones_data:
                f.write(json.dumps(item) + "\n")

        print(f"Extracted {len(distinct_nregistros)} distinct nregistros.")
        return list(distinct_nregistros)

    @task
    def extract_medicamentos(nregistro_list: list[str], **context) -> str:
        """
        Look up medication-level details (name, lab, ATC codes) for each distinct
        'nregistro' found via extract_presentaciones, one call per nregistro via
        GET medicamento?nregistro=X.

        A 204 response means the nregistro has no matching medication and is
        skipped as expected. Any other non-200 status or network error is logged
        and skipped without failing the task.

        Writes the collected medication records to a date-partitioned bronze path
        in GCS (medicamentos/<ds>/data.json).

        Returns:
            str: the GCS path of the written JSON file.
        """
        medicamentos_data = []

        with make_session() as session:
            for nregistro in nregistro_list:
                try:
                    response = session.get(
                        f"{BASE_URL}/medicamento",
                        params={"nregistro": nregistro},
                        timeout=30,
                    )

                    if response.status_code == 200:
                        try:
                            medicamentos_data.append(response.json())
                        except ValueError:
                            pass
                    elif response.status_code == 204:
                        pass
                    else:
                        print(
                            f"WARNING: Skipped nregistro {nregistro} - "
                            f"Status {response.status_code}"
                        )

                except requests.exceptions.RequestException as e:
                    print(f"ERROR: Network failure skipping nregistro {nregistro} - {str(e)}")

                time.sleep(0.1)

        ds = context["ds"]
        target = BASE_PATH / "medicamentos" / ds / "data.json"
        with target.open("w") as f:
            for item in medicamentos_data:
                f.write(json.dumps(item) + "\n")

        return str(target)

    # BigQuery loading tasks
    # --------------------------------------------------------------------------
    # - Concatenates "${{ ds_nodash }}" to the table name to dynamically route
    #   each day's data into a specific date partition.
    # - autodetect=False prevents BigQuery from misidentifying data types across days
    # - ignore_unknown_values=True ensures the DAG won't crash if the AEMPS API
    #   adds a new field to the JSON response.
    # --------------------------------------------------------------------------
    load_psuministro_bq = GCSToBigQueryOperator(
        task_id="load_psuministro_bq",
        bucket=BUCKET,
        source_objects=["psuministro/{{ ds }}/data.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{BQ_DATASET}.psuministro" + "${{ ds_nodash }}",
        time_partitioning={"type": "DAY"},
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema_fields=PSUMINISTRO_SCHEMA,
        ignore_unknown_values=True,
    )

    load_presentaciones_bq = GCSToBigQueryOperator(
        task_id="load_presentaciones_bq",
        bucket=BUCKET,
        source_objects=["presentaciones/{{ ds }}/data.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{BQ_DATASET}.presentaciones" + "${{ ds_nodash }}",
        time_partitioning={"type": "DAY"},
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema_fields=PRESENTACIONES_SCHEMA,
        ignore_unknown_values=True,
    )

    load_medicamentos_bq = GCSToBigQueryOperator(
        task_id="load_medicamentos_bq",
        bucket=BUCKET,
        source_objects=["medicamentos/{{ ds }}/data.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{BQ_DATASET}.medicamentos" + "${{ ds_nodash }}",
        time_partitioning={"type": "DAY"},
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema_fields=MEDICAMENTOS_SCHEMA,
        ignore_unknown_values=True,
    )

    # dbt Transformations via Cosmos
    # --------------------------------------------------------------------------
    # Dynamically locate the absolute dbt project path relative to this DAG file
    # so it works across Docker, local virtual environments, and GitHub Actions.
    # --------------------------------------------------------------------------
    DAG_DIR = Path(__file__).resolve().parent
    DBT_PROJECT_PATH = DAG_DIR / "dbt" / "cima_dbt"

    dbt_transformations = DbtTaskGroup(
        group_id="transform_bronze_to_marts",
        project_config=ProjectConfig(str(DBT_PROJECT_PATH)),
        profile_config=ProfileConfig(
            profile_name="cima_dbt",
            target_name="prod",
            profiles_yml_filepath=str(DBT_PROJECT_PATH / "profiles.yml"),
        ),
    )

    # Pipeline logic
    cns = extract_psuministro()
    nregistros = extract_presentaciones(cns)
    meds = extract_medicamentos(nregistros)

    cns >> load_psuministro_bq
    nregistros >> load_presentaciones_bq
    meds >> load_medicamentos_bq

    # Tells Airflow to wait until all load tasks finish before running dbt
    [load_psuministro_bq, load_presentaciones_bq, load_medicamentos_bq] >> dbt_transformations


cima_pipeline()
