import json
import string
import time

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import ObjectStoragePath
from airflow.timetables.interval import CronDataIntervalTimetable
from schemas.cima import ATC_SCHEMA, LAB_SCHEMA

BASE_URL = "https://cima.aemps.es/cima/rest"
BASE_PATH = ObjectStoragePath("gs://guillemdiaz-suministro/")

# BigQuery configuration
BQ_DATASET = "suministro_bronze"
BUCKET = "guillemdiaz-suministro"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
}


@dag(
    dag_id="cima_maestras_ingestion_monthly",
    start_date=pendulum.datetime(2026, 9, 25, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 6 1 * *", timezone="UTC"),
    catchup=False,
    max_active_runs=1,
)
def cima_maestras_pipeline():

    @task
    def extract_maestras(**context) -> str:
        """
        Fetch the full ATC-code (maestra=7) and laboratory (maestra=6)
        reference catalogs from CIMA.

        The maestras endpoint has no "list everything" mode: it requires at least
        one non-empty search condition, or it returns 204 No Content even for a
        valid maestra id. This loops the 'nombre' filter over every letter a-z,
        paginating within each letter ('pagina' is only sent from page 2 onward,
        since including it on page 1 also triggers a 204), and removes duplicates
        results across letters.

        Catalog items don't consistently expose an 'id' field (ATC entries
        only have 'codigo' + 'nombre', and labs only have 'nombre'), so the logic
        of removing duplicates falls back through id => codigo => nombre, using
        whichever field is present.

        Writes each catalog as a separate file to a date-partitioned bronze path
        in GCS (maestras/<ds>/atc_codes.json and maestras/<ds>/laboratorios.json).
        Intended as a static/slowly-changing reference load, not something that
        needs daily reruns.

        Returns:
            str: the GCS path of the written JSON file.
        """

        def safe_fetch_maestra(maestra_id):
            """
            Fetch one maestras catalog (by maestra_id) by searching every
            letter a-z and merging paginated results, removing duplicates by
            id/codigo/nombre (whichever field is present).
            """
            seen_ids = {}
            for letter in string.ascii_lowercase:
                page = 1
                while True:
                    params = {"maestra": maestra_id, "nombre": letter}
                    if page > 1:
                        params["pagina"] = page

                    res = requests.get(
                        f"{BASE_URL}/maestras",
                        params=params,
                        headers=HEADERS,
                        timeout=30,
                    )

                    if res.status_code == 204:
                        break

                    if res.status_code != 200:
                        print(
                            f"Maestra {maestra_id} letter '{letter}' failed on page {page} "
                            f"with status {res.status_code}"
                        )
                        break

                    try:
                        data = res.json()
                    except ValueError:
                        break

                    results = data.get("resultados", [])
                    if not results:
                        break

                    for item in results:
                        key = item.get("id") or item.get("codigo") or item.get("nombre")
                        if key is not None:
                            seen_ids[key] = item

                    page += 1
                    time.sleep(0.1)

            return list(seen_ids.values())

        combined = {"atc_codes": safe_fetch_maestra(7), "laboratorios": safe_fetch_maestra(6)}

        ds = context["ds"]

        atc_target = BASE_PATH / "maestras" / ds / "atc_codes.json"
        with atc_target.open("w") as f:
            for item in combined["atc_codes"]:
                f.write(json.dumps(item) + "\n")

        labs_target = BASE_PATH / "maestras" / ds / "laboratorios.json"
        with labs_target.open("w") as f:
            for item in combined["laboratorios"]:
                f.write(json.dumps(item) + "\n")

        print(
            f"Extracted {len(combined['atc_codes'])} ATC codes, "
            f"{len(combined['laboratorios'])} labs."
        )
        return str(atc_target)

    # BigQuery loading tasks
    # -------------------------------------------------------------------------
    # - Since these are slow-moving reference catalogs, WRITE_TRUNCATE is used
    #   without time partitioning to replace the entire table with the freshest
    #   data every month.
    # - autodetect=False prevents BigQuery from misidentifying data types across days
    # - ignore_unknown_values=True ensures the DAG won't crash if the AEMPS API
    #   adds a new field to the JSON response.
    # -------------------------------------------------------------------------
    load_atc_codes_bq = GCSToBigQueryOperator(
        task_id="load_atc_codes_bq",
        bucket=BUCKET,
        source_objects=["maestras/{{ ds }}/atc_codes.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{BQ_DATASET}.atc_codes",
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema_fields=ATC_SCHEMA,
        ignore_unknown_values=True,
    )

    load_laboratorios_bq = GCSToBigQueryOperator(
        task_id="load_laboratorios_bq",
        bucket=BUCKET,
        source_objects=["maestras/{{ ds }}/laboratorios.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{BQ_DATASET}.laboratorios",
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        schema_fields=LAB_SCHEMA,
        ignore_unknown_values=True,
    )

    # Pipeline logic
    maestras_extracted = extract_maestras()

    maestras_extracted >> load_atc_codes_bq
    maestras_extracted >> load_laboratorios_bq


cima_maestras_pipeline()
