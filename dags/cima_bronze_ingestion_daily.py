import json
import time

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.sdk import ObjectStoragePath
from airflow.timetables.interval import CronDataIntervalTimetable

BASE_URL = "https://cima.aemps.es/cima/rest"
BASE_PATH = ObjectStoragePath("gs://guillemdiaz-suministro/")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
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

        while True:
            response = requests.get(
                f"{BASE_URL}/psuministro",
                params={"pagina": current_page},
                headers=HEADERS,
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

        for cn in cn_list:
            try:
                response = requests.get(
                    f"{BASE_URL}/presentacion/{cn}", headers=HEADERS, timeout=30
                )

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

        for nregistro in nregistro_list:
            try:
                response = requests.get(
                    f"{BASE_URL}/medicamento",
                    params={"nregistro": nregistro},
                    headers=HEADERS,
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
                    print(f"WARNING: Skipped nregistro {nregistro} - Status {response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"ERROR: Network failure skipping nregistro {nregistro} - {str(e)}")

            time.sleep(0.1)

        ds = context["ds"]
        target = BASE_PATH / "medicamentos" / ds / "data.json"
        with target.open("w") as f:
            for item in medicamentos_data:
                f.write(json.dumps(item) + "\n")

        return str(target)

    cns = extract_psuministro()
    nregistros = extract_presentaciones(cns)
    extract_medicamentos(nregistros)


cima_pipeline()
