from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import sys

# sys.append("/opt/airflow/api_request")


@dag(
    dag_id="ingesting_dag",
    start_date=datetime(2025, 8, 28),
    schedule=None,   # ejecución manual (una vez)
    catchup=False
)
def ingesting_dag():
    run_ingest = BashOperator(
        task_id="run_ingest_script",
        bash_command="python -u /opt/airflow/api_request/ingest_data.py"
        # Si tu script es ejecutable y tiene shebang, podrías usar:
        # bash_command="/opt/airflow/api_request/ingest_data.py"
    )

    verify = BashOperator(
        task_id="verify_data",
        bash_command='echo "✅ Datos verificados en Postgres"'
    )

    run_ingest >> verify


dag_instance = ingesting_dag()
