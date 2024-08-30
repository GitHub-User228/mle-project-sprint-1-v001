import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from steps.prepare_flats import extract, prepare
from steps.common import create_table, load, transform
from steps.messages import (
    send_telegram_success_message,
    send_telegram_failure_message,
)


with DAG(
    dag_id="prepare_flats",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="@once",
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
) as dag:

    create_table_step = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
        op_kwargs={
            "postgres_hook_name": "destination_db",
            "table_name": "prepared_flats",
        },
    )
    extract_step = PythonOperator(task_id="extract", python_callable=extract)
    transform_step = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_kwargs={"transform_func": prepare},
    )
    load_step = PythonOperator(
        task_id="load",
        python_callable=load,
        op_kwargs={
            "postgres_hook_name": "destination_db",
            "table_name": "prepared_flats",
            "replace_index": ["flat_id"],
            "replace": True,
            "clear_table": False,
            "commit_every": 0,
        },
    )

    extract_step >> transform_step
    [create_table_step, transform_step] >> load_step
