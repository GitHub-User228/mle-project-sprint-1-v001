import pandas as pd
from typing import List
from sqlalchemy import inspect
from collections.abc import Callable
from ensure import ensure_annotations
from airflow.providers.postgres.hooks.postgres import PostgresHook

from init_tables import metadata, tables


@ensure_annotations
def create_table(table_name: str, postgres_hook_name: str, **kwargs):
    """
    Creates a PostgreSQL table if it doesn't already exist.

    Args:
        table_name (str):
            The name of the table to create.
            It must be defined in the table.py configuration.
        postgres_hook_name (str):
            The name of the PostgreSQL hook to use.
        **kwargs:
            Additional keyword arguments.

    Raises:
        ValueError:
            If the table name is not found in the table.py configuration.
        Exception:
            If an exception occurs while creating the table.
    """

    if table_name not in tables:
        raise ValueError(f"Table {table_name} not found in table.py")

    try:
        hook = PostgresHook(postgres_hook_name)
        conn = hook.get_sqlalchemy_engine()

        if not inspect(conn).has_table(table_name):
            metadata.create_all(conn, tables=[table_name])
    except Exception as e:
        raise Exception(
            f"An exception occured while creating table {table_name}"
        ) from e


@ensure_annotations
def transform(transform_func: Callable, **kwargs):
    """
    Transforms the extracted data via the transform_func.
    Then pushes the transformed data to the next task.

    Args:
        transform_func (callable):
            A function that takes the extracted data as input
            and returns the transformed data.
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).

    Raises:
        Exception:
            If an exception occurs while transforming the data.
    """

    try:
        # 1. Get the extracted data
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="extract", key="extracted_data")

        # 2. Transformations
        data = transform_func(data)

        # 3. Pushing the transformed data to the next task
        ti.xcom_push("transformed_data", data)

    except Exception as e:
        raise Exception(f"An exception occured while transforming data") from e


@ensure_annotations
def load(
    postgres_hook_name: str,
    table_name: str,
    replace_index: list,
    replace: bool = True,
    clear_table: bool = False,
    commit_every: int = 0,
    **kwargs,
):
    """
    Loads data into a PostgreSQL table.

    Args:
        postgres_hook_name (str):
            The name of the PostgreSQL hook to use.
        table_name (str):
            The name of the table to load the data into.
        replace_index (list[str]):
            The columns to use as the index when replacing existing
            rows.
        replace (bool, optional):
            Whether to replace existing rows instead of insert.
            Defaults to True.
        clear_table (bool, optional):
            Whether to clear the table before loading the data.
            Defaults to False.
        commit_every (int, optional):
            The maximum number of rows to insert in one transaction.
            Set to 0 to insert all rows in one transaction.
            Defaults to 0.
        **kwargs:
            Additional keyword arguments, including the task
            instance (ti).

    Raises:
        Exception:
            If an exception occurs while loading the data.
    """

    try:

        if not all([isinstance(i, str) for i in replace_index]):
            raise ValueError("replace_index must be a list of strings")

        # 1. Get the transformed data
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids="transform", key="transformed_data")

        # 2. Load the data into the destination database via PostgresHook
        hook = PostgresHook(postgres_hook_name)
        if clear_table:
            hook.run(f"DELETE FROM {table_name}")

            hook.insert_rows(
                table=table_name,
                replace=replace,
                target_fields=data.columns.tolist(),
                replace_index=replace_index,
                rows=data.values.tolist(),
                commit_every=commit_every,
            )
    except Exception as e:
        raise Exception(
            f"An exception occurred while loading data into table {table_name}"
        ) from e
