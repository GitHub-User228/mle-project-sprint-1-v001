import pandas as pd
from sqlalchemy import (
    inspect,
    MetaData,
    Table,
    Column,
    Boolean,
    Integer,
    Float,
    UniqueConstraint,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table(**kwargs):
    """
    Creates a SQLAlchemy table named "prepared_flats" with the
    specified columns, if the table does not already exist in
    the destination database.

    The table also has a unique constraint on the "flat_id" column.
    """

    hook = PostgresHook("destination_db")
    conn = hook.get_sqlalchemy_engine()
    metadata = MetaData()
    table = Table(
        "prepared_flats",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("flat_id", Integer),
        Column("floor", Integer),
        Column("is_apartment", Boolean),
        Column("kitchen_area", Float),
        Column("living_area", Float),
        Column("rooms", Integer),
        Column("studio", Boolean),
        Column("total_area", Float),
        Column("target", Float),
        Column("building_id", Integer),
        Column("build_year", Integer),
        Column("building_type_int", Integer),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("ceiling_height", Float),
        Column("flats_count", Integer),
        Column("floors_total", Integer),
        Column("has_elevator", Boolean),
        UniqueConstraint("flat_id", name="unique_flat_id_constraint"),
    )

    if not inspect(conn).has_table(table.name):
        metadata.create_all(conn)


def extract(**kwargs):
    """
    Extracts data from the source database via PostgresHook, merges
    the flats and buildings data, and pushes the merged dataframe to
    the next task.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).
    """

    # 1. Establish a connection to the source db using PostgresHook.
    hook = PostgresHook("source_db")
    conn = hook.get_conn()

    # 2. Get the data from the source database
    flats = pd.read_sql("SELECT * FROM flats", conn)
    buildings = pd.read_sql("SELECT * FROM buildings", conn)

    # 3. Close the connection to the source db
    conn.close()

    # 4. Merge the two dataframes on the "building_id" column
    merged_df = flats.merge(
        buildings,
        left_on="building_id",
        right_on="id",
        how="left",
    )

    # 5. Pushing the merged dataframe to the next task
    ti = kwargs["ti"]
    ti.xcom_push("extracted_data", merged_df)


def transform(**kwargs):
    """
    Transforms the extracted data by renaming columns, dropping
    unnecessary columns, changing dtypes. Finally, pushes the
    transformed data to  the next task.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).
    """

    # 1. Get the extracted data
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract", key="extracted_data")

    # 2. Renaming columns and dropping unnesessary columns
    data.rename(columns={"id_x": "flat_id", "price": "target"}, inplace=True)
    data.drop(columns=["id_y"], inplace=True)

    # 3. Changing the dtype of a target column to float since the price
    # can easily be non-integer
    data["target"] = data["target"].astype(float)

    # 4. Pushing the transformed data to the next task
    ti.xcom_push("transformed_data", data)


def load(**kwargs):
    """
    Loads the transformed data into the destination database using
    a PostgresHook.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).
    """

    # 1. Get the transformed data
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="transform", key="transformed_data")

    # 2. Load the data into the destination database via PostgresHook
    hook = PostgresHook("destination_db")
    hook.insert_rows(
        table="prepared_flats",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=["flat_id"],
        rows=data.values.tolist(),
    )
