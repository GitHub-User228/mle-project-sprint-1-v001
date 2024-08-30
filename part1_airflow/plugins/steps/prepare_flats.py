import pandas as pd
from ensure import ensure_annotations
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract(**kwargs):
    """
    Extracts data from the source database via PostgresHook, merges
    the flats and buildings data, and pushes the merged dataframe to
    the next task.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).

    Raises:
        Exception:
            If an exception occurs during data extraction.
    """
    try:
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

    except Exception as e:
        raise Exception(f"An exception occurred during extraction") from e


@ensure_annotations
def prepare(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the input data by renaming columns, dropping
    unnecessary columns, changing dtypes.

    Args:
        data (pd.DataFrame):
            The input data

    Returns:
        pd.DataFrame:
            The transformed data
    """
    data.rename(columns={"id_x": "flat_id", "price": "target"}, inplace=True)
    data.drop(columns=["id_y"], inplace=True)
    data["target"] = data["target"].astype(float)
    return data
