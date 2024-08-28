from math import log10

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
from sklearn.ensemble import IsolationForest
from airflow.providers.postgres.hooks.postgres import PostgresHook


def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate rows from the input DataFrame based on
    the values in all columns except the "flat_id" column.
    Keeps the first occurrence of each duplicated row and also
    creates a new column called "is_duplicated" that indicates
    whether a row was duplicated or not.

    Args:
        data (pd.DataFrame):
            The input DataFrame to remove duplicates from.

    Returns:
        pd.DataFrame:
            The input DataFrame with duplicate rows removed and a new
            column "is_duplicated" added.
    """
    cols = data.columns.drop("flat_id").tolist()
    data["is_duplicated"] = data.duplicated(subset=cols, keep=False)
    data.drop_duplicates(subset=cols, keep="first", inplace=True)
    return data


def iqr_filter(
    data: pd.DataFrame,
    features: dict[str],
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Filters the input DataFrame by removing rows that have
    values outside the interquartile range (IQR) for specified
    features, while keeping rows with NaNs.

    Args:
        data (pd.DataFrame):
            The input DataFrame to filter.
        features (dict[str]):
            Dictionary of feature names and their corresponding
            threshold values for the IQR filter.
        verbose (bool):
            Whether to print the number of rows removed
            and the percentage of rows removed.
            Defaults to True.

    Returns:
        pd.DataFrame:
            The input DataFrame without outliers.
    """
    len_old = len(data)
    mask = pd.Series(True, index=data.index)
    for feature, threshold in features.items():
        Q1 = data[feature].quantile(0.25)
        Q3 = data[feature].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - threshold * IQR
        upper = Q3 + threshold * IQR
        mask &= ((data[feature] >= lower) & (data[feature] <= upper)) | data[
            feature
        ].isna()
    data = data[mask]
    if verbose:
        print(
            f"Removed {len_old - len(data)} outliers, ratio - "
            f"{round(100 * (len_old - len(data)) / len_old, 2)}%"
        )
    return data


def create_table(**kwargs):
    """
    Creates a SQLAlchemy table named "cleaned_flats" with the
    specified columns, if the table does not already exist in
    the destination database.

    The table also has a unique constraint on the "flat_id" column.
    """

    hook = PostgresHook("destination_db")
    conn = hook.get_sqlalchemy_engine()
    metadata = MetaData()

    table = Table(
        "cleaned_flats",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("flat_id", Integer),
        Column("floor", Integer),
        Column("is_apartment", Boolean),
        Column("kitchen_area", Float),
        Column("living_area", Float),
        Column("rooms", Integer),
        Column("total_area", Float),
        Column("log1p_target", Float),
        Column("building_id", Integer),
        Column("build_year", Integer),
        Column("building_type_int", Integer),
        Column("latitude", Float),
        Column("longitude", Float),
        Column("ceiling_height", Float),
        Column("flats_count", Integer),
        Column("floors_total", Integer),
        Column("has_elevator", Boolean),
        Column("is_duplicated", Boolean),
        UniqueConstraint("flat_id", name="unique_flat_id_constraint_2"),
    )

    if not inspect(conn).has_table(table.name):
        metadata.create_all(conn)


def extract(**kwargs):
    """
    Extracts data from the destination db via PostgresHook, and
    pushes the data to the next task.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).
    """

    # 1. Connect to the destination db using PostgresHook.
    hook = PostgresHook("destination_db")
    conn = hook.get_conn()

    # 2. Get the data from the destination database
    data = pd.read_sql("SELECT * FROM prepared_flats", conn)

    # 3. Close the connection to the destination database
    conn.close()

    # 4. Drop the unnecessary 'id' columns
    data.drop(columns=["id"], inplace=True)

    # 5. Pushing the merged dataframe to the next task
    ti = kwargs["ti"]
    ti.xcom_push("extracted_data", data)


def transform(**kwargs):
    """
    Transforms the extracted data by dealing with duplicates, missing
    data and outliers. Also creates a new target column.
    Finally, pushes the transformed data to  the next task.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).
    """

    # 1. Get the extracted data
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="extract", key="extracted_data")

    # 2. Removing duplicates
    data = remove_duplicates(data)

    # 3. Removing rows with missing data
    data.dropna(axis=0, inplace=True)

    # 4. Creating a new target column
    data["log1p_target"] = data["target"].apply(lambda x: log10(1 + x))

    # 5. Omitting outliers

    ## One-dimensional outliers
    for query in [
        "building_type_int != 5 | building_type_int.isna()",
        "living_area <= 150 | living_area.isna()",
        "kitchen_area <= 35 | kitchen_area.isna()",
        "rooms <= 6 | rooms.isna()",
    ]:
        data = data.query(query)

    data = iqr_filter(
        data=data,
        features={
            "log1p_target": 4,
            "latitude": 1.5,
            "longitude": 2,
            "total_area": 4,
            "ceiling_height": 4,
            "flats_count": 3,
            "floor": 3,
            "floors_total": 3,
            "build_year": 1.5,
        },
        verbose=False,
    )

    ## Multidimensional outliers
    model = IsolationForest(contamination=0.01, random_state=42)
    data["outlier"] = model.fit_predict(
        data[
            [
                "floor",
                "kitchen_area",
                "living_area",
                "rooms",
                "total_area",
                "target",
                "build_year",
                "latitude",
                "longitude",
                "ceiling_height",
                "flats_count",
                "floors_total",
                "is_apartment",
                "has_elevator",
                "is_duplicated",
                "building_type_int",
            ]
        ]
    )
    data = data.query("outlier == 1")

    # 6. Removing unnessesary columns
    data.drop(columns=["studio", "target", "outlier"], inplace=True)

    # 7. Pushing the transformed data to the next task
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
        table="cleaned_flats",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=["flat_id"],
        rows=data.values.tolist(),
    )
