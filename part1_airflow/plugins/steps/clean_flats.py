from math import log10

import pandas as pd
from ensure import ensure_annotations
from sklearn.ensemble import IsolationForest
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract(**kwargs):
    """
    Extracts data from the destination db via PostgresHook, and
    pushes the data to the next task.

    Args:
        **kwargs (dict):
            A dictionary of keyword arguments, including the task
            instance (ti).

    Raises:
        Exception:
            If an exception occurs during data extraction.
    """

    try:
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

    except Exception as e:
        raise Exception(f"An exception occurred during extraction") from e


@ensure_annotations
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


@ensure_annotations
def iqr_filter(
    data: pd.DataFrame,
    features: dict,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Filters the input DataFrame by removing rows that have
    values outside the interquartile range (IQR) for specified
    features, while keeping rows with NaNs.

    Args:
        data (pd.DataFrame):
            The input DataFrame to filter.
        features (dict[str, int | float]):
            Dictionary of feature names and their corresponding
            threshold values for the IQR filter.
        verbose (bool):
            Whether to print the number of rows removed
            and the percentage of rows removed.
            Defaults to True.

    Returns:
        pd.DataFrame:
            The input DataFrame without outliers.

    Raises:
        ValueError:
            If the features dictionary does not match the
            expected format.
    """

    if not all(isinstance(v, (int, float)) for v in features.values()):
        raise ValueError("All values in features dict must be numeric.")
    if not all(isinstance(k, str) for k in features.keys()):
        raise ValueError("All keys in features dict must be strings.")

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


@ensure_annotations
def clean(data: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Cleans data by dealing with duplicates, missing
    data and outliers. Also creates a new target column.

    Args:
        data (pd.DataFrame):
            The input data

    Returns:
        pd.DataFrame:
            The transformed data
    """

    # 1. Removing duplicates
    data = remove_duplicates(data)

    # 2. Removing rows with missing data
    data.dropna(axis=0, inplace=True)

    # 3. Creating a new target column
    data["log1p_target"] = data["target"].apply(lambda x: log10(1 + x))

    # 4. Omitting outliers

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

    return data
