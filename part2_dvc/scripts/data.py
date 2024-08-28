import os
from pathlib import Path

import pandas as pd

from utils import read_yaml, create_connection


def get_data() -> None:
    """
    Loads data from a database and saves the resulting data to a CSV
    file.
    """

    # 1. Loading hyperparams
    params = read_yaml(Path("params.yaml"))

    # 2. Reading data
    conn = create_connection(db_name="DESTINATION")
    data = pd.read_sql(
        "select * from cleaned_flats",
        conn,
        index_col=params["index_col"],
    )
    conn.dispose()

    # 3. Saving data
    os.makedirs("data", exist_ok=True)
    data.to_csv("data/initial_data.csv", index=None)


if __name__ == "__main__":
    get_data()
